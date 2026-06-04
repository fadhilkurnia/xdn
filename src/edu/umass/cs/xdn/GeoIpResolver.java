package edu.umass.cs.xdn;

import java.io.File;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Location;

import edu.umass.cs.nio.interfaces.Geolocation;

/**
 * Resolves a client {@link InetAddress} (IPv4 or IPv6) to an approximate {@link Geolocation} using
 * the GeoLite2-City MaxMind database (the same {@code .mmdb} shipped with xdn-dns). Used by {@link
 * XdnGeoDemandProfiler} as the demand fallback when a request carries no {@code X-Client-Location}
 * header.
 *
 * <p>The (relatively) expensive {@code .mmdb} traversal + record decode runs off the request hot
 * path (in the profiler's background worker). A bounded LRU caches results, including misses, so
 * repeated client IPs skip the lookup. Local / private / non-routable addresses are rejected up
 * front via {@link #isNonGeolocatable} -- they aren't real client geolocations.
 */
public class GeoIpResolver {

  private static final Logger LOGGER = Logger.getLogger(GeoIpResolver.class.getName());

  /** System property overriding the GeoLite2-City db path. */
  public static final String MMDB_PATH_PROPERTY = "xdn.geoip.mmdb";

  /**
   * Default db path: the GeoLite2-City db baked into the node AMI at {@code /opt/xdn/geo/} (same
   * file xdn-dns ships; see aws/ami_common.sh). Absent file -> IP-based demand fallback stays off.
   */
  public static final String DEFAULT_MMDB_PATH = "/opt/xdn/geo/geolocation_city_data.mmdb";

  private static final int RESULT_CACHE_MAX = 50_000;

  private static volatile GeoIpResolver defaultInstance;
  private static volatile boolean defaultInitTried;

  private final DatabaseReader reader;
  // LRU of InetAddress -> Geolocation; caches misses as null so unknown IPs aren't re-queried.
  private final Map<InetAddress, Geolocation> cache;

  public GeoIpResolver(File mmdb) throws Exception {
    // withCache(CHMCache): MaxMind's decoder node cache speeds the tree walk; the LRU below avoids
    // the walk entirely for repeated IPs.
    this.reader = new DatabaseReader.Builder(mmdb).withCache(new CHMCache()).build();
    this.cache =
        new LinkedHashMap<InetAddress, Geolocation>(1024, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<InetAddress, Geolocation> eldest) {
            return size() > RESULT_CACHE_MAX;
          }
        };
  }

  /**
   * Lazily-loaded process-wide resolver from {@code -Dxdn.geoip.mmdb} (default {@link
   * #DEFAULT_MMDB_PATH}), or {@code null} if the db is absent / fails to load. When null, the
   * IP-based demand fallback simply stays off; the {@code X-Client-Location} header path is
   * unaffected. The shared reader is memory-mapped and thread-safe.
   */
  public static GeoIpResolver getDefaultOrNull() {
    if (!defaultInitTried) {
      synchronized (GeoIpResolver.class) {
        if (!defaultInitTried) {
          defaultInitTried = true;
          String path = System.getProperty(MMDB_PATH_PROPERTY, DEFAULT_MMDB_PATH);
          File f = new File(path);
          if (!f.isFile()) {
            LOGGER.log(Level.INFO, "GeoIP db not found at {0}; IP-based demand disabled", path);
          } else {
            try {
              defaultInstance = new GeoIpResolver(f);
              LOGGER.log(Level.INFO, "GeoIP demand fallback enabled from {0}", path);
            } catch (Exception e) {
              LOGGER.log(
                  Level.WARNING, "GeoIP db load failed (" + path + "); IP-based demand disabled", e);
            }
          }
        }
      }
    }
    return defaultInstance;
  }

  /** Client geolocation, or {@code null} for local/non-geolocatable IPs and GeoIP misses. */
  public Geolocation resolve(InetAddress ip) {
    if (isNonGeolocatable(ip)) {
      return null;
    }
    synchronized (cache) {
      if (cache.containsKey(ip)) {
        return cache.get(ip); // cached hit (possibly a cached miss == null)
      }
    }
    Geolocation geo = null;
    try {
      Optional<CityResponse> resp = reader.tryCity(ip);
      if (resp.isPresent()) {
        Location l = resp.get().location();
        if (l != null && l.latitude() != null && l.longitude() != null) {
          geo = new Geolocation(l.latitude(), l.longitude());
        }
      }
    } catch (Exception e) {
      // IO / GeoIp2Exception -> treat as a miss (cached as null below).
    }
    synchronized (cache) {
      cache.put(ip, geo);
    }
    return geo;
  }

  /**
   * True for addresses GeoIP can't (and shouldn't) resolve: loopback, wildcard, link-local, RFC1918
   * site-local, multicast, IPv6 ULA ({@code fc00::/7}) and IPv4 CGNAT ({@code 100.64.0.0/10}). The
   * last two are NOT covered by the JDK's {@link InetAddress} predicates, so we check the bytes.
   */
  public static boolean isNonGeolocatable(InetAddress ip) {
    if (ip == null) {
      return true;
    }
    if (ip.isLoopbackAddress()
        || ip.isAnyLocalAddress()
        || ip.isLinkLocalAddress()
        || ip.isSiteLocalAddress()
        || ip.isMulticastAddress()) {
      return true;
    }
    byte[] b = ip.getAddress();
    if (b.length == 16) {
      return (b[0] & 0xFE) == 0xFC; // fc00::/7 (unique local)
    }
    if (b.length == 4) {
      int o0 = b[0] & 0xFF;
      int o1 = b[1] & 0xFF;
      return o0 == 100 && o1 >= 64 && o1 <= 127; // 100.64.0.0/10 (CGNAT)
    }
    return false;
  }

  // ---- manual test harness: verify IPv4 + IPv6 against a .mmdb ----
  // java -cp build/classes:lib/* edu.umass.cs.xdn.GeoIpResolver xdn-dns/geolocation_city_data.mmdb
  public static void main(String[] args) throws Exception {
    File mmdb = new File(args.length > 0 ? args[0] : DEFAULT_MMDB_PATH);
    System.out.println("opening " + mmdb + "\n");
    GeoIpResolver r = new GeoIpResolver(mmdb);
    String[] tests = {
      "8.8.8.8", "128.119.240.84", // public IPv4 (incl. UMass)
      "2001:4860:4860::8888", // public IPv6
      "127.0.0.1", "::1", "192.168.1.5", "10.0.1.10", // loopback / private -> skip
      "fd12:3456::1", "100.64.0.1" // ULA / CGNAT -> skip
    };
    for (String t : tests) {
      InetAddress ip = InetAddress.getByName(t);
      String fam = ip.getAddress().length == 16 ? "IPv6" : "IPv4";
      Geolocation geo = r.resolve(ip);
      String res =
          isNonGeolocatable(ip)
              ? "(local -> skip)"
              : (geo != null ? geo.latitude() + "," + geo.longitude() : "(no geo / miss)");
      System.out.printf("  %-26s %-5s %s%n", t, fam, res);
    }
  }
}
