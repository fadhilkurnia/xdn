package edu.umass.cs.xdn.utils;

import com.maxmind.db.Reader;
import edu.umass.cs.nio.interfaces.Geolocation;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GeoIpLookup {
    private static final Logger LOG = Logger.getLogger(GeoIpLookup.class.getName());

    // configurable via -Dxdn.geoip.mmdb=/path/to/GeoLite2-City.mmdb
    private static final String MMDB_PATH =
            System.getProperty("xdn.geoip.mmdb", "GeoLite2-City.mmdb");

    private static final Reader READER = initReader();

    private static Reader initReader() {
        try {
            File f = new File(MMDB_PATH);
            if (!f.exists()) {
                LOG.log(Level.WARNING,
                        "GeoIP MMDB file not found at {0} — X-Client-IP lookups disabled",
                        MMDB_PATH);
                return null;
            }
            Reader r = new Reader(f);
            LOG.log(Level.INFO, "GeoIP MMDB loaded from {0}", MMDB_PATH);
            return r;
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to load GeoIP MMDB: {0}", e.getMessage());
            return null;
        }
    }

    /**
     * Looks up the geolocation of the given IP string.
     * Returns null if the MMDB is unavailable, the IP is invalid,
     * or no location record is found.
     */
    @SuppressWarnings("unchecked")
    public static Geolocation lookup(String ip) {
        if (READER == null) {
            LOG.warning("GeoIP lookup skipped — READER is null (MMDB failed to load)");
            return null;
        }
        if (ip == null || ip.isBlank()) return null;
        try {
            InetAddress addr = InetAddress.getByName(ip.trim());
            Map<String, Object> record = READER.get(addr, Map.class);
            if (record == null) {
                LOG.log(Level.WARNING, "GeoIP lookup returned null record for IP: {0}", ip);
                return null;
            }
            Map<String, Object> location = (Map<String, Object>) record.get("location");
            if (location == null) {
                LOG.log(Level.WARNING, "GeoIP record has no location field for IP: {1}", ip);
                return null;
            }
            Double lat = (Double) location.get("latitude");
            Double lng = (Double) location.get("longitude");
            if (lat == null || lng == null) {
                LOG.log(Level.WARNING, "GeoIP location missing lat/lon for IP: {0}", ip);
                return null;
            }
            Geolocation geo = new Geolocation(lat, lng);
            LOG.log(Level.INFO, "GeoIP lookup for {0} -> lat={1} lng={2}",
                    new Object[]{ ip, geo.latitude(), geo.longitude() });
            return geo;
        } catch (Exception e) {
            LOG.log(Level.WARNING, "GeoIP lookup exception for IP {0}: {1}",
                    new Object[]{ ip, e.getMessage() });
            return null;
        }
    }
}