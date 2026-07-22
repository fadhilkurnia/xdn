package edu.umass.cs.xdn;

import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.utils.Shell;
import edu.umass.cs.xdn.utils.ShellOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Per-node bandwidth profiler for cluster-mode services.
 *
 * <p>Each registered service has a tiny probe sidecar sharing its cluster member's network
 * namespace. The profiler periodically reads every TCP socket's kernel byte counters through the
 * probe ({@code docker exec <probe> ss -tainH}; {@code bytes_acked}/{@code bytes_received} from
 * tcp_info are exact cumulative totals, so polling loses nothing on long-lived connections) and
 * folds per-connection deltas into directed per-peer edges.
 *
 * <p>Edges are classified by peer address and port: an address that Docker's embedded DNS reports
 * for a {@code replica-N} overlay alias is coordination traffic to that replica; loopback rows on
 * the entry port's server side are published-port client connections (Docker's userland proxy
 * injects them into the namespace over loopback); all other loopback is intra-pod chatter and is
 * skipped; anything else (bridge gateway, host) is aggregated as {@code client} traffic. This
 * yields the coordinated vs. client-demand split without any protocol knowledge.
 *
 * <p>All failures degrade gracefully: a missing probe or failed exec skips the poll and never
 * affects the service itself.
 */
public class XdnBandwidthProfiler {

  /** Aggregate edge label for non-peer, non-loopback traffic. */
  public static final String CLIENT_EDGE = "client";

  private static final long IP_MAP_REFRESH_MS = 30_000;

  private final Logger logger = Logger.getLogger(XdnBandwidthProfiler.class.getName());
  private final String myNodeId;
  private final Map<String, ServiceState> services = new ConcurrentHashMap<>();
  private ScheduledExecutorService scheduler; // created lazily on first register

  public XdnBandwidthProfiler(String myNodeId) {
    this.myNodeId = myNodeId;
  }

  /** One directed edge from this replica to a peer (or the client aggregate). */
  static final class Edge {
    volatile long txBytes;
    volatile long rxBytes;
    volatile double txRateBps;
    volatile double rxRateBps;
  }

  /** Parsed row of {@code ss -tinH}: one established connection with cumulative counters. */
  record SsConn(String localAddr, int localPort, String peerAddr, int peerPort, long tx, long rx) {
    String tupleKey() {
      return localAddr + ":" + localPort + ">" + peerAddr + ":" + peerPort;
    }
  }

  private static final class ConnCounters {
    long tx;
    long rx;
  }

  private static final class ServiceState {
    final String probeContainerName;
    final int clusterSize;
    final int entryPort;
    volatile Map<String, String> ipToReplica = Map.of();
    long ipMapRefreshedMs;
    final Map<String, ConnCounters> connState = new HashMap<>(); // poll-thread only
    final Map<String, Edge> edges = new ConcurrentHashMap<>();
    long lastPollMs;
    int emptyPolls;
    long lastProbeRestartMs;

    ServiceState(String probeContainerName, int clusterSize, int entryPort) {
      this.probeContainerName = probeContainerName;
      this.clusterSize = clusterSize;
      this.entryPort = entryPort;
    }
  }

  /** Starts profiling {@code serviceName} through the given probe container. */
  public synchronized void register(
      String serviceName, String probeContainerName, int clusterSize, int entryPort) {
    this.services.put(serviceName, new ServiceState(probeContainerName, clusterSize, entryPort));
    if (this.scheduler == null) {
      long intervalMs =
          Config.getGlobalLong(ReconfigurationConfig.RC.XDN_CLUSTER_BW_POLL_INTERVAL_MS);
      this.scheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "xdn-bw-profiler-" + myNodeId);
                t.setDaemon(true);
                return t;
              });
      this.scheduler.scheduleWithFixedDelay(
          this::pollAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }
    logger.log(
        Level.INFO,
        "{0}:{1} bandwidth profiling started for {2} via probe {3}",
        new Object[] {
          myNodeId.toUpperCase(), this.getClass().getSimpleName(), serviceName, probeContainerName
        });
  }

  public void deregister(String serviceName) {
    this.services.remove(serviceName);
  }

  /**
   * Returns the current edge snapshot for {@code serviceName} as JSON, or null if the service is
   * not profiled (tracer disabled, probe failed, or not a cluster service).
   */
  public JSONObject snapshot(String serviceName) {
    ServiceState st = this.services.get(serviceName);
    if (st == null) {
      return null;
    }
    try {
      JSONArray edges = new JSONArray();
      for (Map.Entry<String, Edge> e : st.edges.entrySet()) {
        Edge edge = e.getValue();
        JSONObject o = new JSONObject();
        o.put("peer", e.getKey());
        o.put("txBytes", edge.txBytes);
        o.put("rxBytes", edge.rxBytes);
        o.put("txRateBps", Math.round(edge.txRateBps));
        o.put("rxRateBps", Math.round(edge.rxRateBps));
        edges.put(o);
      }
      JSONObject result = new JSONObject();
      result.put("edges", edges);
      result.put("sampledAt", st.lastPollMs);
      return result;
    } catch (JSONException e) {
      logger.log(Level.WARNING, "failed to serialize bandwidth snapshot: " + e);
      return null;
    }
  }

  private void pollAll() {
    for (Map.Entry<String, ServiceState> entry : this.services.entrySet()) {
      try {
        pollService(entry.getKey(), entry.getValue());
      } catch (Exception e) {
        logger.log(
            Level.FINE, "bandwidth poll failed for " + entry.getKey() + ": " + e.getMessage());
      }
    }
  }

  private void pollService(String serviceName, ServiceState st) {
    long now = System.currentTimeMillis();
    if (now - st.ipMapRefreshedMs > IP_MAP_REFRESH_MS) {
      refreshIpMap(st);
      st.ipMapRefreshedMs = now;
    }

    ShellOutput out =
        Shell.runCommandWithOutput("docker exec " + st.probeContainerName + " ss -tainH", true);
    if (out.exitCode != 0) {
      logger.log(
          Level.FINE,
          "ss probe failed for " + serviceName + " (exit " + out.exitCode + "): " + out.stderr);
      return;
    }

    List<SsConn> conns = parseSsOutput(out.stdout);
    // A namespace with no sockets AT ALL (ss -a shows not even listeners) is almost
    // certainly abandoned: the member container restarted (readiness heal, crash loop
    // during first boot) and got a fresh sandbox while the probe kept the old one.
    // Restarting the probe rejoins the member's current namespace. A quiet-but-live pod
    // still shows its LISTEN sockets and must not trigger this.
    if (out.stdout.isBlank()) {
      st.emptyPolls++;
      if (st.emptyPolls >= 3 && now - st.lastProbeRestartMs > 60_000) {
        logger.log(
            Level.INFO,
            "{0}:{1} probe {2} sees no sockets; restarting it to rebind the namespace",
            new Object[] {
              myNodeId.toUpperCase(), this.getClass().getSimpleName(), st.probeContainerName
            });
        Shell.runCommand("docker restart " + st.probeContainerName, true);
        st.lastProbeRestartMs = now;
        st.emptyPolls = 0;
      }
      st.lastPollMs = now;
      return;
    }
    st.emptyPolls = 0;

    long intervalMs = st.lastPollMs > 0 ? Math.max(1, now - st.lastPollMs) : 0;
    Map<String, long[]> deltaByPeer = new HashMap<>(); // label -> {txDelta, rxDelta}
    for (SsConn conn : conns) {
      String label =
          classifyPeer(
              conn.peerAddr(), conn.localPort(), conn.peerPort(), st.entryPort, st.ipToReplica);
      if (label == null) {
        continue; // intra-pod traffic
      }
      ConnCounters prev = st.connState.get(conn.tupleKey());
      long txDelta;
      long rxDelta;
      if (prev == null || conn.tx() < prev.tx || conn.rx() < prev.rx) {
        // New connection (or 4-tuple reuse after a counter reset): attribute its full
        // cumulative counters, which accrued since the previous poll or tracer start.
        txDelta = conn.tx();
        rxDelta = conn.rx();
        prev = new ConnCounters();
        st.connState.put(conn.tupleKey(), prev);
      } else {
        txDelta = conn.tx() - prev.tx;
        rxDelta = conn.rx() - prev.rx;
      }
      prev.tx = conn.tx();
      prev.rx = conn.rx();
      deltaByPeer.computeIfAbsent(label, k -> new long[2]);
      deltaByPeer.get(label)[0] += txDelta;
      deltaByPeer.get(label)[1] += rxDelta;
    }

    for (Map.Entry<String, long[]> d : deltaByPeer.entrySet()) {
      Edge edge = st.edges.computeIfAbsent(d.getKey(), k -> new Edge());
      edge.txBytes += d.getValue()[0];
      edge.rxBytes += d.getValue()[1];
      if (intervalMs > 0) {
        edge.txRateBps = d.getValue()[0] * 1000.0 / intervalMs;
        edge.rxRateBps = d.getValue()[1] * 1000.0 / intervalMs;
      }
    }
    // An edge with no connections this poll has zero current rate.
    for (Map.Entry<String, Edge> e : st.edges.entrySet()) {
      if (!deltaByPeer.containsKey(e.getKey())) {
        e.getValue().txRateBps = 0;
        e.getValue().rxRateBps = 0;
      }
    }
    st.lastPollMs = now;
  }

  private void refreshIpMap(ServiceState st) {
    StringBuilder names = new StringBuilder();
    for (int i = 0; i < st.clusterSize; i++) {
      names.append(" replica-").append(i);
    }
    // busybox getent exits nonzero if any name is unresolved; parse whatever resolved.
    ShellOutput out =
        Shell.runCommandWithOutput(
            "docker exec " + st.probeContainerName + " getent hosts" + names, true);
    Map<String, String> map = parseGetentOutput(out.stdout);
    if (!map.isEmpty()) {
      st.ipToReplica = map;
    }
  }

  /**
   * Returns the edge label for a connection: the replica name for overlay peers, {@link
   * #CLIENT_EDGE} for external addresses, or null for traffic that must not be counted.
   *
   * <p>Loopback needs port awareness: Docker's userland proxy delivers published-port client
   * connections <em>into</em> the namespace over loopback (dockerd 29+), so a loopback row whose
   * local port is the service's entry port is real client traffic. Its mirror row (the proxy's
   * injected socket, peer port == entry port) and every other loopback flow (sidecar-to-member
   * chatter) are intra-pod and skipped.
   */
  static String classifyPeer(
      String peerAddr,
      int localPort,
      int peerPort,
      int entryPort,
      Map<String, String> ipToReplica) {
    boolean loopback = peerAddr.startsWith("127.") || peerAddr.equals("::1");
    if (loopback) {
      return entryPort > 0 && localPort == entryPort ? CLIENT_EDGE : null;
    }
    String replica = ipToReplica.get(peerAddr);
    return replica != null ? replica : CLIENT_EDGE;
  }

  /**
   * Parses {@code ss -tinH} output: each connection is a state row ({@code ESTAB 0 0 local peer})
   * followed by an indented tcp_info line carrying {@code bytes_acked}/{@code bytes_received}. Rows
   * without counters (no payload yet) are kept with zeros; non-TCP noise is skipped.
   */
  static List<SsConn> parseSsOutput(String output) {
    List<SsConn> conns = new ArrayList<>();
    String pendingLocal = null;
    String pendingPeer = null;
    for (String line : output.split("\n")) {
      if (line.isBlank()) {
        continue;
      }
      boolean isInfoLine = Character.isWhitespace(line.charAt(0));
      if (!isInfoLine) {
        String[] tokens = line.trim().split("\\s+");
        // STATE RECV-Q SEND-Q LOCAL:PORT PEER:PORT
        if (tokens.length >= 5 && tokens[3].contains(":") && tokens[4].contains(":")) {
          pendingLocal = tokens[3];
          pendingPeer = tokens[4];
        } else {
          pendingLocal = null;
          pendingPeer = null;
        }
        continue;
      }
      if (pendingLocal == null) {
        continue;
      }
      long tx = extractCounter(line, "bytes_acked:");
      long rx = extractCounter(line, "bytes_received:");
      // bytes_acked counts the SYN's sequence space; subtract it for pure payload.
      if (tx > 0) {
        tx -= 1;
      }
      String[] local = splitAddrPort(pendingLocal);
      String[] peer = splitAddrPort(pendingPeer);
      if (local != null && peer != null) {
        conns.add(
            new SsConn(
                local[0], Integer.parseInt(local[1]), peer[0], Integer.parseInt(peer[1]), tx, rx));
      }
      pendingLocal = null;
      pendingPeer = null;
    }
    return conns;
  }

  /** Parses {@code getent hosts name...} output ({@code IP name [name...]}) into IP to name. */
  static Map<String, String> parseGetentOutput(String output) {
    Map<String, String> map = new HashMap<>();
    for (String line : output.split("\n")) {
      String[] tokens = line.trim().split("\\s+");
      if (tokens.length >= 2 && !tokens[0].isEmpty()) {
        map.put(normalizeAddr(tokens[0]), tokens[1]);
      }
    }
    return map;
  }

  /** Splits {@code addr:port} (or {@code [v6addr]:port}) into normalized address and port. */
  static String[] splitAddrPort(String addrPort) {
    int idx = addrPort.lastIndexOf(':');
    if (idx <= 0) {
      return null;
    }
    String addr = normalizeAddr(addrPort.substring(0, idx));
    String port = addrPort.substring(idx + 1);
    if (!port.chars().allMatch(Character::isDigit) || port.isEmpty()) {
      return null;
    }
    return new String[] {addr, port};
  }

  /** Strips brackets and the IPv4-mapped-IPv6 prefix: {@code [::ffff:10.0.3.4]} to 10.0.3.4. */
  static String normalizeAddr(String addr) {
    String a = addr;
    if (a.startsWith("[") && a.endsWith("]")) {
      a = a.substring(1, a.length() - 1);
    }
    if (a.startsWith("::ffff:")) {
      a = a.substring("::ffff:".length());
    }
    return a;
  }

  private static long extractCounter(String infoLine, String key) {
    int idx = infoLine.indexOf(key);
    if (idx < 0) {
      return 0;
    }
    int start = idx + key.length();
    int end = start;
    while (end < infoLine.length() && Character.isDigit(infoLine.charAt(end))) {
      end++;
    }
    return end > start ? Long.parseLong(infoLine.substring(start, end)) : 0;
  }
}
