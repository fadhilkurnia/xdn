package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test for the integrated cluster-mode bandwidth profiler.
 *
 * <p>Launches a 3-replica etcd ensemble, drives writes through replica 0, then asserts the
 * replica-info endpoint's {@code bandwidth} section reports nonzero directed traffic to exactly the
 * two overlay peers plus a client-side aggregate. Assertions target names and cumulative bytes, not
 * rates, for stability.
 */
public class XdnClusterBandwidthTest {

  private static final String SERVICE = "etcd-bw-test";
  private static final int CLIENT_PORT = 2379;
  private static final int PEER_PORT = 2380;
  private static final Duration HEALTH_BUDGET = Duration.ofSeconds(120);
  private static final Duration BW_BUDGET = Duration.ofSeconds(60);

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  @Test
  public void testBandwidthEdgesReported() throws Exception {
    assertTrue(XdnTestCluster.isDockerAvailable(), "docker is required for this test");
    assertTrue(
        XdnTestCluster.ensureDockerSwarm(),
        "cluster services need Swarm overlay networking — `docker swarm init` failed");
    assertTrue(
        XdnTestCluster.buildEtcdClusterImage(),
        "could not build xdn-etcd-cluster:test from services/etcd-cluster/");
    assertTrue(
        XdnTestCluster.buildImage("xdn-bw-probe:test", "services/bw-probe/"),
        "could not build xdn-bw-probe:test from services/bw-probe/");

    // Use the locally built probe image and a fast poll so edges appear quickly.
    Config.register(
        new String[] {
          "XDN_CLUSTER_BW_PROBE_IMAGE=xdn-bw-probe:test", "XDN_CLUSTER_BW_POLL_INTERVAL_MS=500"
        });

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchClusterService(
          SERVICE, "xdn-etcd-cluster:test", "/etcd-data/", CLIENT_PORT, PEER_PORT, 3);

      for (int idx = 0; idx < 3; idx++) {
        waitForHealth(cluster, idx, System.nanoTime() + HEALTH_BUDGET.toNanos());
      }

      // Drive some writes through replica 0 so peer replication traffic is guaranteed
      // beyond raft heartbeats.
      for (int i = 0; i < 5; i++) {
        assertEquals(200, putKey(cluster, 0, "bw-key-" + i, "value-" + i).statusCode());
      }

      // Replica 0 must eventually report nonzero traffic to replica-1, replica-2, and a
      // client aggregate (our own health polls and PUTs arrive via the bridge).
      long deadline = System.nanoTime() + BW_BUDGET.toNanos();
      JSONObject lastBandwidth = null;
      while (System.nanoTime() < deadline) {
        HttpResponse<String> info =
            cluster.sendGetRequest(SERVICE, 0, "/api/v2/services/" + SERVICE + "/replica/info");
        if (info.statusCode() == 200) {
          JSONObject json = new JSONObject(info.body());
          if (json.has("bandwidth")) {
            lastBandwidth = json.getJSONObject("bandwidth");
            if (hasExpectedEdges(lastBandwidth)) {
              return;
            }
          }
        }
        Thread.sleep(1000);
      }
      fail("bandwidth edges never complete; last snapshot: " + lastBandwidth);
    }
  }

  /** True when both overlay peers and the client aggregate show nonzero cumulative bytes. */
  private boolean hasExpectedEdges(JSONObject bandwidth) throws Exception {
    JSONArray edges = bandwidth.getJSONArray("edges");
    Set<String> nonzeroPeers = new HashSet<>();
    for (int i = 0; i < edges.length(); i++) {
      JSONObject edge = edges.getJSONObject(i);
      String peer = edge.getString("peer");
      assertTrue(
          peer.equals("replica-1")
              || peer.equals("replica-2")
              || peer.equals(XdnBandwidthProfiler.CLIENT_EDGE),
          "unexpected edge peer '" + peer + "' on replica 0 (self must never appear)");
      if (edge.getLong("txBytes") > 0 && edge.getLong("rxBytes") > 0) {
        nonzeroPeers.add(peer);
      }
    }
    return nonzeroPeers.contains("replica-1")
        && nonzeroPeers.contains("replica-2")
        && nonzeroPeers.contains(XdnBandwidthProfiler.CLIENT_EDGE);
  }

  private void waitForHealth(XdnTestCluster cluster, int replicaIdx, long deadlineNs)
      throws Exception {
    Exception last = null;
    while (System.nanoTime() < deadlineNs) {
      try {
        HttpResponse<String> r = cluster.sendGetRequest(SERVICE, replicaIdx, "/health");
        if (r.statusCode() == 200 && r.body().contains("\"true\"")) {
          return;
        }
        last = new IllegalStateException("replica " + replicaIdx + " not healthy: " + r.body());
      } catch (Exception e) {
        last = e;
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("etcd replica " + replicaIdx + " never reported healthy", last);
  }

  private HttpResponse<String> putKey(XdnTestCluster cluster, int replicaIdx, String k, String v)
      throws Exception {
    JSONObject body = new JSONObject();
    body.put("key", Base64.getEncoder().encodeToString(k.getBytes()));
    body.put("value", Base64.getEncoder().encodeToString(v.getBytes()));
    int port = cluster.getActiveHttpPort("AR" + replicaIdx);
    HttpRequest req =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + port + "/v3/kv/put"))
            .timeout(Duration.ofSeconds(10))
            .header("XDN", SERVICE)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
            .build();
    return http.send(req, HttpResponse.BodyHandlers.ofString());
  }
}
