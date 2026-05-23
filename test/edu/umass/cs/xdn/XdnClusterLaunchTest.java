package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test for {@code xdn cluster launch}.
 *
 * <p>Launches a 3-replica etcd ensemble through XDN's pass-through {@code
 * StatefulClusterReplicaCoordinator}, then proves both halves of the design:
 *
 * <ul>
 *   <li>The cluster's own consensus works: a key written through replica-0 is observable on
 *       replicas 1 and 2 over their XDN HTTP frontends.
 *   <li>XDN reports the new coordinator + consistency through its replica-info endpoint.
 * </ul>
 *
 * <p>Requires Docker, a swarm-initialized daemon (the test ensures this), and the {@code
 * xdn-etcd-cluster:test} image built from {@code services/etcd-cluster/}.
 */
public class XdnClusterLaunchTest {

  private static final String SERVICE = "etcd-cluster-test";
  private static final int CLIENT_PORT = 2379;
  private static final int PEER_PORT = 2380;
  private static final Duration LEADER_ELECTION_BUDGET = Duration.ofSeconds(45);

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  @Test
  public void testEtcdClusterLaunch() throws Exception {
    assertTrue(XdnTestCluster.isDockerAvailable(), "docker is required for this test");
    assertTrue(
        XdnTestCluster.ensureDockerSwarm(),
        "cluster services need Swarm overlay networking — `docker swarm init` failed");
    assertTrue(
        XdnTestCluster.buildEtcdClusterImage(),
        "could not build xdn-etcd-cluster:test from services/etcd-cluster/");

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchClusterService(
          SERVICE, "xdn-etcd-cluster:test", "/etcd-data/", CLIENT_PORT, PEER_PORT, 3, "etcd");

      // Wait for every replica's frontend to forward successfully — that's our proxy for
      // "the cluster has elected a leader and is serving the v3 API."
      long deadline = System.nanoTime() + LEADER_ELECTION_BUDGET.toNanos();
      for (int idx = 0; idx < 3; idx++) {
        waitForHealth(cluster, idx, deadline);
      }

      // Write a key through replica 0 and read it back through replicas 1 and 2. If etcd's
      // own Raft is working and XDN is forwarding correctly, this round-trip succeeds.
      String key = "xdn-cluster-test-key";
      String value = "hello-cluster";
      assertEquals(200, putKey(cluster, 0, key, value).statusCode(), "etcd PUT via replica 0");

      for (int idx = 1; idx <= 2; idx++) {
        HttpResponse<String> rr = rangeKey(cluster, idx, key);
        assertEquals(200, rr.statusCode(), "etcd RANGE via replica " + idx);
        String decoded = decodeFirstKv(rr.body());
        assertEquals(value, decoded, "key read from replica " + idx + " disagrees with replica 0");
      }

      // Check replica-info advertises the new coordinator + consistency tag.
      HttpResponse<String> info =
          cluster.sendGetRequest(SERVICE, "/api/v2/services/" + SERVICE + "/replica/info");
      assertEquals(200, info.statusCode(), "replica/info request failed");
      JSONObject infoJson = new JSONObject(info.body());
      assertEquals(
          "StatefulClusterReplicaCoordinator",
          infoJson.getString("protocol"),
          "expected the cluster coordinator");
      assertEquals(
          "cluster-managed",
          infoJson.getString("consistency"),
          "expected cluster-managed consistency tag");
    }
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private void waitForHealth(XdnTestCluster cluster, int replicaIdx, long deadlineNs)
      throws Exception {
    Exception last = null;
    while (System.nanoTime() < deadlineNs) {
      try {
        HttpResponse<String> r = cluster.sendGetRequest(SERVICE, replicaIdx, "/health");
        // etcd's /health returns 200 with {"health":"true"} when ready
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
    try {
      body.put("key", Base64.getEncoder().encodeToString(k.getBytes()));
      body.put("value", Base64.getEncoder().encodeToString(v.getBytes()));
    } catch (JSONException e) {
      throw new AssertionError(e);
    }
    return postJson(cluster, replicaIdx, "/v3/kv/put", body.toString());
  }

  private HttpResponse<String> rangeKey(XdnTestCluster cluster, int replicaIdx, String k)
      throws Exception {
    JSONObject body = new JSONObject();
    try {
      body.put("key", Base64.getEncoder().encodeToString(k.getBytes()));
    } catch (JSONException e) {
      throw new AssertionError(e);
    }
    return postJson(cluster, replicaIdx, "/v3/kv/range", body.toString());
  }

  private HttpResponse<String> postJson(
      XdnTestCluster cluster, int replicaIdx, String path, String body) throws Exception {
    int port = cluster.getActiveHttpPort("AR" + replicaIdx);
    HttpRequest req =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + port + path))
            .timeout(Duration.ofSeconds(10))
            .header("XDN", SERVICE)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return http.send(req, HttpResponse.BodyHandlers.ofString());
  }

  private String decodeFirstKv(String rangeResponse) throws JSONException {
    JSONObject json = new JSONObject(rangeResponse);
    if (!json.has("kvs")) {
      return null;
    }
    JSONObject first = json.getJSONArray("kvs").getJSONObject(0);
    return new String(Base64.getDecoder().decode(first.getString("value")));
  }
}
