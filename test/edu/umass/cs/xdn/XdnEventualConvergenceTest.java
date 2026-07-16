package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.util.XdnTestCluster;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Integration test for the frontier-based anti-entropy eventual-consistency coordinator
 * (LazyReplicaCoordinator). Uses fadhilkurnia/xdn-randstate: a NON-deterministic service (each
 * write appends a random row, and each replica also boots with different random state) exposing GET
 * /hash, a sha256 digest of its whole on-disk state. Re-executing writes therefore never makes
 * replicas byte-identical; only checkpoint-based anti-entropy can, which is exactly what this test
 * asserts: after writes to all three replicas stop, all replicas eventually report the same state
 * hash.
 */
public class XdnEventualConvergenceTest {

  private static final HttpClient HTTP_CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  private static final String SERVICE_IMAGE = "fadhilkurnia/xdn-randstate";
  private static final int NUM_REPLICAS = 3;

  @Test
  public void testConvergenceWithWritesToAllReplicas() throws Exception {
    assertTrue(
        XdnTestCluster.isDockerAvailable(), "Docker is required for this XDN integration test");

    // Speed up anti-entropy so convergence happens well within the test deadline.
    Config.register(new String[] {"XDN_EVENTUAL_ANTI_ENTROPY_INTERVAL_MS=1000"});

    String serviceName = "randstate_eventual_convergence_test";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      // Launch WITHOUT request matchers and with deterministic=false: default matchers
      // classify POST as read-modify-write, which the relaxed EVENTUAL dispatch must
      // still route to the LazyReplicaCoordinator (previously this would have fallen
      // back to primary-backup).
      cluster.launchService(serviceName, SERVICE_IMAGE, "/app/data/", "EVENTUAL", false, null);

      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);
      for (int replicaIdx = 0; replicaIdx < NUM_REPLICAS; replicaIdx++) {
        cluster.awaitReplicaReady(serviceName, replicaIdx, XdnTestCluster.SERVICE_READY_TIMEOUT);
      }

      // Diverge: write directly to EVERY replica. Each write appends a random row at the
      // receiving replica (and random re-execution at peers via the write-after fast path
      // still yields different bytes), so replica states are guaranteed to differ.
      int numWritesPerReplica = 3;
      for (int replicaIdx = 0; replicaIdx < NUM_REPLICAS; replicaIdx++) {
        for (int i = 0; i < numWritesPerReplica; i++) {
          HttpResponse<String> writeResponse =
              sendPostRequest(cluster, serviceName, replicaIdx, "/write");
          assertTrue(
              writeResponse.statusCode() >= 200 && writeResponse.statusCode() < 300,
              String.format(
                  "write %d to replica %d failed with status %d: %s",
                  i, replicaIdx, writeResponse.statusCode(), writeResponse.body()));
        }
      }

      // Converge: poll every replica's state hash until all three are identical. A replica
      // may briefly refuse connections while it restarts to install a checkpoint; treat
      // that as not-converged-yet rather than a failure.
      String[] lastHashes = new String[NUM_REPLICAS];
      long deadline = System.currentTimeMillis() + 120_000;
      boolean converged = false;
      while (System.currentTimeMillis() < deadline && !converged) {
        boolean allReadable = true;
        for (int replicaIdx = 0; replicaIdx < NUM_REPLICAS; replicaIdx++) {
          try {
            HttpResponse<String> hashResponse =
                cluster.sendGetRequest(serviceName, replicaIdx, "/hash", Duration.ofSeconds(5));
            if (hashResponse.statusCode() != 200 || hashResponse.body().isEmpty()) {
              allReadable = false;
              break;
            }
            lastHashes[replicaIdx] = hashResponse.body().trim();
          } catch (IOException e) {
            allReadable = false;
            break;
          }
        }
        converged =
            allReadable
                && lastHashes[0] != null
                && lastHashes[0].equals(lastHashes[1])
                && lastHashes[1].equals(lastHashes[2]);
        if (!converged) {
          Thread.sleep(500);
        }
      }
      assertTrue(
          converged,
          "replicas did not converge to the same state hash under anti-entropy: "
              + Arrays.toString(lastHashes));
      System.out.println("Replicas converged with state hash: " + lastHashes[0]);

      // Stability: the hashes must still be identical after three more anti-entropy
      // rounds, guarding against having sampled a transient coincidence.
      Thread.sleep(3_000);
      String stableHash = null;
      for (int replicaIdx = 0; replicaIdx < NUM_REPLICAS; replicaIdx++) {
        HttpResponse<String> hashResponse =
            cluster.sendGetRequest(serviceName, replicaIdx, "/hash", Duration.ofSeconds(5));
        assertEquals(200, hashResponse.statusCode(), "hash read failed after convergence");
        if (stableHash == null) {
          stableHash = hashResponse.body().trim();
        } else {
          assertEquals(
              stableHash,
              hashResponse.body().trim(),
              "replica " + replicaIdx + " diverged again after convergence");
        }
      }
    }
  }

  @Test
  public void testEventualServiceUsesLazyCoordinator() throws Exception {
    assertTrue(
        XdnTestCluster.isDockerAvailable(), "Docker is required for this XDN integration test");

    String serviceName = "randstate_eventual_protocol_test";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      // Non-deterministic service with default matchers: under the relaxed dispatch, an
      // explicit EVENTUAL declaration must route to the LazyReplicaCoordinator instead of
      // primary-backup.
      cluster.launchService(serviceName, SERVICE_IMAGE, "/app/data/", "EVENTUAL", false, null);
      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      HttpResponse<String> infoResponse =
          cluster.sendGetRequest(
              serviceName, 0, "/api/v2/services/" + serviceName + "/replica/info");
      assertEquals(200, infoResponse.statusCode(), "replica/info failed: " + infoResponse.body());

      JSONObject info = new JSONObject(infoResponse.body());
      assertEquals(
          "LazyReplicaCoordinator",
          info.getString("protocol"),
          "EVENTUAL service must use the LazyReplicaCoordinator: " + infoResponse.body());
      assertEquals(
          "EVENTUAL",
          info.getString("consistency").toUpperCase(),
          "unexpected consistency: " + infoResponse.body());
    }
  }

  private HttpResponse<String> sendPostRequest(
      XdnTestCluster cluster, String serviceName, int replicaIdx, String endpoint)
      throws IOException, InterruptedException {
    int httpPort = cluster.getActiveHttpPort("AR" + replicaIdx);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + httpPort + endpoint))
            .timeout(XdnTestCluster.REQUEST_TIMEOUT)
            .header("XDN", serviceName)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
