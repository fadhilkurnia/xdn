package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for ActiveReplica restart recovery: a restarted replica must re-register the
 * services it hosts (no permanent 404s) and roll its container state forward from the paxos log (no
 * silent divergence from its peers).
 *
 * <p>Both bugs shipped together historically: gigapaxos log recovery replays straight into {@code
 * XdnGigapaxosApp.restore()} without repopulating {@code XdnReplicaCoordinator}'s routing map, and
 * recovery replay ran through {@code PrimaryBackupMiddlewareApp} before its manager was wired, so
 * every replayed decision threw under {@code -ea}, exhausted gigapaxos's execute retries, and was
 * silently dropped -- the restarted replica then executed later writes on empty state (e.g.
 * assigning {@code id:1} to a write its peers executed as {@code id:2}).
 *
 * <p>The restarted replica runs as a separate OS process (see {@code XdnTestCluster#start}); the
 * restart is SIGTERM + respawn on the same on-disk state, the same recovery path a {@code systemctl
 * restart xdn-ar} (or crash) takes in production.
 */
public class XdnRestartRecoveryTest {

  private static final String IMAGE = "fadhilkurnia/xdn-bookcatalog";
  // Post-restart probes go through full coordination and can ride out leader-election and
  // request-retry windows, so this is deliberately more generous than SERVICE_READY_TIMEOUT.
  private static final Duration CONVERGENCE_TIMEOUT = Duration.ofSeconds(180);

  @Test
  public void testRestartedReplicaDoesNotDivergeLinearizable() throws Exception {
    assertTrue(XdnTestCluster.isDockerAvailable(), "Docker is required for this integration test");

    String serviceName = "xdnrestartlin";
    try (XdnTestCluster cluster = new XdnTestCluster()) {
      // AR1 runs as an external OS process so it can be killed and respawned like a real
      // systemd restart; see the start(...) javadoc for why an in-JVM restart cannot work.
      cluster.start("AR1");
      cluster.launchService(
          serviceName, IMAGE, "/app/data/", "LINEARIZABLE", true, "/api/books", null);
      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);
      for (int i = 0; i < 3; i++) {
        cluster.awaitReplicaReady(serviceName, i, XdnTestCluster.SERVICE_READY_TIMEOUT);
      }

      HttpResponse<String> post =
          cluster.sendPostRequest(
              serviceName,
              0,
              "/api/books",
              "{\"title\":\"Committed Before Restart\",\"author\":\"A\"}",
              null);
      assertEquals(200, post.statusCode(), "pre-restart write failed: " + post.body());

      // All replicas agree before the restart.
      String baseline = awaitBooks(cluster, serviceName, 0, 1);
      for (int i = 1; i < 3; i++) {
        assertEquals(baseline, awaitBooks(cluster, serviceName, i, 1));
      }

      // Restart one member; its on-disk paxos logs and XDN state survive, so the fresh
      // process recovers via log replay.
      int restartedIdx = 1;
      cluster.restartActiveReplica(restartedIdx);

      // The recovered replica must serve the committed item (routing repaired) with content
      // identical to a non-restarted peer (state rolled forward, not re-initialized empty).
      String restartedView = awaitBooks(cluster, serviceName, restartedIdx, 1);
      String peerView = awaitBooks(cluster, serviceName, 0, 1);
      assertEquals(peerView, restartedView, "restarted replica diverged from its peer");

      // A write through the restarted replica must sequence on top of the recovered state
      // (the divergence bug surfaced as this write getting id:1 instead of id:2).
      HttpResponse<String> postAfter =
          cluster.sendPostRequest(
              serviceName,
              restartedIdx,
              "/api/books",
              "{\"title\":\"Committed After Restart\",\"author\":\"B\"}",
              null);
      assertEquals(200, postAfter.statusCode(), "post-restart write failed: " + postAfter.body());

      String finalView = awaitBooks(cluster, serviceName, 0, 2);
      assertDistinctIds(finalView);
      for (int i = 0; i < 3; i++) {
        assertEquals(finalView, awaitBooks(cluster, serviceName, i, 2));
      }
    }
  }

  @Test
  @Disabled(
      "Primary-backup LAUNCH never completes in this environment: duplicate xdn-pb-init threads"
          + " race the recorder's wipe-and-recreate of the bind-mount source, so `docker run`"
          + " livelocks on 'bind source path does not exist' (Docker Desktop validates bind"
          + " sources eagerly). Enable once the PB launch path is fixed; see issue #82.")
  public void testRestartedPrimaryKeepsCommittedStatePrimaryBackup() throws Exception {
    assertTrue(XdnTestCluster.isDockerAvailable(), "Docker is required for this integration test");

    String serviceName = "xdnrestartpbx";
    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();
      // deterministic=false selects the primary-backup coordinator.
      cluster.launchService(
          serviceName, IMAGE, "/app/data/", "LINEARIZABLE", false, "/api/books", null);
      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);
      for (int i = 0; i < 3; i++) {
        cluster.awaitReplicaReady(serviceName, i, XdnTestCluster.SERVICE_READY_TIMEOUT);
      }

      HttpResponse<String> post =
          cluster.sendPostRequest(
              serviceName,
              0,
              "/api/books",
              "{\"title\":\"Committed Before Restart\",\"author\":\"A\"}",
              null);
      assertEquals(200, post.statusCode(), "pre-restart write failed: " + post.body());
      String baseline = awaitBooks(cluster, serviceName, 0, 1);

      // Restarting the PRIMARY is the interesting case for primary-backup: committed state
      // must survive whether the old primary recovers or a backup takes over.
      int primaryIdx = findPrimaryIdx(cluster, serviceName);
      cluster.restartActiveReplica(primaryIdx);

      // Committed data must remain readable through every frontend, byte-identical.
      for (int i = 0; i < 3; i++) {
        assertEquals(
            baseline,
            awaitBooks(cluster, serviceName, i, 1),
            "replica " + i + " lost or diverged committed state after primary restart");
      }

      // And the group must still accept writes that sequence on the surviving state.
      HttpResponse<String> postAfter =
          cluster.sendPostRequest(
              serviceName,
              0,
              "/api/books",
              "{\"title\":\"Committed After Restart\",\"author\":\"B\"}",
              null);
      assertEquals(200, postAfter.statusCode(), "post-restart write failed: " + postAfter.body());

      String finalView = awaitBooks(cluster, serviceName, 0, 2);
      assertDistinctIds(finalView);
      for (int i = 0; i < 3; i++) {
        assertEquals(finalView, awaitBooks(cluster, serviceName, i, 2));
      }
    }
  }

  /**
   * Polls one replica's frontend until {@code GET /api/books} returns 200 with at least {@code
   * minBooks} entries, returning the body. Connection errors are retried: the replica may be
   * mid-recovery when polling starts.
   */
  private static String awaitBooks(
      XdnTestCluster cluster, String serviceName, int replicaIdx, int minBooks) throws Exception {
    long deadline = System.nanoTime() + CONVERGENCE_TIMEOUT.toNanos();
    String lastSeen = null;
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<String> response =
            cluster.sendGetRequest(serviceName, replicaIdx, "/api/books");
        lastSeen = response.statusCode() + " " + response.body();
        if (response.statusCode() == 200) {
          JSONArray books = new JSONArray(response.body());
          if (books.length() >= minBooks) {
            return response.body();
          }
        }
      } catch (Exception ignored) {
        // replica still recovering; retry below
      }
      TimeUnit.MILLISECONDS.sleep(500);
    }
    fail(
        "replica %d never served >=%d book(s) within %s; last seen: %s"
            .formatted(replicaIdx, minBooks, CONVERGENCE_TIMEOUT, lastSeen));
    return null; // unreachable
  }

  /** Asserts every book in the JSON array carries a unique id (catches restart re-sequencing). */
  private static void assertDistinctIds(String booksJson) throws Exception {
    JSONArray books = new JSONArray(booksJson);
    java.util.Set<Integer> ids = new java.util.HashSet<>();
    for (int i = 0; i < books.length(); i++) {
      int id = books.getJSONObject(i).getInt("id");
      assertTrue(ids.add(id), "duplicate book id " + id + " in " + booksJson);
    }
  }

  private static int findPrimaryIdx(XdnTestCluster cluster, String serviceName) throws Exception {
    long deadline = System.nanoTime() + CONVERGENCE_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      for (int i = 0; i < 3; i++) {
        try {
          HttpResponse<String> response =
              cluster.sendGetRequest(
                  serviceName, i, "/api/v2/services/" + serviceName + "/replica/info");
          if (response.statusCode() == 200) {
            JSONObject info = new JSONObject(response.body());
            if ("primary".equalsIgnoreCase(info.optString("role"))) {
              return i;
            }
          }
        } catch (Exception ignored) {
          // keep probing
        }
      }
      TimeUnit.MILLISECONDS.sleep(500);
    }
    throw new AssertionError("no replica reported role=primary within " + CONVERGENCE_TIMEOUT);
  }
}
