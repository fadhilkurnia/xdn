package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

public class XdnGetReplicaInfoTest {

  @Test
  public void testGetReplicaInfoSingleService() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdntestservice";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      // Launch a service with Paxos replication (deterministic=true)
      cluster.launchService(
          serviceName, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true);

      Thread.sleep(2000); // wait for service to be created

      System.out.println("Checking service connectivity");
      HttpResponse<String> response =
          cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      // Service should be ready and running
      assertEquals(308, response.statusCode(), "Service did not return HTTP 308");
      assertFalse(response.body().isEmpty(), "Service returned empty body");

      // Get replica info for the service
      HttpResponse<String> infoResponse =
          cluster.sendGetRequest(serviceName, "/api/v2/services/" + serviceName + "/replica/info");
      assertEquals(200, infoResponse.statusCode(), "Replica info request failed");

      // Parse and validate response
      JSONObject info = new JSONObject(infoResponse.body());

      // Verify protocol information
      assertNotNull(info.getString("replica"), "Missing replica ID");
      assertEquals(
          "PaxosReplicaCoordinator",
          info.getString("protocol"),
          "Expected PaxosReplicaCoordinator protocol");
      assertEquals(
          "LINEARIZABLE",
          info.getString("requestedConsistency"),
          "Wrong requested consistency model");
      assertEquals("LINEARIZABILITY", info.getString("consistency"), "Wrong consistency model");
      assertNotNull(info.getString("role"), "Missing protocol role");

      // Verify container metadata
      JSONArray containers = info.getJSONArray("containers");
      assertNotNull(containers, "Missing containers array");
      assertTrue(containers.length() > 0, "No containers found");

      // At least one container should be running
      boolean hasRunningContainer = false;
      for (int i = 0; i < containers.length(); i++) {
        JSONObject container = containers.getJSONObject(i);

        // Each container should have all required fields
        assertNotNull(container.getString("id"), "Container missing ID");
        assertNotNull(container.getString("createdAt"), "Container missing creation time");
        assertNotNull(container.getString("status"), "Container missing status");

        // Creation time should be in human readable format
        assertTrue(
            container.getString("createdAt").contains(" ago"),
            "Creation time not in human readable format");

        if ("running".equalsIgnoreCase(container.getString("status"))) {
          hasRunningContainer = true;
        }
      }
      assertTrue(hasRunningContainer, "No running containers found");
    }
  }

  @Test
  public void testGetReplicaInfoNonExistentService() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      // Try to get info for non-existent service
      HttpResponse<String> infoResponse =
          cluster.sendGetRequest(
              "nonexistentservice", "/api/v2/services/nonexistentservice/replica/info");

      // Should return an error
      assertEquals(404, infoResponse.statusCode(), "Expected 404 for non-existent service");
    }
  }

  @Test
  public void testGetPaxosReplicaInfo() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdntestservice-paxos";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceName, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true);

      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      int replicaCount = 3;
      Map<String, String> roleByReplica = new HashMap<>();
      boolean hasLeaderAndFollowers = false;
      long deadline = System.currentTimeMillis() + 60_000;

      while (System.currentTimeMillis() < deadline && !hasLeaderAndFollowers) {
        roleByReplica.clear();
        int leaderCount = 0;
        int followerCount = 0;

        for (int replicaIdx = 0; replicaIdx < replicaCount; replicaIdx++) {
          HttpResponse<String> infoResponse =
              cluster.sendGetRequest(
                  serviceName, replicaIdx, "/api/v2/services/" + serviceName + "/replica/info");

          if (infoResponse.statusCode() != 200) {
            continue;
          }

          JSONObject info = new JSONObject(infoResponse.body());
          String replicaId = info.optString("replica", "");
          String role = info.optString("role", "").toLowerCase();

          if (replicaId.isEmpty() || role.isEmpty()) {
            continue;
          }

          roleByReplica.put(replicaId, role);
          if ("leader".equals(role)) {
            leaderCount++;
          } else if ("follower".equals(role)) {
            followerCount++;
          }
        }

        hasLeaderAndFollowers =
            roleByReplica.size() == replicaCount
                && leaderCount == 1
                && followerCount == replicaCount - 1;

        if (!hasLeaderAndFollowers) {
          Thread.sleep(500);
        }
      }

      assertTrue(
          hasLeaderAndFollowers, "Timed out waiting for leader and follower roles to stabilize");
      assertEquals(replicaCount, roleByReplica.size(), "Each replica should report its role");

      int leaderCount = 0;
      int followerCount = 0;
      for (String role : roleByReplica.values()) {
        if ("leader".equals(role)) {
          leaderCount++;
        } else if ("follower".equals(role)) {
          followerCount++;
        }
      }

      assertEquals(1, leaderCount, "Expected exactly one leader replica");
      assertEquals(replicaCount - 1, followerCount, "Expected remaining replicas to be followers");
    }
  }

  @Test
  @DisabledIfEnvironmentVariable(named = "RUNNER_ENVIRONMENT", matches = "github-hosted")
  public void testGetPrimaryBackupReplicaInfo() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdntestservice-pb";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceName, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", false);

      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      int replicaCount = 3;
      Map<String, String> roleByReplica = new HashMap<>();
      boolean hasPrimaryAndBackups = false;
      long deadline = System.currentTimeMillis() + 60_000;

      while (System.currentTimeMillis() < deadline && !hasPrimaryAndBackups) {
        roleByReplica.clear();
        int primaryCount = 0;
        int backupCount = 0;

        for (int replicaIdx = 0; replicaIdx < replicaCount; replicaIdx++) {
          HttpResponse<String> infoResponse =
              cluster.sendGetRequest(
                  serviceName, replicaIdx, "/api/v2/services/" + serviceName + "/replica/info");

          if (infoResponse.statusCode() != 200) {
            continue;
          }

          JSONObject info = new JSONObject(infoResponse.body());
          String replicaId = info.optString("replica", "");
          String role = info.optString("role", "").toLowerCase();

          if (replicaId.isEmpty() || role.isEmpty()) {
            continue;
          }

          roleByReplica.put(replicaId, role);
          if ("primary".equals(role)) {
            primaryCount++;
          } else if ("backup".equals(role)) {
            backupCount++;
          }
        }

        hasPrimaryAndBackups =
            roleByReplica.size() == replicaCount
                && primaryCount == 1
                && backupCount == replicaCount - 1;

        if (!hasPrimaryAndBackups) {
          Thread.sleep(500);
        }
      }

      assertTrue(
          hasPrimaryAndBackups, "Timed out waiting for primary and backup roles to stabilize");
      assertEquals(replicaCount, roleByReplica.size(), "Each replica should report its role");

      int primaryCount = 0;
      int backupCount = 0;
      for (String role : roleByReplica.values()) {
        if ("primary".equals(role)) {
          primaryCount++;
        } else if ("backup".equals(role)) {
          backupCount++;
        }
      }

      assertEquals(1, primaryCount, "Expected exactly one primary replica");
      assertEquals(replicaCount - 1, backupCount, "Expected remaining replicas to be backups");
    }
  }
}
