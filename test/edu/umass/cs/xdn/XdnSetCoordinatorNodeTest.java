package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import org.json.JSONObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class XdnSetCoordinatorNodeTest {

  private static final List<String> DEFAULT_REPLICAS = List.of("AR0", "AR1", "AR2");
  private static final int REPLICA_COUNT = DEFAULT_REPLICAS.size();
  private static final HttpClient HTTP_CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  // Because changing leader in Paxos uses best-effort approach, we might
  // need to try several times before the leader actually changed.
  private static final int MAX_SET_COORDINATOR_ATTEMPT = 3;

  @Test
  public void testSetCoordinatorChangesLeader_Paxos() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdn-set-coordinator-test";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceName, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true);

      Thread.sleep(2000); // wait for service to be created

      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      Map<String, String> initialRoles =
          awaitReplicaRoles(cluster, serviceName, Duration.ofSeconds(60));
      assertEquals(
          REPLICA_COUNT,
          initialRoles.size(),
          "Failed to retrieve role information from all replicas");

      String initialLeader = findReplicaByRole(initialRoles, "leader");
      assertNotNull(initialLeader, "Expected to observe a leader before changing coordinator");
      System.out.println(">>> Detected initial leader: " + initialLeader);

      for (String newLeader : DEFAULT_REPLICAS) {
        System.out.println(">>> Changing leader into " + newLeader);
        int currAttempt = 0;
        String detectedLeader = null;
        Set<String> detectedFollowers = new HashSet<>();
        while (currAttempt < MAX_SET_COORDINATOR_ATTEMPT) {
          detectedLeader = null;
          detectedFollowers.clear();
          HttpResponse<String> setCoordinatorResponse =
              sendSetCoordinatorRequest(cluster, serviceName, newLeader);
          assertEquals(
              200,
              setCoordinatorResponse.statusCode(),
              "Set coordinator request failed: " + setCoordinatorResponse.body());

          Map<String, String> roles = fetchReplicaRoles(cluster, serviceName);
          for (var pair : roles.entrySet()) {
            if (pair.getValue().equals("leader")) {
              detectedLeader = pair.getKey();
            }
            if (pair.getValue().equals("follower")) {
              detectedFollowers.add(pair.getKey());
            }
          }

          if (newLeader.equals(detectedLeader)) {
            break;
          }

          Thread.sleep(Duration.ofSeconds(1));
          currAttempt++;
          if (currAttempt != MAX_SET_COORDINATOR_ATTEMPT) {
            System.out.println(">>> Fail to change the leader, retrying ...");
          }
        }

        Set<String> expectedFollowers = new HashSet<>();
        for (var r : DEFAULT_REPLICAS) {
          if (!r.equals(newLeader)) expectedFollowers.add(r);
        }

        assertEquals(newLeader, detectedLeader);
        assertEquals(detectedFollowers, expectedFollowers);
        assertTrue(currAttempt <= MAX_SET_COORDINATOR_ATTEMPT);
      }
    }
  }

  @Test
  @Disabled
  public void testSetCoordinatorChangesLeader_PrimaryBackup() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdn-set-coordinator-test-pb";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceName, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", false);

      Thread.sleep(2000); // wait for service to be created

      cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      Map<String, String> initialRoles =
          awaitReplicaRoles(cluster, serviceName, Duration.ofSeconds(60));
      assertEquals(
          REPLICA_COUNT,
          initialRoles.size(),
          "Failed to retrieve role information from all replicas");

      String initialPrimary = findReplicaByRole(initialRoles, "primary");
      assertNotNull(initialPrimary, "Expected to observe a primary before changing coordinator");
      System.out.println(">>> Detected initial primary: " + initialPrimary);

      // TODO: implement checking for change primary
      for (String newPrimary : DEFAULT_REPLICAS) {
        System.out.println(">>> Changing primary into " + newPrimary);
        int currAttempt = 0;
        String detectedLeader = null;
        Set<String> detectedBackups = new HashSet<>();
        while (currAttempt < MAX_SET_COORDINATOR_ATTEMPT) {
          detectedLeader = null;
          detectedBackups.clear();
          HttpResponse<String> setCoordinatorResponse =
              sendSetCoordinatorRequest(cluster, serviceName, newPrimary);
          assertEquals(
              200,
              setCoordinatorResponse.statusCode(),
              "Set coordinator request failed: " + setCoordinatorResponse.body());

          Thread.sleep(Duration.ofSeconds(2)); // wait until the primary is changed

          Map<String, String> roles = fetchReplicaRoles(cluster, serviceName);
          for (var pair : roles.entrySet()) {
            if (pair.getValue().equals("primary")) {
              detectedLeader = pair.getKey();
            }
            if (pair.getValue().equals("backup")) {
              detectedBackups.add(pair.getKey());
            }
          }

          if (newPrimary.equals(detectedLeader)) {
            break;
          }

          Thread.sleep(Duration.ofSeconds(1));
          currAttempt++;
          if (currAttempt != MAX_SET_COORDINATOR_ATTEMPT) {
            System.out.println(">>> Fail to change the primary, retrying ...");
          }
        }

        Set<String> expectedBackups = new HashSet<>();
        for (var r : DEFAULT_REPLICAS) {
          if (!r.equals(newPrimary)) expectedBackups.add(r);
        }

        assertEquals(newPrimary, detectedLeader);
        assertEquals(detectedBackups, expectedBackups);
        assertTrue(currAttempt <= MAX_SET_COORDINATOR_ATTEMPT);
      }
    }
  }

  private HttpResponse<String> sendSetCoordinatorRequest(
      XdnTestCluster cluster, String serviceName, String newCoordinator) throws Exception {
    String endpoint =
        "http://127.0.0.1:"
            + cluster.getReconfiguratorHttpPort()
            + "/api/v2/services/"
            + serviceName
            + "/coordinator";
    JSONObject payload = new JSONObject();
    payload.put("newCoordinatorNodeId", newCoordinator);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .timeout(XdnTestCluster.REQUEST_TIMEOUT)
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(payload.toString()))
            .build();

    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private Map<String, String> awaitReplicaRoles(
      XdnTestCluster cluster, String serviceName, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    Map<String, String> roles = new HashMap<>();

    while (System.currentTimeMillis() < deadline) {
      roles = fetchReplicaRoles(cluster, serviceName);
      if (roles.size() == REPLICA_COUNT) {
        return roles;
      }
      Thread.sleep(500);
    }

    return roles;
  }

  private Map<String, String> fetchReplicaRoles(XdnTestCluster cluster, String serviceName)
      throws Exception {
    Map<String, String> roleByReplica = new HashMap<>();
    String endpoint = "/api/v2/services/" + serviceName + "/replica/info";

    for (int replicaIdx = 0; replicaIdx < REPLICA_COUNT; replicaIdx++) {
      HttpResponse<String> infoResponse =
          cluster.sendGetRequest(serviceName, replicaIdx, endpoint, XdnTestCluster.REQUEST_TIMEOUT);
      if (infoResponse.statusCode() != 200) {
        continue;
      }
      JSONObject infoJson = new JSONObject(infoResponse.body());
      String replicaId = infoJson.optString("replica", "");
      String role = infoJson.optString("role", "").toLowerCase();
      if (!replicaId.isEmpty() && !role.isEmpty()) {
        roleByReplica.put(replicaId, role);
      }
    }

    return roleByReplica;
  }

  private String findReplicaByRole(Map<String, String> roleByReplica, String role) {
    for (Map.Entry<String, String> entry : roleByReplica.entrySet()) {
      if (role.equals(entry.getValue())) {
        return entry.getKey();
      }
    }
    return null;
  }
}
