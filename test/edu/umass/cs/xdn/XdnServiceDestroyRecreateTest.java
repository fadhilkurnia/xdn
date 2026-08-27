package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class XdnServiceDestroyRecreateTest {

  private static final Duration DELETE_COMPLETION_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration RECREATE_TIMEOUT = Duration.ofSeconds(90);

  /**
   * Regression test: destroying a service and re-creating it under the same name used to wedge
   * forever. The reconfiguration drop deleted the app-level state (containers, state dirs) but
   * never removed the paxos-level epoch-final checkpoint, so re-creating {@code name:0} tripped
   * gigapaxos' going-back-in-time check (equalOrHigherVersionStopped) on every node that hosted the
   * previous incarnation, each threw PaxosInstanceCreationException, and the reconfigurator re-sent
   * START_EPOCH indefinitely. Fixed by {@code XdnReplicaCoordinator#deleteFinalState} delegating to
   * the paxos coordinator in addition to the app-level cleanup.
   */
  @Test
  public void testDestroyThenRecreateSameName() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdnsvcrecreate";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      // first incarnation: on a 3-AR cluster the service is placed on every node,
      // so the re-create below hits nodes that hosted the previous incarnation.
      cluster.launchService(
          serviceName, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true, "/api/books", null);
      awaitServiceServing(cluster, serviceName);

      // destroy it and wait until the name record is fully removed
      cluster.deleteService(serviceName);
      awaitServiceDeleted(cluster, serviceName);

      // Second incarnation under the same name must come up again. The record lingers in
      // WAIT_DELETE for a moment after the GET above starts reporting NONEXISTENT, and a
      // create that lands in that window is dropped without a response -- so retry until
      // the reconfigurator accepts it. Pre-fix, the accepted re-create wedged forever
      // (START_EPOCH re-sent indefinitely), so every attempt here kept failing.
      long deadline = System.currentTimeMillis() + RECREATE_TIMEOUT.toMillis();
      Exception lastLaunchFailure = null;
      boolean isRecreated = false;
      while (System.currentTimeMillis() < deadline) {
        try {
          cluster.launchService(
              serviceName, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true, "/api/books", null);
          isRecreated = true;
          break;
        } catch (Exception e) {
          lastLaunchFailure = e;
          Thread.sleep(1000);
        }
      }
      assertTrue(
          isRecreated,
          "Re-creating the destroyed service was never accepted; last failure: "
              + lastLaunchFailure);
      awaitServiceServing(cluster, serviceName);

      HttpResponse<String> apiResponse = cluster.sendGetRequest(serviceName, "/api/books");
      assertEquals(200, apiResponse.statusCode(), "Re-created service did not return HTTP 200");
      assertEquals("[]", apiResponse.body(), "Re-created service did not start from empty state");
    }
  }

  /**
   * Polls the AR frontend until bookcatalog answers its HTTP 308 redirect, i.e. the service is
   * registered and its container is serving. {@link XdnTestCluster#awaitServiceReady} returns on
   * any status below 500 -- including the 404 an AR answers while service registration is still
   * propagating on a slow runner -- so poll for the expected status explicitly.
   */
  private void awaitServiceServing(XdnTestCluster cluster, String serviceName) throws Exception {
    long deadline = System.currentTimeMillis() + XdnTestCluster.SERVICE_READY_TIMEOUT.toMillis();
    Integer lastStatus = null;
    Exception lastError = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        HttpResponse<String> response = cluster.invokeService(serviceName);
        lastStatus = response.statusCode();
        if (lastStatus == 308) {
          assertFalse(response.body().isEmpty(), "Service returned empty body");
          return;
        }
      } catch (Exception e) {
        lastError = e;
      }
      Thread.sleep(1000);
    }
    fail(
        "Service "
            + serviceName
            + " never returned HTTP 308; last status: "
            + lastStatus
            + ", last error: "
            + lastError);
  }

  /** Polls the reconfigurator until the service name record no longer exists. */
  private void awaitServiceDeleted(XdnTestCluster cluster, String serviceName) throws Exception {
    HttpClient httpClient = HttpClient.newHttpClient();
    String endpoint =
        "http://127.0.0.1:%d/api/v2/services/%s"
            .formatted(cluster.getReconfiguratorHttpPort(), serviceName);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .timeout(XdnTestCluster.REQUEST_TIMEOUT)
            .GET()
            .build();
    long deadline = System.currentTimeMillis() + DELETE_COMPLETION_TIMEOUT.toMillis();
    String lastBody = "";
    while (System.currentTimeMillis() < deadline) {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      lastBody = response.body();
      if (lastBody.contains("NONEXISTENT_NAME_ERROR")) {
        return;
      }
      Thread.sleep(1000);
    }
    fail("Service " + serviceName + " was not deleted in time; last response: " + lastBody);
  }
}
