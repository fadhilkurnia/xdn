package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a primary-backup (Blue-Green) service can be deleted and then created again under
 * the same name. The second incarnation must elect one primary and two backups, serve requests,
 * accept a write, and start from empty state, not from the state of the first incarnation.
 *
 * <p>This test only runs against XdnApp (the Blue-Green primary-backup manager). It is skipped
 * unless the system property XDN_TEST_GIGAPAXOS_CONFIG is gigapaxos.xdn.new-local.properties.
 */
public class XdnPbRecreateTest {

  private static final String XDN_APP_CONFIG = "gigapaxos.xdn.new-local.properties";
  private static final String SERVICE_NAME = "pbrecreatetest";
  private static final int NUM_REPLICAS = 3;
  private static final List<String> ACTIVE_IDS = List.of("AR0", "AR1", "AR2");

  private static final Duration SERVE_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration ROLES_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration DELETE_COMPLETION_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration RECREATE_ACCEPT_TIMEOUT = Duration.ofSeconds(120);

  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  public void testRecreateSameNameAfterDelete() throws Exception {
    assumeTrue(
        XDN_APP_CONFIG.equals(System.getProperty("XDN_TEST_GIGAPAXOS_CONFIG")),
        "This test only runs on XdnApp (" + XDN_APP_CONFIG + ")");
    assertTrue(XdnTestCluster.isDockerAvailable(), "Docker is required for this test");

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      // first incarnation, with one write
      launch(cluster);
      awaitIncarnationReady(cluster, "first");
      String oldTitle = "OldBook-" + System.nanoTime();
      writeBook(cluster, oldTitle);
      HttpResponse<String> before = cluster.sendGetRequest(SERVICE_NAME, 0, "/api/books");
      System.out.println("[recreate-test] books before delete: " + before.body());
      assertTrue(
          before.body().contains(oldTitle), "The first write is not visible: " + before.body());

      // delete, then wait until the reconfigurator removed the name record
      cluster.deleteService(SERVICE_NAME);
      awaitServiceDeleted(cluster);
      System.out.println("[recreate-test] first incarnation deleted");

      // second incarnation under the same name. The name record lingers for a moment after the
      // delete and a create in that window is refused, so retry until it is accepted.
      long deadline = System.currentTimeMillis() + RECREATE_ACCEPT_TIMEOUT.toMillis();
      Exception lastFailure = null;
      boolean accepted = false;
      int attempts = 0;
      while (System.currentTimeMillis() < deadline) {
        attempts++;
        try {
          launch(cluster);
          accepted = true;
          break;
        } catch (Exception e) {
          lastFailure = e;
          Thread.sleep(1000);
        }
      }
      System.out.println(
          "[recreate-test] re-create accepted=" + accepted + " attempts=" + attempts);
      assertTrue(accepted, "Re-create was never accepted; last failure: " + lastFailure);

      // the second incarnation must come up and elect a primary
      awaitIncarnationReady(cluster, "second");

      // and it must start from empty state, then accept a new write
      for (int i = 0; i < NUM_REPLICAS; i++) {
        HttpResponse<String> r = cluster.sendGetRequest(SERVICE_NAME, i, "/api/books");
        System.out.println("[recreate-test] books on " + ACTIVE_IDS.get(i) + ": " + r.body());
        assertEquals(200, r.statusCode(), "Replica " + i + " did not answer 200");
        assertFalse(
            r.body().contains(oldTitle),
            "Replica " + i + " still shows state of the first incarnation: " + r.body());
      }
      String newTitle = "NewBook-" + System.nanoTime();
      writeBook(cluster, newTitle);
      HttpResponse<String> after = cluster.sendGetRequest(SERVICE_NAME, 0, "/api/books");
      assertTrue(
          after.body().contains(newTitle), "The write to the second incarnation is not visible");
    }
  }

  private void launch(XdnTestCluster cluster) throws Exception {
    JSONArray requests = new JSONArray();
    requests.put(
        new JSONObject()
            .put("path_prefix", "/")
            .put("methods", "GET,OPTIONS,HEAD")
            .put("behavior", "read_only"));
    requests.put(
        new JSONObject()
            .put("path_prefix", "/")
            .put("methods", "PUT,POST,DELETE")
            .put("behavior", "write_only"));
    cluster.launchService(
        SERVICE_NAME,
        "fadhilkurnia/xdn-bookcatalog",
        "/app/data/",
        "SEQUENTIAL",
        false,
        requests,
        "/api/books",
        null);
  }

  // Every replica serves reads and the roles settle at 1 primary and 2 backups.
  private void awaitIncarnationReady(XdnTestCluster cluster, String label) throws Exception {
    for (int i = 0; i < NUM_REPLICAS; i++) {
      awaitBooksEndpoint(cluster, i, SERVE_TIMEOUT, label);
    }
    awaitRoles(cluster, ROLES_TIMEOUT, label);
  }

  private void writeBook(XdnTestCluster cluster, String title) throws Exception {
    JSONObject book = new JSONObject().put("title", title).put("author", "Recreate Author");
    int port0 = cluster.getActiveHttpPort(ACTIVE_IDS.get(0));
    HttpRequest post =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + port0 + "/api/books"))
            .timeout(Duration.ofSeconds(20))
            .header("XDN", SERVICE_NAME)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(book.toString()))
            .build();
    HttpResponse<String> resp = client.send(post, HttpResponse.BodyHandlers.ofString());
    assertEquals(2, resp.statusCode() / 100, "Write failed: " + resp.statusCode());
  }

  private void awaitBooksEndpoint(XdnTestCluster cluster, int idx, Duration timeout, String label)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<String> r =
            cluster.sendGetRequest(SERVICE_NAME, idx, "/api/books", Duration.ofSeconds(3));
        if (r.statusCode() == 200) {
          return;
        }
        last = new IllegalStateException("HTTP " + r.statusCode());
      } catch (Exception e) {
        last = e;
      }
      Thread.sleep(1000);
    }
    throw new RuntimeException(
        "The " + label + " incarnation never served /api/books on replica " + idx, last);
  }

  private String getRole(XdnTestCluster cluster, int idx) throws Exception {
    HttpResponse<String> r =
        cluster.sendGetRequest(
            SERVICE_NAME, idx, "/api/v2/services/" + SERVICE_NAME + "/replica/info");
    if (r.statusCode() != 200) {
      return "unknown";
    }
    return new JSONObject(r.body()).optString("role", "unknown");
  }

  private void awaitRoles(XdnTestCluster cluster, Duration timeout, String label) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    String last = "";
    while (System.nanoTime() < deadline) {
      int primaries = 0;
      int backups = 0;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < NUM_REPLICAS; i++) {
        String role;
        try {
          role = getRole(cluster, i);
        } catch (Exception e) {
          role = "error";
        }
        sb.append(ACTIVE_IDS.get(i)).append('=').append(role).append(' ');
        if ("primary".equals(role)) primaries++;
        if ("backup".equals(role)) backups++;
      }
      last = sb.toString();
      if (primaries == 1 && backups == NUM_REPLICAS - 1) {
        System.out.println("[recreate-test] " + label + " roles: " + last);
        return;
      }
      Thread.sleep(1000);
    }
    fail(
        "The "
            + label
            + " incarnation roles never settled to 1 primary and 2 backups. Last: "
            + last);
  }

  // Polls the reconfigurator until the service name record no longer exists.
  private void awaitServiceDeleted(XdnTestCluster cluster) throws Exception {
    String endpoint =
        "http://127.0.0.1:%d/api/v2/services/%s"
            .formatted(cluster.getReconfiguratorHttpPort(), SERVICE_NAME);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .timeout(XdnTestCluster.REQUEST_TIMEOUT)
            .GET()
            .build();
    long deadline = System.currentTimeMillis() + DELETE_COMPLETION_TIMEOUT.toMillis();
    String lastBody = "";
    while (System.currentTimeMillis() < deadline) {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      lastBody = response.body();
      if (lastBody.contains("NONEXISTENT_NAME_ERROR")) {
        return;
      }
      Thread.sleep(1000);
    }
    fail("Service " + SERVICE_NAME + " was not deleted in time; last response: " + lastBody);
  }
}
