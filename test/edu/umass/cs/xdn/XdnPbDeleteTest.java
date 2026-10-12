package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Verifies the delete of a primary-backup (Blue-Green) service. The delete sends StopEpoch to the
 * replicas. Every replica runs its stop work and acks, then the reconfigurator drops the epoch and
 * removes the name record. If the stop work never acked, the delete would hang and the name record
 * would never go away.
 *
 * <p>This test only runs against XdnApp (the Blue-Green primary-backup manager). It is skipped
 * unless the system property XDN_TEST_GIGAPAXOS_CONFIG is gigapaxos.xdn.new-local.properties.
 */
public class XdnPbDeleteTest {

  private static final String XDN_APP_CONFIG = "gigapaxos.xdn.new-local.properties";
  private static final String SERVICE_NAME = "pbdeletetest";
  private static final int NUM_REPLICAS = 3;
  private static final List<String> ACTIVE_IDS = List.of("AR0", "AR1", "AR2");

  // Containers are named c<i>.e<epoch>.<service>.<node>.xdn.io (NonDeterministicService).
  private static final String CONTAINER_NAME_MARKER = "." + SERVICE_NAME + ".";

  // State directories are /tmp/xdn/state/<recorder>/<node>/<service>/e<epoch>/ or, for fuselog,
  // /tmp/xdn/state/fuselog/<node>/mnt/<service>/e<epoch>/ (AbstractStateDiffRecorder).
  private static final Path STATE_ROOT = Paths.get("/tmp/xdn/state");
  private static final int STATE_SEARCH_MAX_DEPTH = 4;

  private static final Duration DELETE_COMPLETION_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration CLEANUP_TIMEOUT = Duration.ofSeconds(30);

  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  public void testDeleteServiceCompletesAndCleansUp() throws Exception {
    assumeTrue(
        XDN_APP_CONFIG.equals(System.getProperty("XDN_TEST_GIGAPAXOS_CONFIG")),
        "This test only runs on XdnApp (" + XDN_APP_CONFIG + ")");
    assertTrue(XdnTestCluster.isDockerAvailable(), "Docker is required for this test");

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

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

      // every replica must serve reads
      for (int i = 0; i < NUM_REPLICAS; i++) {
        awaitBooksEndpoint(cluster, i, Duration.ofSeconds(90));
      }

      // one primary and two backups
      awaitRoles(cluster, Duration.ofSeconds(60));

      // write one book through replica 0 (a backup forwards it to the primary)
      String title = "DeleteBook-" + System.nanoTime();
      JSONObject book = new JSONObject().put("title", title).put("author", "Delete Author");
      int port0 = cluster.getActiveHttpPort(ACTIVE_IDS.get(0));
      HttpRequest post =
          HttpRequest.newBuilder()
              .uri(URI.create("http://127.0.0.1:" + port0 + "/api/books"))
              .timeout(Duration.ofSeconds(20))
              .header("XDN", SERVICE_NAME)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(book.toString()))
              .build();
      HttpResponse<String> postResp = client.send(post, HttpResponse.BodyHandlers.ofString());
      assertEquals(2, postResp.statusCode() / 100, "Write failed: " + postResp.statusCode());

      // Precondition, so the checks after the delete cannot pass vacuously. The service must have
      // containers and state directories now.
      List<String> containersBefore = listServiceContainers();
      List<Path> stateDirsBefore = findServiceStateDirs();
      System.out.println("[delete-test] containers before delete: " + containersBefore);
      System.out.println("[delete-test] state dirs before delete: " + stateDirsBefore);
      assertFalse(
          containersBefore.isEmpty(),
          "Expected containers matching '" + CONTAINER_NAME_MARKER + "' before the delete");
      assertFalse(
          stateDirsBefore.isEmpty(),
          "Expected a '" + SERVICE_NAME + "' state directory under " + STATE_ROOT);

      // delete, then wait until the reconfigurator removed the name record, which only happens
      // after every replica acked the stop and the epoch was dropped
      long deleteStart = System.nanoTime();
      cluster.deleteService(SERVICE_NAME);
      awaitServiceDeleted(cluster);
      System.out.println(
          "[delete-test] delete completed in "
              + Duration.ofNanos(System.nanoTime() - deleteStart).toMillis()
              + " ms");

      // no replica serves the service anymore
      for (int i = 0; i < NUM_REPLICAS; i++) {
        assertNotServing(cluster, i);
      }

      // containers and state directories are gone on every node
      awaitContainersGone();
      awaitStateDirsGone();
    }
  }

  // Waits until the AR frontend of the replica serves /api/books with HTTP 200.
  private void awaitBooksEndpoint(XdnTestCluster cluster, int idx, Duration timeout)
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
    throw new RuntimeException("Replica " + idx + " never served /api/books", last);
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

  private void awaitRoles(XdnTestCluster cluster, Duration timeout) throws Exception {
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
        System.out.println("[delete-test] roles: " + last);
        return;
      }
      Thread.sleep(1000);
    }
    fail("Roles never settled to 1 primary and 2 backups. Last seen: " + last);
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

  // After the delete, a request must not be answered with HTTP 200. A refused connection or a
  // timeout is also fine.
  private void assertNotServing(XdnTestCluster cluster, int idx) throws Exception {
    try {
      HttpResponse<String> r =
          cluster.sendGetRequest(SERVICE_NAME, idx, "/api/books", Duration.ofSeconds(3));
      System.out.println(
          "[delete-test] replica " + idx + " answered HTTP " + r.statusCode() + " after delete");
      assertNotEquals(
          200, r.statusCode(), "Replica " + idx + " still serves the deleted service: " + r.body());
    } catch (IOException e) {
      System.out.println(
          "[delete-test] replica " + idx + " refused the request after delete: " + e);
    }
  }

  private void awaitContainersGone() throws Exception {
    long deadline = System.nanoTime() + CLEANUP_TIMEOUT.toNanos();
    List<String> remaining = listServiceContainers();
    while (!remaining.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(1000);
      remaining = listServiceContainers();
    }
    assertTrue(remaining.isEmpty(), "Containers left after delete: " + remaining);
  }

  private void awaitStateDirsGone() throws Exception {
    long deadline = System.nanoTime() + CLEANUP_TIMEOUT.toNanos();
    List<Path> remaining = findServiceStateDirs();
    while (!remaining.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(1000);
      remaining = findServiceStateDirs();
    }
    assertTrue(remaining.isEmpty(), "State directories left after delete: " + remaining);
  }

  // Names of all containers (running or stopped) that belong to the service.
  private static List<String> listServiceContainers() throws Exception {
    Process process =
        new ProcessBuilder("docker", "ps", "-a", "--format", "{{.Names}}")
            .redirectErrorStream(true)
            .start();
    List<String> matching = new ArrayList<>();
    String output = new String(process.getInputStream().readAllBytes());
    int exit = process.waitFor();
    assertEquals(0, exit, "docker ps failed: " + output);
    for (String line : output.split("\\R")) {
      String name = line.trim();
      if (name.contains(CONTAINER_NAME_MARKER)) {
        matching.add(name);
      }
    }
    return matching;
  }

  // Directories named exactly like the service, up to STATE_SEARCH_MAX_DEPTH levels below
  // /tmp/xdn/state. The search never descends into a service directory, so it does not read
  // through a FUSE mount.
  private static List<Path> findServiceStateDirs() {
    List<Path> found = new ArrayList<>();
    collectServiceStateDirs(STATE_ROOT, 1, found);
    return found;
  }

  private static void collectServiceStateDirs(Path dir, int depth, List<Path> found) {
    if (depth > STATE_SEARCH_MAX_DEPTH || !Files.isDirectory(dir)) {
      return;
    }
    try (DirectoryStream<Path> children = Files.newDirectoryStream(dir)) {
      for (Path child : children) {
        if (!Files.isDirectory(child)) {
          continue;
        }
        if (SERVICE_NAME.equals(child.getFileName().toString())) {
          found.add(child);
        } else {
          collectServiceStateDirs(child, depth + 1, found);
        }
      }
    } catch (IOException e) {
      // unreadable directory, for example a stale FUSE mount, so skip it
    }
  }
}
