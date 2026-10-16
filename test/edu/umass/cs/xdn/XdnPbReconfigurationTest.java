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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Verifies the reconfiguration of a primary-backup (Blue-Green) service, which is a change of the
 * set of replicas that host it. The old placement stops and captures its final state. The new
 * placement is seeded from that state, elects a primary and serves again. Then the old epoch is
 * dropped. Writes made before a reconfiguration must be readable after it.
 *
 * <p>Two scenarios. The away-and-back scenario runs on three nodes and shrinks the service from
 * three replicas to two and back, so two nodes stay through both reconfigurations. The disjoint
 * scenario runs on six nodes and moves the service to three other nodes and back, so every node of
 * the new placement is fresh.
 *
 * <p>Each scenario runs for three services. The plain methods use the single-container bookcatalog
 * with SQLite. The Mysql and Postgres methods use bookcatalog-nd with a database container, whose
 * data directory is the state that moves.
 *
 * <p>These tests only run against XdnApp (the Blue-Green primary-backup manager). Each is skipped
 * unless the system property XDN_TEST_GIGAPAXOS_CONFIG names its own properties file.
 */
public class XdnPbReconfigurationTest {

  private static final String CONFIG_3_NODES = "gigapaxos.xdn.new-local.properties";
  private static final String CONFIG_6_NODES = "gigapaxos.xdn.new-local-6.properties";

  /**
   * The service under test. SQLITE is the single-container bookcatalog. MYSQL and POSTGRES are the
   * two-container bookcatalog-nd with a database. The names are not substrings of each other,
   * because listContainers matches by substring.
   */
  private enum Flavor {
    SQLITE("pbreconfigaway", "pbreconfigdisjoint"),
    MYSQL("pbrcmysqlaway", "pbrcmysqldisjoint"),
    POSTGRES("pbrcpgaway", "pbrcpgdisjoint");

    final String awayService;
    final String disjointService;

    Flavor(String awayService, String disjointService) {
      this.awayService = awayService;
      this.disjointService = disjointService;
    }
  }

  private static final List<String> THREE_NODES = List.of("AR0", "AR1", "AR2");
  private static final List<String> SIX_NODES = List.of("AR0", "AR1", "AR2", "AR3", "AR4", "AR5");

  // Final state of an epoch is kept at /tmp/xdn/final/<node>/<service>/<epoch>/ until the epoch is
  // dropped (DockerSandboxManager).
  private static final Path FINAL_STATE_ROOT = Paths.get("/tmp/xdn/final");

  private static final Duration SETTLE_TIMEOUT = Duration.ofSeconds(180);
  private static final Duration DROP_TIMEOUT = Duration.ofSeconds(120);
  private static final Duration NOT_SERVING_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration CONTAINERS_GONE_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration BOOK_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(60);
  // The placement request may be answered only after the reconfiguration is done.
  private static final Duration PLACEMENT_REQUEST_TIMEOUT = Duration.ofSeconds(120);

  private final HttpClient client = HttpClient.newHttpClient();

  /** The reconfigurator's view of a service, from GET /api/v2/services/{name}/placement. */
  private record Placement(int epoch, Set<String> nodes) {
    Placement {
      nodes = Collections.unmodifiableSet(new TreeSet<>(nodes));
    }
  }

  @Test
  public void testReconfigureAwayAndBack() throws Exception {
    runAwayAndBack(Flavor.SQLITE);
  }

  @Test
  public void testReconfigureAwayAndBackMysql() throws Exception {
    runAwayAndBack(Flavor.MYSQL);
  }

  @Test
  public void testReconfigureAwayAndBackPostgres() throws Exception {
    runAwayAndBack(Flavor.POSTGRES);
  }

  @Test
  public void testReconfigureToDisjointNodes() throws Exception {
    runToDisjointNodes(Flavor.SQLITE);
  }

  @Test
  public void testReconfigureToDisjointNodesMysql() throws Exception {
    runToDisjointNodes(Flavor.MYSQL);
  }

  @Test
  public void testReconfigureToDisjointNodesPostgres() throws Exception {
    runToDisjointNodes(Flavor.POSTGRES);
  }

  private void runAwayAndBack(Flavor flavor) throws Exception {
    assumeTrue(
        CONFIG_3_NODES.equals(System.getProperty("XDN_TEST_GIGAPAXOS_CONFIG")),
        "This test only runs on XdnApp (" + CONFIG_3_NODES + ")");
    assertTrue(XdnTestCluster.isDockerAvailable(), "Docker is required for this test");
    final String service = flavor.awayService;

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      launch(cluster, service, flavor);
      Placement initial = awaitSettled(cluster, service, null, 0, "initial placement");
      log("initial placement: " + initial);
      assertEquals(Set.copyOf(THREE_NODES), initial.nodes(), "Expected all three nodes");

      String titleA = "BookA-" + System.nanoTime();
      writeBook(cluster, service, 0, titleA);
      awaitBookOnNodes(cluster, service, initial.nodes(), titleA);

      // Precondition, so the checks on the leaving node cannot pass vacuously.
      List<String> containersBefore = listContainers(service, "AR2");
      log("containers on AR2 before: " + containersBefore);
      assertFalse(
          listContainers(service, null).isEmpty(),
          "Expected containers for the service before the reconfiguration");

      // three replicas to two. AR0 and AR1 stay, AR2 leaves.
      Placement shrunk = reconfigure(cluster, service, List.of("AR0", "AR1"), initial);
      log("after shrinking: " + shrunk);
      awaitBookOnNodes(cluster, service, shrunk.nodes(), titleA);

      // the staying nodes must take new writes after the old epoch was dropped
      String titleB = "BookB-" + System.nanoTime();
      writeBook(cluster, service, 0, titleB);
      awaitBookOnNodes(cluster, service, shrunk.nodes(), titleB);

      // two replicas to three. AR2 comes back from the final state of the old epoch.
      Placement regrown = reconfigure(cluster, service, THREE_NODES, shrunk);
      log("after growing: " + regrown);
      awaitBookOnNodes(cluster, service, regrown.nodes(), titleA);
      awaitBookOnNodes(cluster, service, regrown.nodes(), titleB);

      String titleC = "BookC-" + System.nanoTime();
      writeBook(cluster, service, 0, titleC);
      awaitBookOnNodes(cluster, service, regrown.nodes(), titleC);
    }
  }

  private void runToDisjointNodes(Flavor flavor) throws Exception {
    assumeTrue(
        CONFIG_6_NODES.equals(System.getProperty("XDN_TEST_GIGAPAXOS_CONFIG")),
        "This test only runs on XdnApp with six nodes (" + CONFIG_6_NODES + ")");
    assertTrue(XdnTestCluster.isDockerAvailable(), "Docker is required for this test");
    final String service = flavor.disjointService;

    try (XdnTestCluster cluster = new XdnTestCluster(SIX_NODES)) {
      cluster.start();

      // The default placement decides where the service starts, so read it back.
      launch(cluster, service, flavor);
      Placement initial = awaitSettled(cluster, service, null, 0, "initial placement");
      log("initial placement: " + initial);

      String titleA = "BookA-" + System.nanoTime();
      String writer = initial.nodes().iterator().next();
      writeBook(cluster, service, indexOf(writer), titleA);
      awaitBookOnNodes(cluster, service, initial.nodes(), titleA);

      assertFalse(
          listContainers(service, null).isEmpty(),
          "Expected containers for the service before the reconfiguration");

      // first reconfiguration, to AR3 to AR5
      List<String> far = List.of("AR3", "AR4", "AR5");
      Set<String> overlap = new TreeSet<>(initial.nodes());
      overlap.retainAll(far);
      log("nodes shared by the initial placement and " + far + ": " + overlap);
      Placement atFar = reconfigure(cluster, service, far, initial);
      log("after the first reconfiguration: " + atFar);
      awaitBookOnNodes(cluster, service, atFar.nodes(), titleA);

      String titleB = "BookB-" + System.nanoTime();
      writeBook(cluster, service, indexOf("AR3"), titleB);
      awaitBookOnNodes(cluster, service, atFar.nodes(), titleB);

      // second reconfiguration, back to AR0 to AR2, which shares no node with AR3 to AR5
      Placement atHome = reconfigure(cluster, service, THREE_NODES, atFar);
      log("after the second reconfiguration: " + atHome);
      assertTrue(
          Collections.disjoint(atFar.nodes(), atHome.nodes()),
          "The second reconfiguration should be fully disjoint");
      awaitBookOnNodes(cluster, service, atHome.nodes(), titleA);
      awaitBookOnNodes(cluster, service, atHome.nodes(), titleB);

      String titleC = "BookC-" + System.nanoTime();
      writeBook(cluster, service, 0, titleC);
      awaitBookOnNodes(cluster, service, atHome.nodes(), titleC);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Reconfiguration
  // ---------------------------------------------------------------------------------------------

  /**
   * Requests the new placement and waits until it is fully in effect. That means the reconfigurator
   * reports the target nodes at a higher epoch, every target node serves with one primary, the
   * leaving nodes serve nothing and have no containers, and the old epoch was dropped everywhere.
   */
  private Placement reconfigure(
      XdnTestCluster cluster, String service, List<String> target, Placement before)
      throws Exception {
    Set<String> targetSet = new TreeSet<>(target);
    Set<String> leaving = new TreeSet<>(before.nodes());
    leaving.removeAll(targetSet);
    Set<String> joining = new TreeSet<>(targetSet);
    joining.removeAll(before.nodes());
    log(
        "reconfiguring "
            + before.nodes()
            + " to "
            + targetSet
            + ", leaving="
            + leaving
            + " joining="
            + joining);

    postPlacement(cluster, service, target);
    Placement after =
        awaitSettled(cluster, service, targetSet, before.epoch() + 1, "placement " + targetSet);

    for (String node : leaving) {
      awaitNotServing(cluster, service, node);
      awaitNoContainers(service, node);
    }
    awaitOldEpochDropped(service, before.epoch());
    return after;
  }

  private void postPlacement(XdnTestCluster cluster, String service, List<String> nodes)
      throws Exception {
    String body = new JSONObject().put("NODES", new JSONArray(nodes)).toString();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(reconfiguratorUrl(cluster, service) + "/placement"))
            .timeout(PLACEMENT_REQUEST_TIMEOUT)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    log("placement request " + body + " answered HTTP " + response.statusCode());
    assertEquals(200, response.statusCode(), "Placement request failed: " + response.body());
    assertFalse(
        response.body().contains("\"FAILED\":true"),
        "Placement request was rejected: " + response.body());
  }

  /**
   * Polls until the placement is in effect. With a null expectedNodes any node set is accepted,
   * which is how the initial placement is read.
   */
  private Placement awaitSettled(
      XdnTestCluster cluster, String service, Set<String> expectedNodes, int minEpoch, String label)
      throws Exception {
    long deadline = System.nanoTime() + SETTLE_TIMEOUT.toNanos();
    String last = "nothing read yet";
    while (System.nanoTime() < deadline) {
      Placement placement = readPlacement(cluster, service);
      if (placement == null) {
        last = "the placement could not be read";
      } else if (placement.epoch() < minEpoch) {
        last = "epoch " + placement.epoch() + " is below " + minEpoch;
      } else if (expectedNodes != null && !placement.nodes().equals(expectedNodes)) {
        last = "nodes " + placement.nodes() + " are not " + expectedNodes;
      } else {
        String problem = checkNodesServing(cluster, service, placement);
        if (problem == null) {
          return placement;
        }
        last = "epoch " + placement.epoch() + " nodes " + placement.nodes() + ": " + problem;
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("Timed out waiting for " + label + ". Last state: " + last);
  }

  // Returns null if every node of the placement serves at the placement's epoch with exactly one
  // primary, otherwise a description of the first problem.
  private String checkNodesServing(XdnTestCluster cluster, String service, Placement placement)
      throws InterruptedException {
    int primaries = 0;
    int backups = 0;
    for (String node : placement.nodes()) {
      int idx = indexOf(node);
      try {
        HttpResponse<String> info =
            cluster.sendGetRequest(
                service,
                idx,
                "/api/v2/services/" + service + "/replica/info",
                Duration.ofSeconds(3));
        if (info.statusCode() != 200) {
          return node + " replica/info answered HTTP " + info.statusCode();
        }
        JSONObject json = new JSONObject(info.body());
        int epoch = json.optInt("epoch", -1);
        if (epoch != placement.epoch()) {
          return node + " reports epoch " + epoch;
        }
        String role = json.optString("role", "unknown");
        if ("primary".equals(role)) {
          primaries++;
        } else if ("backup".equals(role)) {
          backups++;
        } else {
          return node + " has role " + role;
        }
        HttpResponse<String> books =
            cluster.sendGetRequest(service, idx, "/api/books", Duration.ofSeconds(3));
        if (books.statusCode() != 200) {
          return node + " /api/books answered HTTP " + books.statusCode();
        }
      } catch (IOException | JSONException | RuntimeException e) {
        return node + " failed: " + e;
      }
    }
    if (primaries != 1 || backups != placement.nodes().size() - 1) {
      return "roles are " + primaries + " primary and " + backups + " backup";
    }
    return null;
  }

  // Reads GET /api/v2/services/{name}/placement. Returns null if it cannot be read yet.
  private Placement readPlacement(XdnTestCluster cluster, String service)
      throws InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(reconfiguratorUrl(cluster, service) + "/placement"))
            .timeout(XdnTestCluster.REQUEST_TIMEOUT)
            .GET()
            .build();
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        return null;
      }
      JSONObject root = new JSONObject(response.body());
      JSONObject data = root.optJSONObject("DATA");
      JSONArray nodes = data == null ? null : data.optJSONArray("NODES");
      if (nodes == null) {
        return null;
      }
      Set<String> ids = new TreeSet<>();
      for (int i = 0; i < nodes.length(); i++) {
        ids.add(nodes.getJSONObject(i).getString("ID"));
      }
      return new Placement(root.getInt("EPOCH"), ids);
    } catch (IOException | JSONException | RuntimeException e) {
      return null;
    }
  }

  private static String reconfiguratorUrl(XdnTestCluster cluster, String service) {
    return "http://127.0.0.1:%d/api/v2/services/%s"
        .formatted(cluster.getReconfiguratorHttpPort(), service);
  }

  // ---------------------------------------------------------------------------------------------
  // Service traffic
  // ---------------------------------------------------------------------------------------------

  private void launch(XdnTestCluster cluster, String service, Flavor flavor) throws Exception {
    if (flavor == Flavor.SQLITE) {
      launchSqlite(cluster, service);
    } else {
      // launchClusterService only posts the prebuilt spec. It needs no cluster fields, so it also
      // serves a plain multi-component service. Its error text says "Cluster" for that reason.
      cluster.launchClusterService(service, databaseSpec(flavor));
    }
  }

  // The same service as bookcatalog-nd.my.yaml and bookcatalog-nd.pg.yaml, as the JSON that
  // ServiceProperty parses. Both use the :5 app image. The name is set by launchClusterService.
  private static JSONObject databaseSpec(Flavor flavor) throws JSONException {
    boolean mysql = flavor == Flavor.MYSQL;
    String dbComponent = mysql ? "mysql" : "postgres";

    JSONObject database =
        new JSONObject()
            .put("image", mysql ? "mysql:8.0.41-debian" : "postgres:17.4-bookworm")
            .put("stateful", true)
            .put(
                "environments",
                mysql
                    ? environments("MYSQL_DATABASE", "books", "MYSQL_ROOT_PASSWORD", "root")
                    : environments("POSTGRES_DB", "books", "POSTGRES_PASSWORD", "root"));

    JSONObject app =
        new JSONObject()
            .put("image", "michael2718/bookcatalog-nd:5")
            .put("stateful", false)
            .put("entry", true)
            .put("port", 80)
            .put("environments", environments("DB_TYPE", dbComponent, "DB_HOST", dbComponent))
            .put("healthcheck", new JSONObject().put("path", "/api/books"));

    JSONArray components =
        new JSONArray()
            .put(new JSONObject().put(dbComponent, database))
            .put(new JSONObject().put("app", app));

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
    requests.put(
        new JSONObject()
            .put("path_prefix", "/")
            .put("methods", "PUT,POST,DELETE")
            .put("behavior", "monotonic"));

    return new JSONObject()
        .put("components", components)
        .put("state", mysql ? "mysql:/var/lib/mysql/" : "postgres:/var/lib/postgresql/data/")
        .put("deterministic", false)
        .put("consistency", "sequential")
        .put("requests", requests);
  }

  // Name and value pairs as the one-key objects that ServiceProperty expects.
  private static JSONArray environments(String... namesAndValues) throws JSONException {
    JSONArray result = new JSONArray();
    for (int i = 0; i < namesAndValues.length; i += 2) {
      result.put(new JSONObject().put(namesAndValues[i], namesAndValues[i + 1]));
    }
    return result;
  }

  private void launchSqlite(XdnTestCluster cluster, String service) throws Exception {
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
        service,
        "fadhilkurnia/xdn-bookcatalog",
        "/app/data/",
        "SEQUENTIAL",
        false,
        requests,
        "/api/books",
        null);
  }

  // Retries, because a node that just won the election may answer 503 for a moment.
  private void writeBook(XdnTestCluster cluster, String service, int idx, String title)
      throws Exception {
    JSONObject book = new JSONObject().put("title", title).put("author", "Reconfig Author");
    long deadline = System.nanoTime() + WRITE_TIMEOUT.toNanos();
    String last = "";
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<String> response =
            cluster.sendPostRequest(
                service, idx, "/api/books", book.toString(), Duration.ofSeconds(20));
        if (response.statusCode() / 100 == 2) {
          return;
        }
        last = "HTTP " + response.statusCode() + " " + response.body();
      } catch (IOException e) {
        last = e.toString();
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("Write of '" + title + "' never succeeded. Last: " + last);
  }

  private void awaitBookOnNodes(
      XdnTestCluster cluster, String service, Set<String> nodes, String title) throws Exception {
    for (String node : nodes) {
      awaitBook(cluster, service, node, title);
    }
  }

  private void awaitBook(XdnTestCluster cluster, String service, String node, String title)
      throws Exception {
    long deadline = System.nanoTime() + BOOK_TIMEOUT.toNanos();
    String last = "";
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<String> response =
            cluster.sendGetRequest(service, indexOf(node), "/api/books", Duration.ofSeconds(3));
        if (response.statusCode() == 200 && response.body().contains(title)) {
          return;
        }
        last = "HTTP " + response.statusCode() + " " + response.body();
      } catch (IOException e) {
        last = e.toString();
      }
      Thread.sleep(1000);
    }
    throw new AssertionError(
        "Book '" + title + "' never became visible on " + node + ". Last: " + last);
  }

  // A node that left the placement must not answer 200. A refused connection is also fine.
  private void awaitNotServing(XdnTestCluster cluster, String service, String node)
      throws Exception {
    long deadline = System.nanoTime() + NOT_SERVING_TIMEOUT.toNanos();
    int lastStatus = -1;
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<String> response =
            cluster.sendGetRequest(service, indexOf(node), "/api/books", Duration.ofSeconds(3));
        lastStatus = response.statusCode();
        if (lastStatus != 200) {
          log(node + " answered HTTP " + lastStatus + " after leaving");
          return;
        }
      } catch (IOException e) {
        log(node + " refused the request after leaving: " + e);
        return;
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("Node " + node + " still serves " + service + " after leaving");
  }

  // ---------------------------------------------------------------------------------------------
  // Cleanup checks
  // ---------------------------------------------------------------------------------------------

  private void awaitNoContainers(String service, String node) throws Exception {
    long deadline = System.nanoTime() + CONTAINERS_GONE_TIMEOUT.toNanos();
    List<String> remaining = listContainers(service, node);
    while (!remaining.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(1000);
      remaining = listContainers(service, node);
    }
    assertTrue(remaining.isEmpty(), "Containers left on " + node + " after leaving: " + remaining);
  }

  // The drop of the old epoch removes its final state directory on every old node, staying or
  // leaving, so this is the signal that the drop finished.
  private void awaitOldEpochDropped(String service, int oldEpoch) throws Exception {
    long deadline = System.nanoTime() + DROP_TIMEOUT.toNanos();
    List<Path> remaining = findFinalStateDirs(service, oldEpoch);
    while (!remaining.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(1000);
      remaining = findFinalStateDirs(service, oldEpoch);
    }
    assertTrue(remaining.isEmpty(), "Epoch " + oldEpoch + " was not dropped. Left: " + remaining);
  }

  private static List<Path> findFinalStateDirs(String service, int epoch) {
    List<Path> found = new ArrayList<>();
    if (!Files.isDirectory(FINAL_STATE_ROOT)) {
      return found;
    }
    try (DirectoryStream<Path> nodeDirs = Files.newDirectoryStream(FINAL_STATE_ROOT)) {
      for (Path nodeDir : nodeDirs) {
        Path epochDir = nodeDir.resolve(service).resolve(Integer.toString(epoch));
        if (Files.exists(epochDir)) {
          found.add(epochDir);
        }
      }
    } catch (IOException e) {
      // unreadable directory, so report what was found
    }
    return found;
  }

  // Names of all containers (running or stopped) of the service. With a node, only the containers
  // of that node. Containers are named c<i>.e<epoch>.<service>.<node>.xdn.io.
  private static List<String> listContainers(String service, String node) throws Exception {
    Process process =
        new ProcessBuilder("docker", "ps", "-a", "--format", "{{.Names}}")
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes());
    int exit = process.waitFor();
    assertEquals(0, exit, "docker ps failed: " + output);
    List<String> matching = new ArrayList<>();
    for (String line : output.split("\\R")) {
      String name = line.trim();
      if (name.contains(service)
          && (node == null || name.toLowerCase().contains("." + node.toLowerCase() + "."))) {
        matching.add(name);
      }
    }
    return matching;
  }

  // ---------------------------------------------------------------------------------------------
  // Small helpers
  // ---------------------------------------------------------------------------------------------

  // The index of the node in the cluster, which is its number for AR0, AR1 and so on.
  private static int indexOf(String node) {
    return Integer.parseInt(node.substring("AR".length()));
  }

  private static void log(String message) {
    System.out.println("[reconfig-test] " + message);
  }
}
