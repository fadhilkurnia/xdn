package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test for a <b>multi-component</b> cluster service: a self-clustering MySQL (Group
 * Replication) stateful member plus a WordPress entry sidecar that shares the member's network
 * namespace. This exercises the parts of cluster mode that {@link XdnClusterLaunchTest} (single
 * etcd image) does not — multi-component placement and sidecar netns sharing — against a database
 * that, unlike etcd/rqlite, has no one-flag clustering.
 *
 * <p>Asserts the two halves of the design (per the chosen scope: cluster forms + app reaches DB):
 *
 * <ul>
 *   <li>The MySQL group converges: all 3 members report {@code ONLINE} in {@code
 *       performance_schema.replication_group_members}.
 *   <li>On every replica the WordPress sidecar reaches its local MySQL and serves (a fresh install
 *       redirects to {@code wp-admin/install.php}); a DB-connection failure would instead surface
 *       as HTTP 500.
 * </ul>
 *
 * <p>Requires Docker + a swarm-initialized daemon (the test ensures this) and builds {@code
 * xdn-mysql-cluster:test} from {@code services/mysql-cluster/}; WordPress is pulled on first run.
 * This is heavier than the etcd test (3 MySQL members + 3 WordPress tiers), so the budgets below
 * are generous.
 */
public class XdnWordPressClusterTest {

  private static final String SERVICE = "wp-mysql-cluster";
  private static final String MYSQL_IMAGE = "xdn-mysql-cluster:test";
  private static final String WORDPRESS_IMAGE = "wordpress:6.5.4-apache";
  private static final String ROOT_PW = "supersecret";
  private static final int NUM_REPLICAS = 3;
  private static final int PEER_PORT = 33061; // MySQL Group Replication group-comm port

  // MySQL GR bootstrap is slow: each member runs a full datadir --initialize (tens of seconds),
  // all 3 concurrently, before the group even forms — so this budget is deliberately large. The
  // poll exits as soon as the group is up, so the ceiling only matters on failure.
  private static final Duration GROUP_BUDGET = Duration.ofSeconds(360);
  private static final Duration WORDPRESS_BUDGET = Duration.ofSeconds(180);

  @Test
  public void testWordPressMysqlClusterLaunch() throws Exception {
    assertTrue(XdnTestCluster.isDockerAvailable(), "docker is required for this test");
    assertTrue(
        XdnTestCluster.ensureDockerSwarm(),
        "cluster services need Swarm overlay networking — `docker swarm init` failed");
    assertTrue(
        XdnTestCluster.buildImage(MYSQL_IMAGE, "services/mysql-cluster/"),
        "could not build " + MYSQL_IMAGE + " from services/mysql-cluster/");
    // Pre-pull WordPress so it is not fetched (~600MB) on the cluster CREATE's critical path —
    // the create starts containers synchronously, and an on-demand pull there times the POST out.
    assertTrue(dockerPull(WORDPRESS_IMAGE), "could not pull " + WORDPRESS_IMAGE + " before launch");

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();
      cluster.launchClusterService(SERVICE, buildSpec());

      // (1) The MySQL group converges to NUM_REPLICAS ONLINE members. We read this by exec'ing
      // into any one member container — replication_group_members reflects the whole group.
      awaitOnlineMembers(NUM_REPLICAS, GROUP_BUDGET);

      // (2) Every replica's WordPress reaches its (local, shared-netns) MySQL and serves. A fresh
      // WordPress 302-redirects to the installer; a broken DB connection would be a 500 instead.
      for (int idx = 0; idx < NUM_REPLICAS; idx++) {
        HttpResponse<String> resp = awaitWordPressServing(cluster, idx, WORDPRESS_BUDGET);
        assertTrue(
            looksLikeWordPress(resp),
            "replica "
                + idx
                + " did not look like a WordPress install page: status="
                + resp.statusCode());
      }

      // XDN advertises the cluster coordinator through replica-info, same as the etcd test.
      HttpResponse<String> info =
          cluster.sendGetRequest(SERVICE, "/api/v2/services/" + SERVICE + "/replica/info");
      assertEquals(200, info.statusCode(), "replica/info request failed");
      JSONObject infoJson = new JSONObject(info.body());
      assertEquals(
          "StatefulClusterReplicaCoordinator",
          infoJson.getString("protocol"),
          "expected the cluster coordinator");
    }
  }

  // ── service spec ───────────────────────────────────────────────────────────

  /** The multi-component cluster spec: mysql (stateful member) + wordpress (entry sidecar). */
  private JSONObject buildSpec() throws JSONException {
    JSONObject mysql =
        new JSONObject()
            .put("image", MYSQL_IMAGE)
            .put("stateful", true) // the cluster member: joins the overlay, runs Group Replication
            .put("expose", 3306) // client port, reached by the sidecar at 127.0.0.1 (shared netns)
            .put("environments", env("MYSQL_ROOT_PASSWORD", ROOT_PW));

    JSONObject wordpress =
        new JSONObject()
            .put("image", WORDPRESS_IMAGE)
            .put("port", 80) // entry port: published to the host, routed by the XDN frontend
            .put("entry", true)
            .put(
                "environments",
                new JSONArray()
                    .put(kv("WORDPRESS_DB_HOST", "127.0.0.1")) // MySQL shares this netns
                    .put(kv("WORDPRESS_DB_USER", "root"))
                    .put(kv("WORDPRESS_DB_PASSWORD", ROOT_PW))
                    .put(kv("WORDPRESS_DB_NAME", "wordpress"))
                    .put(kv("WORDPRESS_CONFIG_EXTRA", "define('DISABLE_WP_CRON', true);")));

    // mysql must be component index 0 (the stateful member); wordpress is the entry sidecar.
    JSONArray components =
        new JSONArray()
            .put(new JSONObject().put("mysql", mysql))
            .put(new JSONObject().put("wordpress", wordpress));

    return new JSONObject()
        .put("mode", "cluster")
        .put("peer_port", PEER_PORT)
        .put("num_replicas", NUM_REPLICAS)
        .put("state", "mysql:/var/lib/mysql/")
        .put("components", components);
  }

  private static JSONArray env(String key, String value) throws JSONException {
    return new JSONArray().put(kv(key, value));
  }

  private static JSONObject kv(String key, String value) throws JSONException {
    return new JSONObject().put(key, value);
  }

  // ── MySQL group-membership probe (via docker exec) ───────────────────────────

  private void awaitOnlineMembers(int expected, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    int last = -1;
    while (System.nanoTime() < deadline) {
      Integer online = onlineMemberCount();
      if (online != null) {
        last = online;
        if (online >= expected) {
          return;
        }
      }
      Thread.sleep(3000);
    }
    throw new AssertionError(
        "MySQL Group Replication never reached "
            + expected
            + " ONLINE members (last seen: "
            + last
            + ")");
  }

  /** Returns the count of ONLINE GR members, or null if no member container is queryable yet. */
  private Integer onlineMemberCount() throws Exception {
    String container = firstMysqlContainer();
    if (container == null) {
      return null;
    }
    ProcResult r =
        runProcess(
            List.of(
                "docker",
                "exec",
                container,
                "mysql",
                "-uroot",
                "-p" + ROOT_PW,
                "-N",
                "-B",
                "-e",
                "SELECT COUNT(*) FROM performance_schema.replication_group_members"
                    + " WHERE MEMBER_STATE = 'ONLINE'"));
    if (r.exitCode != 0) {
      return null; // mysqld may not be accepting connections yet
    }
    try {
      return Integer.parseInt(r.stdout.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Finds one running MySQL cluster-member container by image, regardless of XDN's naming. */
  private String firstMysqlContainer() throws Exception {
    ProcResult r =
        runProcess(
            List.of(
                "docker", "ps", "--filter", "ancestor=" + MYSQL_IMAGE, "--format", "{{.Names}}"));
    if (r.exitCode != 0) {
      return null;
    }
    for (String line : r.stdout.split("\\R")) {
      if (!line.isBlank()) {
        return line.trim();
      }
    }
    return null;
  }

  // ── WordPress reachability probe (via the XDN frontend) ──────────────────────

  private HttpResponse<String> awaitWordPressServing(
      XdnTestCluster cluster, int replicaIdx, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    HttpResponse<String> last = null;
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<String> r =
            cluster.sendGetRequest(SERVICE, replicaIdx, "/", Duration.ofSeconds(5));
        last = r;
        // 5xx means WordPress is up but cannot reach/select its database — keep waiting.
        if (r.statusCode() < 500) {
          return r;
        }
      } catch (Exception ignored) {
        // frontend not forwarding yet
      }
      Thread.sleep(2000);
    }
    throw new AssertionError(
        "WordPress on replica "
            + replicaIdx
            + " never served a non-5xx response"
            + (last != null ? " (last status " + last.statusCode() + ")" : ""));
  }

  private boolean looksLikeWordPress(HttpResponse<String> resp) {
    int code = resp.statusCode();
    String location = resp.headers().firstValue("Location").orElse("");
    String body = resp.body() == null ? "" : resp.body();
    if (code == 301 || code == 302) {
      return location.contains("install.php") || location.contains("wp-admin");
    }
    return code == 200 && (body.contains("WordPress") || body.contains("wp-admin"));
  }

  // ── tiny process helper (captures stdout; avoids Shell's whitespace-splitting) ──

  private static boolean dockerPull(String image) throws Exception {
    return runProcess(List.of("docker", "pull", image)).exitCode() == 0;
  }

  private record ProcResult(String stdout, String stderr, int exitCode) {}

  private static ProcResult runProcess(List<String> command) throws Exception {
    Process p = new ProcessBuilder(new ArrayList<>(command)).start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    String err = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    int exit = p.waitFor();
    return new ProcResult(out, err, exit);
  }
}
