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
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Verifies the primary-backup SEQUENTIAL cookie. A client that writes through any replica (a backup
 * forwards the write to the primary) must be able to read its own write back from that same replica
 * when it presents the cookie returned by the write.
 *
 * <p>This test only runs against XdnApp (the Blue-Green primary-backup manager). It is skipped
 * unless the system property XDN_TEST_GIGAPAXOS_CONFIG is gigapaxos.xdn.new-local.properties.
 */
public class XdnPbSequentialCookieTest {

  private static final String XDN_APP_CONFIG = "gigapaxos.xdn.new-local.properties";
  private static final String SERVICE_NAME = "pbcookietest";
  private static final String COOKIE_NAME = "XDN-PB-SC-" + SERVICE_NAME;
  private static final Pattern COOKIE_VALUE = Pattern.compile("\\d+\\.\\d+");
  private static final int NUM_REPLICAS = 3;
  private static final List<String> ACTIVE_IDS = List.of("AR0", "AR1", "AR2");

  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  public void testWriteThenReadOwnWriteWithCookie() throws Exception {
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

      // write through every replica, then read back from the same replica
      for (int i = 0; i < NUM_REPLICAS; i++) {
        String activeId = ACTIVE_IDS.get(i);
        int port = cluster.getActiveHttpPort(activeId);
        String role = getRole(cluster, i);
        String title = "CookieBook-" + activeId + "-" + System.nanoTime();

        // 1. write
        JSONObject book = new JSONObject().put("title", title).put("author", "Cookie Author");
        HttpRequest post =
            HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/api/books"))
                .timeout(Duration.ofSeconds(20))
                .header("XDN", SERVICE_NAME)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(book.toString()))
                .build();
        HttpResponse<String> postResp = client.send(post, HttpResponse.BodyHandlers.ofString());
        assertEquals(
            2,
            postResp.statusCode() / 100,
            "Write via " + activeId + " (" + role + ") failed: " + postResp.statusCode());

        // 2. the write response must carry the cookie
        String cookieValue = extractCookieValue(postResp);
        assertNotNull(
            cookieValue,
            "No " + COOKIE_NAME + " Set-Cookie on write via " + activeId + " (" + role + ")");
        assertTrue(
            COOKIE_VALUE.matcher(cookieValue).matches(),
            "Unexpected cookie value format: " + cookieValue);
        System.out.println("[cookie-test] " + activeId + " (" + role + ") cookie=" + cookieValue);

        // 3. read own write on the same replica with the cookie, with no retry
        HttpResponse<String> getResp = get(port, "/api/books", COOKIE_NAME + "=" + cookieValue);
        assertEquals(200, getResp.statusCode(), "Read via " + activeId + " failed");
        assertTrue(
            getResp.body().contains(title),
            "Read-your-write violated on "
                + activeId
                + " ("
                + role
                + "). Missing '"
                + title
                + "' in body: "
                + getResp.body());

        // 4. optional control on backups: the same read without a cookie is normally stale
        if (!"primary".equals(role)) {
          HttpResponse<String> noCookie = get(port, "/api/books", null);
          if (noCookie.statusCode() == 200 && noCookie.body().contains(title)) {
            System.out.println(
                "[cookie-test] control inconclusive on "
                    + activeId
                    + ": write already visible without cookie (backup refresh landed)");
          } else {
            System.out.println(
                "[cookie-test] control ok on "
                    + activeId
                    + ": stale without cookie, fresh with it");
          }
        }
      }
    }
  }

  private HttpResponse<String> get(int port, String path, String cookieHeader) throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + port + path))
            .timeout(Duration.ofSeconds(20))
            .header("XDN", SERVICE_NAME)
            .GET();
    if (cookieHeader != null) {
      b.header("Cookie", cookieHeader);
    }
    return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String extractCookieValue(HttpResponse<String> resp) {
    for (String header : resp.headers().allValues("Set-Cookie")) {
      String prefix = COOKIE_NAME + "=";
      if (header.startsWith(prefix)) {
        String rest = header.substring(prefix.length());
        int semi = rest.indexOf(';');
        return (semi >= 0 ? rest.substring(0, semi) : rest).trim();
      }
    }
    return null;
  }

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
        System.out.println("[cookie-test] roles: " + last);
        return;
      }
      Thread.sleep(1000);
    }
    fail("Roles never settled to 1 primary and 2 backups. Last seen: " + last);
  }
}
