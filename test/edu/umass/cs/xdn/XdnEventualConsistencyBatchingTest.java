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
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class XdnEventualConsistencyBatchingTest {

  private static final HttpClient HTTP_CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  @Test
  public void testEventualConsistencyWithBatchingEnabledPostRequest() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    Config.register(new String[] {"HTTP_AR_FRONTEND_BATCH_ENABLED=true"});

    String serviceName = "todo_eventual_batch_test";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      // Launch service with EVENTUAL consistency and proper request matchers
      cluster.launchService(
          serviceName,
          "fadhilkurnia/xdn-todo",
          "/app/data/",
          "EVENTUAL",
          true,
          new JSONArray()
              .put(
                  new JSONObject()
                      .put("path_prefix", "/")
                      .put("methods", "GET,OPTIONS,HEAD")
                      .put("behavior", "read_only"))
              .put(
                  new JSONObject()
                      .put("path_prefix", "/")
                      .put("methods", "PUT,POST,DELETE")
                      .put("behavior", "write_only"))
              .put(
                  new JSONObject()
                      .put("path_prefix", "/")
                      .put("methods", "PUT,POST,DELETE")
                      .put("behavior", "monotonic")));

      Thread.sleep(2000);

      System.out.println("Checking service connectivity");
      HttpResponse<String> readyResponse =
          cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      assertTrue(
          readyResponse.statusCode() >= 200 && readyResponse.statusCode() < 500,
          "Service did not return a valid HTTP status: " + readyResponse.statusCode());

      // Send multiple POST requests (write-only + monotonic operations)
      int numRequests = 3;
      for (int i = 1; i <= numRequests; i++) {
        String jsonBody = String.format("{\"item\":\"Task %d\"}", i);
        HttpResponse<String> postResponse =
            sendPostRequest(cluster, serviceName, "/api/todo/tasks", jsonBody);

        assertTrue(
            postResponse.statusCode() == 200 || postResponse.statusCode() == 201,
            String.format(
                "POST request %d failed with status %d: %s",
                i, postResponse.statusCode(), postResponse.body()));

        System.out.printf(
            "POST request %d succeeded with status %d%n", i, postResponse.statusCode());
      }

      // Verify that the data can be readed (eventual consistency)
      Thread.sleep(1000);
      HttpResponse<String> getResponse = cluster.sendGetRequest(serviceName, "/api/todo/tasks");
      assertEquals(200, getResponse.statusCode(), "GET request failed: " + getResponse.body());
      assertFalse(getResponse.body().isEmpty(), "GET response body should not be empty");

      System.out.println("All requests completed successfully with batching enabled");
    }
  }

  private HttpResponse<String> sendPostRequest(
      XdnTestCluster cluster, String serviceName, String endpoint, String jsonBody)
      throws IOException, InterruptedException {
    int httpPort = cluster.getActiveHttpPort("AR0");
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + httpPort + endpoint))
            .timeout(XdnTestCluster.REQUEST_TIMEOUT)
            .header("XDN", serviceName)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build();
    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
