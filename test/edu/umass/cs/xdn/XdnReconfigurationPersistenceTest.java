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

public class XdnReconfigurationPersistenceTest {

  private static final HttpClient HTTP_CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  @Test
  public void testStatePersistedAfterReconfiguration() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    Config.register(
        new String[] {"DISABLE_CHECKPOINTING=false", "HTTP_AR_FRONTEND_BATCH_ENABLED=true"});

    String serviceName = "todo_linearizable";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceName,
          "fadhilkurnia/xdn-todo",
          "/app/data/",
          "LINEARIZABLE",
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

      System.out.println("Waiting for service to be ready");
      HttpResponse<String> readyResponse =
          cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);
      assertTrue(
          readyResponse.statusCode() >= 200 && readyResponse.statusCode() < 500,
          "Service did not return a valid HTTP status: " + readyResponse.statusCode());

      // POST 3 tasks
      String[] tasks = {"task1", "task2", "task3"};
      for (String task : tasks) {
        String jsonBody = String.format("{\"item\":\"%s\"}", task);
        HttpResponse<String> postResponse =
            sendPostRequest(cluster, serviceName, "/api/todo/tasks", jsonBody);
        assertTrue(
            postResponse.statusCode() == 200 || postResponse.statusCode() == 201,
            String.format(
                "POST for %s failed with status %d: %s",
                task, postResponse.statusCode(), postResponse.body()));
        System.out.printf("Created %s (status %d)%n", task, postResponse.statusCode());
      }

      // Verify tasks exist before reconfiguration
      Thread.sleep(1000);
      HttpResponse<String> preReconfigGet = cluster.sendGetRequest(serviceName, "/api/todo/tasks");
      assertEquals(
          200,
          preReconfigGet.statusCode(),
          "Pre-reconfiguration GET failed: " + preReconfigGet.body());
      for (String task : tasks) {
        assertTrue(
            preReconfigGet.body().contains(task),
            "Pre-reconfiguration: expected " + task + " in response: " + preReconfigGet.body());
      }
      System.out.println("Pre-reconfiguration verification passed");

      // Trigger reconfiguration to the same 3 nodes
      int rcHttpPort = cluster.getReconfiguratorHttpPort();
      String placementUrl =
          String.format(
              "http://127.0.0.1:%d/api/v2/services/%s/placement", rcHttpPort, serviceName);
      JSONObject placementBody = new JSONObject();
      placementBody.put("NODES", new JSONArray().put("AR0").put("AR1").put("AR2"));

      HttpRequest placementRequest =
          HttpRequest.newBuilder()
              .uri(URI.create(placementUrl))
              .timeout(XdnTestCluster.REQUEST_TIMEOUT)
              .header("Content-Type", "application/json")
              .PUT(HttpRequest.BodyPublishers.ofString(placementBody.toString()))
              .build();
      HttpResponse<String> placementResponse =
          HTTP_CLIENT.send(placementRequest, HttpResponse.BodyHandlers.ofString());
      System.out.printf(
          "Reconfiguration response: status=%d body=%s%n",
          placementResponse.statusCode(), placementResponse.body());
      assertEquals(
          200,
          placementResponse.statusCode(),
          "Reconfiguration failed: " + placementResponse.body());

      Thread.sleep(10000);

      // Verify tasks still exist after reconfiguration
      HttpResponse<String> postReconfigGet = cluster.sendGetRequest(serviceName, "/api/todo/tasks");
      assertEquals(
          200,
          postReconfigGet.statusCode(),
          "Post-reconfiguration GET failed: " + postReconfigGet.body());
      for (String task : tasks) {
        assertTrue(
            postReconfigGet.body().contains(task),
            "Post-reconfiguration: expected " + task + " in response: " + postReconfigGet.body());
      }
      System.out.println("Post-reconfiguration verification passed");
    }
  }

  @Test
  public void testStatePersistedAfterReconfigurationPostHibernation() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    Config.register(
        new String[] {
          "DISABLE_CHECKPOINTING=false",
          "HTTP_AR_FRONTEND_BATCH_ENABLED=true",
          "DEACTIVATION_PERIOD=5000"
        });

    String serviceName = "todo_linearizable";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceName,
          "fadhilkurnia/xdn-todo",
          "/app/data/",
          "LINEARIZABLE",
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

      System.out.println("Waiting for service to be ready");
      HttpResponse<String> readyResponse =
          cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);
      assertTrue(
          readyResponse.statusCode() >= 200 && readyResponse.statusCode() < 500,
          "Service did not return a valid HTTP status: " + readyResponse.statusCode());

      // Create 3 tasks
      String[] tasks = {"task1", "task2", "task3"};
      for (String task : tasks) {
        String jsonBody = String.format("{\"item\":\"%s\"}", task);
        HttpResponse<String> postResponse =
            sendPostRequest(cluster, serviceName, "/api/todo/tasks", jsonBody);
        assertTrue(
            postResponse.statusCode() == 200 || postResponse.statusCode() == 201,
            String.format(
                "POST for %s failed with status %d: %s",
                task, postResponse.statusCode(), postResponse.body()));
        System.out.printf("Created %s (status %d)%n", task, postResponse.statusCode());
      }

      // Verify tasks exist before hibernation
      Thread.sleep(1000);
      HttpResponse<String> preHibernateGet = cluster.sendGetRequest(serviceName, "/api/todo/tasks");
      assertEquals(
          200,
          preHibernateGet.statusCode(),
          "Pre-hibernation GET failed: " + preHibernateGet.body());
      for (String task : tasks) {
        assertTrue(
            preHibernateGet.body().contains(task),
            "Pre-hibernation: expected " + task + " in response: " + preHibernateGet.body());
      }
      System.out.println("Pre-hibernation verification passed");

      // Wait for RC’s Paxos instances to hibernate.
      // With DEACTIVATION_PERIOD=5000, instances become idle after 5s.
      System.out.println("Waiting for Paxos instances to hibernate...");
      Thread.sleep(12000);

      // Trigger reconfiguration after hibernation
      int rcHttpPort = cluster.getReconfiguratorHttpPort();
      String placementUrl =
          String.format(
              "http://127.0.0.1:%d/api/v2/services/%s/placement", rcHttpPort, serviceName);
      JSONObject placementBody = new JSONObject();
      placementBody.put("NODES", new JSONArray().put("AR0").put("AR1").put("AR2"));

      HttpRequest placementRequest =
          HttpRequest.newBuilder()
              .uri(URI.create(placementUrl))
              .timeout(XdnTestCluster.REQUEST_TIMEOUT)
              .header("Content-Type", "application/json")
              .PUT(HttpRequest.BodyPublishers.ofString(placementBody.toString()))
              .build();
      HttpResponse<String> placementResponse =
          HTTP_CLIENT.send(placementRequest, HttpResponse.BodyHandlers.ofString());
      System.out.printf(
          "Post-hibernation reconfiguration response: status=%d body=%s%n",
          placementResponse.statusCode(), placementResponse.body());
      assertEquals(
          200,
          placementResponse.statusCode(),
          "Reconfiguration after hibernation failed: " + placementResponse.body());

      Thread.sleep(10000);

      // Verify tasks still exist after hibernation + reconfiguration
      HttpResponse<String> postReconfigGet = cluster.sendGetRequest(serviceName, "/api/todo/tasks");
      assertEquals(
          200,
          postReconfigGet.statusCode(),
          "Post-hibernation reconfiguration GET failed: " + postReconfigGet.body());
      for (String task : tasks) {
        assertTrue(
            postReconfigGet.body().contains(task),
            "Post-hibernation reconfiguration: expected "
                + task
                + " in response: "
                + postReconfigGet.body());
      }
      System.out.println(
          "Post-hibernation reconfiguration verification passed - all tasks persisted");
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
