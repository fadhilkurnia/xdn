package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

public class XdnMultiServiceTest {

  @Test
  public void testTwoPaxosBasedServices() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceAlpha = "xdnsvcalpha";
    String serviceBeta = "xdnsvcbeta";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceAlpha, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true);
      cluster.launchService(
          serviceBeta, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true);

      Thread.sleep(2000); // wait for services to be created

      System.out.println("Checking service connectivity");
      HttpResponse<String> alphaResponse =
          cluster.awaitServiceReady(serviceAlpha, XdnTestCluster.SERVICE_READY_TIMEOUT);
      HttpResponse<String> betaResponse =
          cluster.awaitServiceReady(serviceBeta, XdnTestCluster.SERVICE_READY_TIMEOUT);

      // both xdn-bookcatalog services should return HTTP 308 (i.e., redirect) with non-empty body
      assertEquals(308, alphaResponse.statusCode(), "Service alpha did not return HTTP 308");
      assertFalse(alphaResponse.body().isEmpty(), "Service alpha returned empty body");
      assertEquals(308, betaResponse.statusCode(), "Service beta did not return HTTP 308");
      assertFalse(betaResponse.body().isEmpty(), "Service beta returned empty body");

      HttpResponse<String> alphaApiResponse = cluster.sendGetRequest(serviceAlpha, "/api/books");
      HttpResponse<String> betaApiResponse = cluster.sendGetRequest(serviceBeta, "/api/books");
      assertEquals(200, alphaApiResponse.statusCode(), "Service alpha did not return HTTP 200");
      assertEquals("[]", alphaApiResponse.body(), "Service alpha did not return empty book list");
      assertEquals(200, betaApiResponse.statusCode(), "Service beta did not return HTTP 200");
      assertEquals("[]", betaApiResponse.body(), "Service beta did not return empty book list");
    }
  }

  @Test
  @DisabledIfEnvironmentVariable(named = "RUNNER_ENVIRONMENT", matches = "github-hosted")
  public void testTwoPrimaryBackupBasedServices() throws Exception {
    // FIXME: this test runs well in a local machine, but fails in Github Actions.
    //  Possible reasons include limited resources in the runner, or incomplete initialization.
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceAlpha = "svc1";
    String serviceBeta = "svc2";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(
          serviceAlpha, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", false);
      cluster.launchService(
          serviceBeta, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", false);

      System.out.println("Waiting for services to be ready...");
      Thread.sleep(2000); // wait for services to be created

      HttpResponse<String> alphaResponse =
          cluster.awaitServiceReady(serviceAlpha, XdnTestCluster.SERVICE_READY_TIMEOUT);
      HttpResponse<String> betaResponse =
          cluster.awaitServiceReady(serviceBeta, XdnTestCluster.SERVICE_READY_TIMEOUT);

      // both xdn-bookcatalog services should return HTTP 308 (i.e., redirect) with non-empty body
      assertEquals(
          308,
          alphaResponse.statusCode(),
          "Service alpha did not return HTTP 308: " + alphaResponse.body());
      assertFalse(alphaResponse.body().isEmpty(), "Service alpha returned empty body");
      assertEquals(
          308,
          betaResponse.statusCode(),
          "Service beta did not return HTTP 308: " + betaResponse.body());
      assertFalse(betaResponse.body().isEmpty(), "Service beta returned empty body");

      HttpResponse<String> alphaApiResponse = cluster.sendGetRequest(serviceAlpha, "/api/books");
      HttpResponse<String> betaApiResponse = cluster.sendGetRequest(serviceBeta, "/api/books");
      assertEquals(200, alphaApiResponse.statusCode(), "Service alpha did not return HTTP 200");
      assertEquals("[]", alphaApiResponse.body(), "Service alpha did not return empty book list");
      assertEquals(200, betaApiResponse.statusCode(), "Service beta did not return HTTP 200");
      assertEquals("[]", betaApiResponse.body(), "Service beta did not return empty book list");
    }
  }
}
