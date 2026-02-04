package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class XdnTaggedImageLaunchTest {

  @Test
  public void testLaunchServiceWithTaggedImage() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdntaggedimage";
    String imageName = "fadhilkurnia/xdn-bookcatalog:latest";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(serviceName, imageName, "/app/data/", "LINEARIZABLE", true);

      HttpResponse<String> response =
          cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);

      assertEquals(308, response.statusCode(), "Service did not return HTTP 308");

      // Ensure all replicas are running and return HTTP 308
      for (int replicaIdx = 0; replicaIdx < 3; replicaIdx++) {
        HttpResponse<String> replicaResponse =
            cluster.sendGetRequest(serviceName, replicaIdx, "/", Duration.ofSeconds(10));
        assertEquals(
            308,
            replicaResponse.statusCode(),
            "Replica " + replicaIdx + " did not return HTTP 308");
      }
    }
  }
}
