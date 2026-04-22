package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umass.cs.xdn.util.XdnTestCluster;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

/**
 * Reproduces a regression where direct client HTTP requests to a follower ActiveReplica's frontend
 * hang indefinitely under Linux CI, even though the follower's container is healthy and it is
 * executing paxos batches originated elsewhere. In the observed failure one specific follower
 * accepts the TCP connection on its frontend port but never produces a response; the request times
 * out.
 *
 * <p>Symptom summary from the original CI log:
 *
 * <ul>
 *   <li>3-node cluster, LINEARIZABLE consistency (paxos coordinator).
 *   <li>Paxos roles: {@code AR1=[C,A]} (leader), {@code AR0=[A]} / {@code AR2=[A]} (followers).
 *   <li>Direct {@code GET /} to the AR0 frontend (follower) returned 308 in ~30 ms.
 *   <li>Direct {@code GET /} to the AR2 frontend (the other follower) never returned; container and
 *       {@code ss -ltn} confirmed AR2 was otherwise healthy.
 *   <li>Every other XDN integration test reaches the cluster through {@code awaitServiceReady} /
 *       {@code invokeService}, which only probe AR0 — that is why the bug went unnoticed until a
 *       test started probing followers directly.
 * </ul>
 *
 * <p>The test launches a LINEARIZABLE service, waits for it to become ready through the cluster
 * HTTP frontend, then issues a {@code GET /} to each replica individually. All three replicas are
 * expected to respond with 308. On a platform/environment that exhibits the regression, at least
 * one replica index will time out.
 */
public class XdnPerReplicaRequestTest {

  @Test
  public void testEveryReplicaServesDirectRequests() throws Exception {
    boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
    assertTrue(isDockerAvailable, "Docker is required for this XDN integration test");

    String serviceName = "xdnperreplica";
    String imageName = "fadhilkurnia/xdn-bookcatalog";

    try (XdnTestCluster cluster = new XdnTestCluster()) {
      cluster.start();

      cluster.launchService(serviceName, imageName, "/app/data/", "LINEARIZABLE", true);

      // At least one replica is reachable through the cluster HTTP frontend.
      HttpResponse<String> aggregate =
          cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);
      assertEquals(308, aggregate.statusCode(), "Service did not return HTTP 308");

      // Every replica's own frontend must answer a direct client GET with 308. A follower whose
      // outbound coordination path is wedged will time out here.
      for (int replicaIdx = 0; replicaIdx < 3; replicaIdx++) {
        HttpResponse<String> replicaResponse =
            cluster.awaitReplicaReady(
                serviceName, replicaIdx, XdnTestCluster.SERVICE_READY_TIMEOUT);
        assertEquals(
            308,
            replicaResponse.statusCode(),
            "Replica " + replicaIdx + " did not return HTTP 308");
      }
    }
  }
}
