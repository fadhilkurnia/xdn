package edu.umass.cs.xdn;

import edu.umass.cs.xdn.util.XdnTestCluster;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

public class XdnGetReplicaInfoTest {
    
    @Test
    public void testGetReplicaInfoSingleService() throws Exception {
        boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
        assertTrue(isDockerAvailable,
                "Docker is required for this XDN integration test");

        String serviceName = "xdntestservice";

        try (XdnTestCluster cluster = new XdnTestCluster()) {
            cluster.start();

            // Launch a service with Paxos replication (deterministic=true)
            cluster.launchService(serviceName, "fadhilkurnia/xdn-bookcatalog", 
                "/app/data/", "LINEARIZABLE", true);

            Thread.sleep(2000); // wait for service to be created

            System.out.println("Checking service connectivity");
            HttpResponse<String> response = 
                cluster.awaitServiceReady(serviceName, XdnTestCluster.SERVICE_READY_TIMEOUT);
            
            // Service should be ready and running
            assertEquals(308, response.statusCode(),
                    "Service did not return HTTP 308");
            assertFalse(response.body().isEmpty(),
                    "Service returned empty body");

            // Get replica info for the service
            HttpResponse<String> infoResponse = 
                cluster.sendGetRequest(serviceName, "/api/v2/services/" + serviceName + "/replica/info");
            assertEquals(200, infoResponse.statusCode(),
                    "Replica info request failed");
            
            // Parse and validate response
            JSONObject info = new JSONObject(infoResponse.body());
            
            // Verify protocol information
            assertNotNull(info.getString("replica"), "Missing replica ID");
            assertEquals("PaxosReplicaCoordinator", info.getString("protocol"),
                "Expected PaxosReplicaCoordinator protocol");
            assertEquals("LINEARIZABLE", info.getString("requestedConsistency"),
                "Wrong requested consistency model");
            assertEquals("LINEARIZABILITY", info.getString("consistency"),
                "Wrong consistency model");
            assertNotNull(info.getString("role"), "Missing protocol role");

            // Verify container metadata
            JSONArray containers = info.getJSONArray("containers");
            assertNotNull(containers, "Missing containers array");
            assertTrue(containers.length() > 0, "No containers found");

            // At least one container should be running
            boolean hasRunningContainer = false;
            for (int i = 0; i < containers.length(); i++) {
                JSONObject container = containers.getJSONObject(i);
                
                // Each container should have all required fields
                assertNotNull(container.getString("id"), "Container missing ID");
                assertNotNull(container.getString("created_at"), "Container missing creation time");
                assertNotNull(container.getString("status"), "Container missing status");

                // Creation time should be in human readable format
                assertTrue(container.getString("created_at").contains(" ago"),
                    "Creation time not in human readable format");

                if ("running".equalsIgnoreCase(container.getString("status"))) {
                    hasRunningContainer = true;
                }
            }
            assertTrue(hasRunningContainer, "No running containers found");
        }
    }

    @Test
    public void testGetReplicaInfoNonExistentService() throws Exception {
        boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
        assertTrue(isDockerAvailable,
                "Docker is required for this XDN integration test");

        try (XdnTestCluster cluster = new XdnTestCluster()) {
            cluster.start();

            // Try to get info for non-existent service
            HttpResponse<String> infoResponse = 
                cluster.sendGetRequest("nonexistentservice", "/api/v2/services/nonexistentservice/replica/info");
            
            // Should return an error
            assertEquals(404, infoResponse.statusCode(),
                    "Expected 404 for non-existent service");
        }
    }
}
