package edu.umass.cs.xdn;

import edu.umass.cs.xdn.util.XdnTestCluster;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

public class XdnMultiServiceTest {

    @Test
    public void testTwoPaxosBasedServices() throws Exception {
        boolean isDockerAvailable = XdnTestCluster.isDockerAvailable();
        assertTrue(isDockerAvailable,
                "Docker is required for this XDN integration test");

        String serviceAlpha = "xdnsvcalpha";
        String serviceBeta = "xdnsvcbeta";

        try (XdnTestCluster cluster = new XdnTestCluster()) {
            System.out.println("About to start the cluster ...");
            cluster.start();
        }
//
//            cluster.launchService(serviceAlpha, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true);
//            cluster.launchService(serviceBeta, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", true);
//
//            Thread.sleep(1000); // wait for services to be created
//
//            HttpResponse<String> alphaResponse =
//                    cluster.awaitServiceReady(serviceAlpha, XdnTestCluster.SERVICE_READY_TIMEOUT);
//            HttpResponse<String> betaResponse =
//                    cluster.awaitServiceReady(serviceBeta, XdnTestCluster.SERVICE_READY_TIMEOUT);
//
//            // both xdn-bookcatalog services should return HTTP 308 (i.e., redirect) with non-empty body
//            assertEquals(308, alphaResponse.statusCode(),
//                    "Service alpha did not return HTTP 308");
//            assertFalse(alphaResponse.body().isEmpty(),
//                    "Service alpha returned empty body");
//            assertEquals(308, betaResponse.statusCode(),
//                    "Service beta did not return HTTP 308");
//            assertFalse(betaResponse.body().isEmpty(),
//                    "Service beta returned empty body");
//
//            HttpResponse<String> alphaApiResponse =
//                    cluster.sendGetRequest(serviceAlpha, "/api/books");
//            HttpResponse<String> betaApiResponse =
//                    cluster.sendGetRequest(serviceBeta, "/api/books");
//            assertEquals(200, alphaApiResponse.statusCode(),
//                    "Service alpha did not return HTTP 200");
//            assertEquals("[]", alphaApiResponse.body(),
//                    "Service alpha did not return empty book list");
//            assertEquals(200, betaApiResponse.statusCode(),
//                    "Service beta did not return HTTP 200");
//            assertEquals("[]", betaApiResponse.body(),
//                    "Service beta did not return empty book list");
//        }
    }

    @Test
    @Disabled("Temporary disable for Github Actions debugging")
    public void testTwoPrimaryBackupBasedServices() throws Exception {
        Assumptions.assumeTrue(XdnTestCluster.isDockerAvailable(),
                "Docker is required for this XDN integration test");

        String serviceAlpha = "svc1";
        String serviceBeta = "svc2";

        try (XdnTestCluster cluster = new XdnTestCluster()) {
            cluster.start();

            cluster.launchService(serviceAlpha, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", false);
            cluster.launchService(serviceBeta, "fadhilkurnia/xdn-bookcatalog", "/app/data/", "LINEARIZABLE", false);

            Thread.sleep(1000); // wait for services to be created

            HttpResponse<String> alphaResponse =
                    cluster.awaitServiceReady(serviceAlpha, XdnTestCluster.SERVICE_READY_TIMEOUT);
            HttpResponse<String> betaResponse =
                    cluster.awaitServiceReady(serviceBeta, XdnTestCluster.SERVICE_READY_TIMEOUT);

            // both xdn-bookcatalog services should return HTTP 308 (i.e., redirect) with non-empty body
            assertEquals(308, alphaResponse.statusCode(),
                    "Service alpha did not return HTTP 308: " + alphaResponse.body());
            assertFalse(alphaResponse.body().isEmpty(),
                    "Service alpha returned empty body");
            assertEquals(308, betaResponse.statusCode(),
                    "Service beta did not return HTTP 308: " + betaResponse.body());
            assertFalse(betaResponse.body().isEmpty(),
                    "Service beta returned empty body");

            HttpResponse<String> alphaApiResponse =
                    cluster.sendGetRequest(serviceAlpha, "/api/books");
            HttpResponse<String> betaApiResponse =
                    cluster.sendGetRequest(serviceBeta, "/api/books");
            assertEquals(200, alphaApiResponse.statusCode(),
                    "Service alpha did not return HTTP 200");
            assertEquals("[]", alphaApiResponse.body(),
                    "Service alpha did not return empty book list");
            assertEquals(200, betaApiResponse.statusCode(),
                    "Service beta did not return HTTP 200");
            assertEquals("[]", betaApiResponse.body(),
                    "Service beta did not return empty book list");
        }
    }
}
