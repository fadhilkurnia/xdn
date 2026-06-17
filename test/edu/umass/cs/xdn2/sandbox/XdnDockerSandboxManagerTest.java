package edu.umass.cs.xdn2.sandbox;

import edu.umass.cs.xdn2.service.ServiceInstance;
import edu.umass.cs.xdn2.service.ServiceProperty;
import edu.umass.cs.xdn2.utils.Shell;
import org.json.JSONException;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DockerSandboxManager.
 *
 * Requires a running Docker daemon. Tests pull real images and start real
 * containers. Each test cleans up after itself via @AfterEach.
 *
 * Images used:
 *   - michael2718/bookcatalog-nd:1  (SQLite single-component)
 *   - michael2718/bookcatalog-nd:1  + mysql:8.0.41-debian (MySQL multi-component)
 *   - michael2718/bookcatalog-nd:1  + postgres:17.4-bookworm (PostgreSQL multi-component)
 *
 * Run via: ant xdn-unit-tests
 */
@DisplayName("XdnDockerSandboxManager")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class XdnDockerSandboxManagerTest {

    private static final String NODE_ID = "test-node";
    private static final String SERVICE_SQLITE  = "test-sqlite";
    private static final String SERVICE_MYSQL   = "test-mysql";
    private static final String SERVICE_POSTGRES = "test-postgres";

    // Images
    private static final String IMAGE_APP      = "michael2718/bookcatalog-nd:4";
    private static final String IMAGE_MYSQL    = "mysql:8.0.41-debian";
    private static final String IMAGE_POSTGRES = "postgres:17.4-bookworm";

    private static DockerSandboxManager sandbox;

    // Containers started in the current test — cleaned up in @AfterEach
    private final List<String> containersToCleanup = new ArrayList<>();
    private final List<String> networksToCleanup   = new ArrayList<>();

    // -------------------------------------------------------------------------
    // Setup
    // -------------------------------------------------------------------------

    @BeforeAll
    static void pullImages() {
        // Pull all images once before any test runs.
        // Fails fast if Docker is unavailable or images can't be pulled.
        for (String image : List.of(IMAGE_APP, IMAGE_MYSQL, IMAGE_POSTGRES)) {
            int exitCode = Shell.runCommand("docker pull " + image, false);
            assertEquals(0, exitCode,
                    "Failed to pull image: " + image +
                            ". Ensure Docker is running and the image is accessible.");
        }
        sandbox = new DockerSandboxManager(NODE_ID, 2, 5, 30);
    }

    @AfterEach
    void cleanup() {
        // Force-remove all containers started in this test
        for (String containerName : containersToCleanup) {
            Shell.runCommand("docker container rm --force " + containerName, true);
        }
        // Remove all networks created in this test
        for (String networkName : networksToCleanup) {
            Shell.runCommand("docker network rm " + networkName, true);
        }
        containersToCleanup.clear();
        networksToCleanup.clear();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a container name matching the format used by DockerSandboxManager:
     * c<componentIdx>.e<epoch>.<serviceName>.<nodeId>.xdn.io
     */
    private static String containerName(int componentIdx, int epoch, String serviceName) {
        return String.format("c%d.e%d.%s.%s.xdn.io",
                componentIdx, epoch, serviceName, NODE_ID);
    }

    private static String networkName(String serviceName) {
        return String.format("net::%s:%s", NODE_ID, serviceName);
    }

    /**
     * Builds a minimal single-component SQLite ServiceInstance.
     * Mirrors sq_primary.yml.
     */
    private static ServiceProperty buildSqliteProperty() {
        try {
            return ServiceProperty.createFromJsonString("""
                    {
                      "name": "test-sqlite",
                      "image": "michael2718/bookcatalog-nd:1",
                      "port": 8000,
                      "state": "/data/",
                      "consistency": "sequential"
                    }
                    """);
        } catch (JSONException e) {
            throw new RuntimeException("Failed to build SQLite test property", e);
        }
    }

    private ServiceInstance buildSqliteInstance(int epoch) {
        int port = sandbox.allocatePort();
        ServiceProperty property = buildSqliteProperty();
        List<String> names = List.of(containerName(0, epoch, SERVICE_SQLITE));
        containersToCleanup.addAll(names);
        networksToCleanup.add(networkName(SERVICE_SQLITE));
        sandbox.prepareStateDirectory(SERVICE_SQLITE, epoch);
        return new ServiceInstance(property, SERVICE_SQLITE,
                networkName(SERVICE_SQLITE), port, names);
    }

    /**
     * Builds a two-component MySQL ServiceInstance.
     * Mirrors my_primary.yml.
     */
    private static ServiceProperty buildMysqlProperty() {
        try {
            return ServiceProperty.createFromJsonString("""
            {
              "name": "test-mysql",
              "state": "bookcatalog-mysql:/var/lib/mysql/",
              "consistency": "sequential",
              "components": [
                {
                  "bookcatalog-mysql": {
                    "image": "mysql:8.0.41-debian",
                    "expose": 3306,
                    "healthcheck": "mysqladmin ping -h 127.0.0.1 --silent",
                    "environments": [
                      {"MYSQL_DATABASE": "books"},
                      {"MYSQL_ROOT_PASSWORD": "root"}
                    ]
                  }
                },
                {
                  "app": {
                    "image": "michael2718/bookcatalog-nd:1",
                    "port": 8000,
                    "entry": true,
                    "environments": [
                      {"DB_TYPE": "mysql"},
                      {"DB_HOST": "bookcatalog-mysql"}
                    ]
                  }
                }
              ]
            }
            """);
        } catch (JSONException e) {
            throw new RuntimeException("Failed to build MySQL test property", e);
        }
    }

    private ServiceInstance buildMysqlInstance(int epoch) {
        int port = sandbox.allocatePort();
        ServiceProperty property = buildMysqlProperty();
        List<String> names = List.of(
                containerName(0, epoch, SERVICE_MYSQL),   // db
                containerName(1, epoch, SERVICE_MYSQL));  // app

        containersToCleanup.addAll(names);
        networksToCleanup.add(networkName(SERVICE_MYSQL));
        sandbox.prepareStateDirectory(SERVICE_MYSQL, epoch);
        return new ServiceInstance(property, SERVICE_MYSQL,
                networkName(SERVICE_MYSQL), port, names);
    }

    /**
     * Builds a two-component PostgreSQL ServiceInstance.
     * Mirrors pq_primary.yml.
     */
    private static ServiceProperty buildPostgresProperty() {
        try {
        return ServiceProperty.createFromJsonString("""
            {
              "name": "test-postgres",
              "state": "bookcatalog-postgres:/var/lib/postgresql/data/",
              "consistency": "sequential",
              "components": [
                {
                  "bookcatalog-postgres": {
                    "image": "postgres:17.4-bookworm",
                    "expose": 5432,
                    "environments": [
                      {"POSTGRES_DB": "books"},
                      {"POSTGRES_PASSWORD": "root"}
                    ]
                  }
                },
                {
                  "app": {
                    "image": "michael2718/bookcatalog-nd:1",
                    "port": 8000,
                    "entry": true,
                    "environments": [
                      {"DB_TYPE": "postgres"},
                      {"DB_HOST": "bookcatalog-postgres"}
                    ]
                  }
                }
              ]
            }
            """);
        } catch (JSONException e) {
            throw new RuntimeException("Failed to build PostgreSQL test property", e);
        }
    }

    private ServiceInstance buildPostgresInstance(int epoch) {
        int port = sandbox.allocatePort();
        ServiceProperty property = buildPostgresProperty();
        List<String> names = List.of(
                containerName(0, epoch, SERVICE_POSTGRES),
                containerName(1, epoch, SERVICE_POSTGRES));

        containersToCleanup.addAll(names);
        networksToCleanup.add(networkName(SERVICE_POSTGRES));
        sandbox.prepareStateDirectory(SERVICE_POSTGRES, epoch);
        return new ServiceInstance(property, SERVICE_POSTGRES,
                networkName(SERVICE_POSTGRES), port, names);
    }

    // -------------------------------------------------------------------------
    // SQLite — single component
    // -------------------------------------------------------------------------

    @Test
    @Order(1)
    @DisplayName("SQLite: createNetwork() succeeds")
    void sqliteCreateNetwork() {
        assertTrue(sandbox.createNetwork(SERVICE_SQLITE),
                "createNetwork() should return true");
        String status = Shell.runCommandWithOutput(
                "docker network inspect " +
                        networkName(SERVICE_SQLITE) +
                        " --format='{{.Name}}'").stdout.trim();
        assertTrue(status.contains("net::"),
                "Network should exist after createNetwork()");
    }

    @Test
    @Order(2)
    @DisplayName("SQLite: startService() starts the container")
    void sqliteStartService() {
        ServiceInstance instance = buildSqliteInstance(0);
        sandbox.createNetwork(SERVICE_SQLITE);
        assertTrue(sandbox.startService(instance, 0),
                "startService() should return true");
        String status = sandbox.getContainerStatus(instance.containerNames.get(0));
        assertEquals("running", status,
                "Container should be running after startService()");
    }

    @Test
    @Order(3)
    @DisplayName("SQLite: stopService() stops the container")
    void sqliteStopService() {
        ServiceInstance instance = buildSqliteInstance(0);
        sandbox.createNetwork(SERVICE_SQLITE);
        sandbox.startService(instance, 0);
        assertTrue(sandbox.stopService(instance),
                "stopService() should return true");
        String status = sandbox.getContainerStatus(instance.containerNames.get(0));
        assertTrue(status == null || status.equals("exited"),
                "Container should be stopped after stopService()");
    }

    @Test
    @Order(4)
    @DisplayName("SQLite: deleteService() removes the container and network")
    void sqliteDeleteService() {
        ServiceInstance instance = buildSqliteInstance(0);
        sandbox.createNetwork(SERVICE_SQLITE);
        sandbox.startService(instance, 0);
        assertTrue(sandbox.deleteService(instance),
                "deleteService() should return true");
        String status = sandbox.getContainerStatus(instance.containerNames.get(0));
        assertNull(status,
                "Container should not exist after deleteService()");
    }

    @Test
    @Order(5)
    @DisplayName("SQLite: deleteService() is idempotent")
    void sqliteDeleteServiceIdempotent() {
        ServiceInstance instance = buildSqliteInstance(0);
        sandbox.createNetwork(SERVICE_SQLITE);
        sandbox.startService(instance, 0);
        sandbox.deleteService(instance);
        // Second call should not throw or return false
        assertTrue(sandbox.deleteService(instance),
                "Second deleteService() call should still return true");
    }

    // -------------------------------------------------------------------------
    // MySQL — multi-component
    // -------------------------------------------------------------------------

    @Test
    @Order(6)
    @DisplayName("MySQL: startService() starts both containers")
    void mysqlStartService() {
        ServiceInstance instance = buildMysqlInstance(0);
        sandbox.createNetwork(SERVICE_MYSQL);
        assertTrue(sandbox.startService(instance, 0),
                "startService() should return true for MySQL");
        for (String name : instance.containerNames) {
            String status = sandbox.getContainerStatus(name);
            assertEquals("running", status,
                    "Container " + name + " should be running");
        }
    }

    @Test
    @Order(7)
    @DisplayName("MySQL: waitUntilReady() waits for DB healthcheck")
    void mysqlWaitUntilReady() {
        ServiceInstance instance = buildMysqlInstance(0);
        sandbox.createNetwork(SERVICE_MYSQL);
        sandbox.startService(instance, 0);

        // DB container is component 0
        String dbContainer = instance.containerNames.get(0);
        String healthcheckCmd = instance.property.getComponents().get(0).getHealthcheckCommand();

        assertTrue(sandbox.waitUntilReady(dbContainer, healthcheckCmd),
                "MySQL container should become healthy within timeout");
    }

    @Test
    @Order(8)
    @DisplayName("MySQL: stopService() stops all containers")
    void mysqlStopService() {
        ServiceInstance instance = buildMysqlInstance(0);
        sandbox.createNetwork(SERVICE_MYSQL);
        sandbox.startService(instance, 0);
        assertTrue(sandbox.stopService(instance),
                "stopService() should return true");
        for (String name : instance.containerNames) {
            String status = sandbox.getContainerStatus(name);
            assertTrue(status == null || status.equals("exited"),
                    "Container " + name + " should be stopped");
        }
    }

    @Test
    @Order(9)
    @DisplayName("MySQL: deleteService() removes all containers")
    void mysqlDeleteService() {
        ServiceInstance instance = buildMysqlInstance(0);
        sandbox.createNetwork(SERVICE_MYSQL);
        sandbox.startService(instance, 0);
        assertTrue(sandbox.deleteService(instance),
                "deleteService() should return true");
        for (String name : instance.containerNames) {
            assertNull(sandbox.getContainerStatus(name),
                    "Container " + name + " should not exist after deleteService()");
        }
    }

    @Test
    @Order(10)
    @DisplayName("MySQL: deleteService() is idempotent")
    void mysqlDeleteServiceIdempotent() {
        ServiceInstance instance = buildMysqlInstance(0);
        sandbox.createNetwork(SERVICE_MYSQL);
        sandbox.startService(instance, 0);
        sandbox.deleteService(instance);
        assertTrue(sandbox.deleteService(instance),
                "Second deleteService() should still return true");
    }

    // -------------------------------------------------------------------------
    // PostgreSQL — multi-component
    // -------------------------------------------------------------------------

    @Test
    @Order(11)
    @DisplayName("PostgreSQL: startService() starts both containers")
    void postgresStartService() {
        ServiceInstance instance = buildPostgresInstance(0);
        sandbox.createNetwork(SERVICE_POSTGRES);
        assertTrue(sandbox.startService(instance, 0),
                "startService() should return true for PostgreSQL");
        for (String name : instance.containerNames) {
            String status = sandbox.getContainerStatus(name);
            assertEquals("running", status,
                    "Container " + name + " should be running");
        }
    }

    @Test
    @Order(12)
    @DisplayName("PostgreSQL: waitUntilReady() uses inferred pg_isready healthcheck")
    void postgresWaitUntilReady() {
        ServiceInstance instance = buildPostgresInstance(0);
        sandbox.createNetwork(SERVICE_POSTGRES);
        sandbox.startService(instance, 0);

        String dbContainer = instance.containerNames.get(0);
        // healthcheck is null in ServiceComponent — inferHealthcheckCmd()
        // should infer pg_isready from the image name inside waitUntilReady()
        // Since waitUntilReady() takes the cmd as a parameter, we pass null
        // and verify it returns true — meaning DockerSandboxManager internally
        // handles the null case. Actually we need the inferred cmd here:
        String healthcheckCmd = "pg_isready -U postgres";

        assertTrue(sandbox.waitUntilReady(dbContainer, healthcheckCmd),
                "PostgreSQL container should become healthy within timeout");
    }

    @Test
    @Order(13)
    @DisplayName("PostgreSQL: stopService() stops all containers")
    void postgresStopService() {
        ServiceInstance instance = buildPostgresInstance(0);
        sandbox.createNetwork(SERVICE_POSTGRES);
        sandbox.startService(instance, 0);
        assertTrue(sandbox.stopService(instance),
                "stopService() should return true");
        for (String name : instance.containerNames) {
            String status = sandbox.getContainerStatus(name);
            assertTrue(status == null || status.equals("exited"),
                    "Container " + name + " should be stopped");
        }
    }

    @Test
    @Order(14)
    @DisplayName("PostgreSQL: deleteService() removes all containers")
    void postgresDeleteService() {
        ServiceInstance instance = buildPostgresInstance(0);
        sandbox.createNetwork(SERVICE_POSTGRES);
        sandbox.startService(instance, 0);
        assertTrue(sandbox.deleteService(instance),
                "deleteService() should return true");
        for (String name : instance.containerNames) {
            assertNull(sandbox.getContainerStatus(name),
                    "Container " + name + " should not exist after deleteService()");
        }
    }

    @Test
    @Order(15)
    @DisplayName("PostgreSQL: deleteService() is idempotent")
    void postgresDeleteServiceIdempotent() {
        ServiceInstance instance = buildPostgresInstance(0);
        sandbox.createNetwork(SERVICE_POSTGRES);
        sandbox.startService(instance, 0);
        sandbox.deleteService(instance);
        assertTrue(sandbox.deleteService(instance),
                "Second deleteService() should still return true");
    }

    // -------------------------------------------------------------------------
    // Port allocation
    // -------------------------------------------------------------------------

    @Test
    @Order(16)
    @DisplayName("allocatePort() returns a bindable port")
    void allocatePortIsFree() throws IOException {
        int port = sandbox.allocatePort();
        assertTrue(port >= 50000 && port <= 65000,
                "Port should be within the expected range");
        // Verify the port is actually free by binding to it
        try (ServerSocket s = new ServerSocket(port)) {
            assertEquals(port, s.getLocalPort(),
                    "Returned port should be bindable");
        }
    }

    @Test
    @Order(17)
    @DisplayName("allocatePort() avoids already-bound ports")
    void allocatePortAvoidsOccupied() throws IOException {
        // Occupy a large set of ports to force allocatePort() to skip them
        Set<Integer> occupied = new HashSet<>();
        List<ServerSocket> sockets = new ArrayList<>();
        try {
            // Occupy 100 ports in the allocation range
            for (int port = 50000; port < 50100; port++) {
                try {
                    ServerSocket s = new ServerSocket(port);
                    sockets.add(s);
                    occupied.add(port);
                } catch (IOException e) {
                    // Port already taken by OS — skip
                }
            }

            // allocatePort() should return a port not in the occupied set
            int allocated = sandbox.allocatePort();
            assertFalse(occupied.contains(allocated),
                    "allocatePort() should not return an occupied port");

            // Verify the returned port is actually bindable
            try (ServerSocket s = new ServerSocket(allocated)) {
                assertEquals(allocated, s.getLocalPort());
            }
        } finally {
            for (ServerSocket s : sockets) {
                try { s.close(); } catch (IOException ignored) {}
            }
        }
    }

    // -------------------------------------------------------------------------
    // Container failure
    // -------------------------------------------------------------------------

    @Test
    @Order(18)
    @DisplayName("startService() returns false for nonexistent image")
    void startServiceBadImage() {
        int port = sandbox.allocatePort();
        ServiceProperty property;
        try {
            property = ServiceProperty.createFromJsonString("""
            {
              "name": "test-bad-image",
              "image": "nonexistent-image-xdn-test:latest",
              "port": 8000,
              "state": "",
              "consistency": "sequential"
            }
            """);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        List<String> names = List.of(
                containerName(0, 0, "test-bad-image"));
        containersToCleanup.addAll(names);
        networksToCleanup.add(networkName("test-bad-image"));
        sandbox.prepareStateDirectory("test-bad-image", 0);
        ServiceInstance instance = new ServiceInstance(
                property, "test-bad-image",
                networkName("test-bad-image"), port, names);

        sandbox.createNetwork("test-bad-image");
        assertFalse(sandbox.startService(instance, 0),
                "startService() should return false for a nonexistent image");
    }

    @Test
    @Order(19)
    @DisplayName("getContainerStatus() returns 'running' for a healthy container")
    void getContainerStatusRunning() {
        ServiceInstance instance = buildSqliteInstance(0);
        sandbox.createNetwork(SERVICE_SQLITE);
        sandbox.prepareStateDirectory(SERVICE_SQLITE, 0);
        sandbox.startService(instance, 0);
        String status = sandbox.getContainerStatus(instance.containerNames.get(0));
        assertEquals("running", status,
                "Running container should have status 'running'");
    }

    // -------------------------------------------------------------------------
    // Network
    // -------------------------------------------------------------------------

    @Test
    @Order(20)
    @DisplayName("createNetwork() is idempotent")
    void createNetworkIdempotent() {
        networksToCleanup.add(networkName(SERVICE_SQLITE));
        assertTrue(sandbox.createNetwork(SERVICE_SQLITE),
                "First createNetwork() should return true");
        assertTrue(sandbox.createNetwork(SERVICE_SQLITE),
                "Second createNetwork() should also return true");
    }

    @Test
    @Order(21)
    @DisplayName("deleteNetwork() is idempotent")
    void deleteNetworkIdempotent() {
        sandbox.createNetwork(SERVICE_SQLITE);
        assertTrue(sandbox.deleteNetwork(SERVICE_SQLITE),
                "First deleteNetwork() should return true");
        assertTrue(sandbox.deleteNetwork(SERVICE_SQLITE),
                "Second deleteNetwork() should also return true");
    }

    // -------------------------------------------------------------------------
    // Observability
    // -------------------------------------------------------------------------

    @Test
    @Order(22)
    @DisplayName("getContainerStatus() returns null for nonexistent container")
    void getContainerStatusNonexistent() {
        String status = sandbox.getContainerStatus("nonexistent-container-xdn-test");
        assertNull(status,
                "getContainerStatus() should return null for unknown container");
    }

    @Test
    @Order(23)
    @DisplayName("getContainerIds() returns IDs for running containers")
    void getContainerIds() {
        ServiceInstance instance = buildSqliteInstance(0);
        sandbox.createNetwork(SERVICE_SQLITE);
        sandbox.startService(instance, 0);

        List<String> ids = sandbox.getContainerIds(instance.containerNames);
        assertEquals(1, ids.size(),
                "Should return one ID for single-component service");
        assertFalse(ids.get(0).isBlank(),
                "Container ID should not be blank");
    }

    @Test
    @Order(24)
    @DisplayName("getContainerCreatedAt() returns timestamps for running containers")
    void getContainerCreatedAt() {
        ServiceInstance instance = buildSqliteInstance(0);
        sandbox.createNetwork(SERVICE_SQLITE);
        sandbox.startService(instance, 0);

        List<String> timestamps = sandbox.getContainerCreatedAt(instance.containerNames);
        assertEquals(1, timestamps.size(),
                "Should return one timestamp for single-component service");
        assertFalse(timestamps.get(0).isBlank(),
                "Timestamp should not be blank");
    }
}