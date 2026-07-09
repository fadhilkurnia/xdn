package edu.umass.cs.xdn2.sandbox;

import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.xdn2.service.ServiceComponent;
import edu.umass.cs.xdn2.service.ServiceInstance;
import edu.umass.cs.xdn2.utils.Shell;
import edu.umass.cs.xdn2.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DockerSandboxManager is the concrete Docker implementation of SandboxManager.
 *
 * It manages the full lifecycle of Docker containers for XDN services:
 *   - Network creation/deletion
 *   - Container start/stop/delete (single and multi-component via docker run)
 *   - State snapshot capture and restore (tar-based, via shell tar)
 *   - Readiness polling via docker inspect health status
 *   - Port allocation via ServerSocket binding check
 *   - Container observability (status, IDs, creation timestamps)
 *
 * Multi-component services run each component as a separate docker run call,
 * joined to the same Docker network. docker compose is not used.
 *
 * Large state transfer (>500KB) uses GigaPaxos LargeCheckpointer, which
 * runs an embedded HTTP server on the old AR to serve the state file to
 * the new AR during reconfiguration.
 */
public class DockerSandboxManager extends SandboxManager {

    // Base directory for all state directories
    // Structure: /tmp/xdn2/state/<nodeId>/<serviceName>/e<epoch>/
    private static final String BASE_STATE_PATH = "/tmp/xdn2/state/";

    // Base directory for final state archives
    // Structure: /tmp/xdn2/final/<nodeId>/<serviceName>/<epoch>/final_state.tar
    private static final String BASE_FINAL_PATH = "/tmp/xdn2/final/";

    // Port allocation range
    private static final int PORT_RANGE_MIN = 50000;
    private static final int PORT_RANGE_MAX = 65000;
    private static final int PORT_ALLOC_MAX_ATTEMPTS = 10;

    // Healthcheck defaults from XdnConfig
    private final int healthcheckIntervalSeconds;
    private final int healthcheckTimeoutSeconds;
    private final int healthcheckRetries;

    // LargeCheckpointer for transferring state > 500KB between ARs
    private final LargeCheckpointer largeCheckpointer;

    // Threshold for inline vs URL-based state transfer (~500KB)
    private static final int MAX_INLINE_STATE_BYTES = 62500;

    private final Logger logger = Logger.getLogger(DockerSandboxManager.class.getName());

    public DockerSandboxManager(String nodeId,
                                int healthcheckIntervalSeconds,
                                int healthcheckTimeoutSeconds,
                                int healthcheckRetries) {
        super(nodeId);
        this.healthcheckIntervalSeconds = healthcheckIntervalSeconds;
        this.healthcheckTimeoutSeconds  = healthcheckTimeoutSeconds;
        this.healthcheckRetries         = healthcheckRetries;
        this.largeCheckpointer = new LargeCheckpointer(
                String.format("%s%s/", BASE_FINAL_PATH, nodeId), nodeId);
    }

    // -------------------------------------------------------------------------
    // Network lifecycle
    // -------------------------------------------------------------------------

    @Override
    public boolean createNetwork(String serviceName) {
        String networkName = buildNetworkName(serviceName);
        String cmd = "docker network create " + networkName;
        int exitCode = Shell.runCommand(cmd, true);
        // exit code 1 means network already exists — treat as success
        if (exitCode != 0 && exitCode != 1) {
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager failed to create network {1}",
                    new Object[]{nodeId, networkName});
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteNetwork(String serviceName) {
        String networkName = buildNetworkName(serviceName);
        String cmd = "docker network rm " + networkName;
        int exitCode = Shell.runCommand(cmd, true);
        if (exitCode != 0 && exitCode != 1) {
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager failed to delete network {1}",
                    new Object[]{nodeId, networkName});
            return false;
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Container lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts all components of the service at the given epoch.
     * Each component is started via docker run with appropriate flags.
     * Each component joins the same Docker network.
     * Components with a healthcheck (client-specified or inferred) have
     * --health-* flags added so Docker tracks readiness internally.
     * waitUntilReady() must be called separately after this method.
     */
    public boolean startService(ServiceInstance instance, int epoch,
                                String mountPath, int allocatedPort) {
        // Same as startService(instance, epoch, mountPath) but uses explicit
        // allocatedPort instead of instance.allocatedHttpPort for the entry component.
        String serviceName = instance.serviceName;
        String networkName = buildNetworkName(serviceName);
        String stateDirMountTarget = instance.property.getStatefulComponentDirectory();
        List<ServiceComponent> components = instance.property.getComponents();

        long statefulCount = components.stream()
                .filter(ServiceComponent::isStateful)
                .count();
        if (statefulCount > 1) {
            logger.log(Level.SEVERE,
                    "{0}:DockerSandboxManager unsupported: {1} stateful containers for {2}",
                    new Object[]{nodeId, statefulCount, serviceName});
            return false;
        }

        int statefulIdx = -1;
        for (int i = 0; i < components.size(); i++) {
            ServiceComponent component = components.get(i);
            if (!component.isStateful()) continue;

            statefulIdx = i;
            String containerName = instance.containerNames.get(i);
            String imageName = component.getImageName();

            Shell.runCommand("mkdir -p " + mountPath, true);

            String healthcheckCmd = component.getHealthcheckCommand();
            if (healthcheckCmd == null || healthcheckCmd.isBlank()) {
                healthcheckCmd = inferHealthcheckCmd(imageName);
            }

            Integer publishedPort = component.getEntryPort();
            Integer allocatedPortForComponent = component.isEntryComponent()
                    ? allocatedPort : null;
            Integer exposedPort = component.getExposedPort();

            boolean started = runDockerContainer(
                    imageName, containerName, networkName,
                    component.getComponentName(), exposedPort, publishedPort,
                    allocatedPortForComponent, mountPath, stateDirMountTarget,
                    component.getEnvironmentVariables(), healthcheckCmd);

            if (!started) {
                logger.log(Level.SEVERE,
                        "{0}:DockerSandboxManager failed to start stateful container {1} for {2}",
                        new Object[]{nodeId, containerName, serviceName});
                return false;
            }

            boolean ready = waitUntilReady(containerName, healthcheckCmd);
            if (!ready) {
                logger.log(Level.SEVERE,
                        "{0}:DockerSandboxManager stateful container {1} failed healthcheck for {2}",
                        new Object[]{nodeId, containerName, serviceName});
                return false;
            }
            break;
        }

        for (int i = 0; i < components.size(); i++) {
            if (i == statefulIdx) continue;

            ServiceComponent component = components.get(i);
            String containerName = instance.containerNames.get(i);
            String imageName = component.getImageName();

            String healthcheckCmd = component.getHealthcheckCommand();
            if (healthcheckCmd == null || healthcheckCmd.isBlank()) {
                healthcheckCmd = inferHealthcheckCmd(imageName);
            }

            Integer publishedPort = component.getEntryPort();
            Integer allocatedPortForComponent = component.isEntryComponent()
                    ? allocatedPort : null;
            Integer exposedPort = component.getExposedPort();

            boolean started = runDockerContainer(
                    imageName, containerName, networkName,
                    component.getComponentName(), exposedPort, publishedPort,
                    allocatedPortForComponent, null, null,
                    component.getEnvironmentVariables(), healthcheckCmd);

            if (!started) {
                logger.log(Level.SEVERE,
                        "{0}:DockerSandboxManager failed to start non-stateful container {1} for {2}",
                        new Object[]{nodeId, containerName, serviceName});
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean startService(ServiceInstance instance, int epoch, String mountPath) {
        return startService(instance, epoch, mountPath, instance.allocatedHttpPort);
    }

    @Override
    public boolean startService(ServiceInstance instance, int epoch) {
        return startService(instance, epoch, getStateDirectory(instance.serviceName, epoch));
    }


    @Override
    public boolean stopService(ServiceInstance instance) {
        boolean allStopped = true;
        for (String containerName : instance.containerNames) {
            int exitCode = Shell.runCommand(
                    "docker container stop " + containerName, true);
            // exit code 1 = container not found, treat as success (idempotent)
            if (exitCode != 0 && exitCode != 1) {
                logger.log(Level.WARNING,
                        "{0}:DockerSandboxManager failed to stop container {1}",
                        new Object[]{nodeId, containerName});
                allStopped = false;
            }
        }
        return allStopped;
    }

    @Override
    public boolean stopContainer(String containerName) {
        int stopCode = Shell.runCommand(
                String.format("docker stop %s", containerName), true);
        if (stopCode != 0) {
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager failed to stop container {1} exit={2}",
                    new Object[]{nodeId, containerName, stopCode});
        }
        int rmCode = Shell.runCommand(
                String.format("docker rm %s", containerName), true);
        if (rmCode != 0) {
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager failed to remove container {1} exit={2}",
                    new Object[]{nodeId, containerName, rmCode});
        }
        return stopCode == 0 && rmCode == 0;
    }

    @Override
    public boolean deleteService(ServiceInstance instance) {
        boolean allDeleted = true;

        // Stop and remove all containers
        for (String containerName : instance.containerNames) {
            int exitCode = Shell.runCommand(
                    "docker container rm --force " + containerName, true);
            if (exitCode != 0 && exitCode != 1) {
                logger.log(Level.WARNING,
                        "{0}:DockerSandboxManager failed to remove container {1}",
                        new Object[]{nodeId, containerName});
                allDeleted = false;
            }
        }

        // Delete the network
        deleteNetwork(instance.serviceName);

        return allDeleted;
    }

    // -------------------------------------------------------------------------
    // State management
    // -------------------------------------------------------------------------

    @Override
    public boolean prepareStateDirectory(String serviceName, int epoch) {
        String stateDir = getStateDirectory(serviceName, epoch);
        Shell.runCommand("rm -rf " + stateDir, true);
        int code = Shell.runCommand("mkdir -p " + stateDir, true);
        return code == 0;
    }

    /**
     * Captures the full state directory as a compressed tar archive.
     * Uses shell `tar -czf` to avoid ZipFiles dependency.
     * Retries up to 10 times if rsync copy fails.
     *
     * @return absolute path to the tar archive, or null on failure
     */
    @Override
    public String captureStateSnapshot(String serviceName, int epoch) {
        String stateDir   = getStateDirectory(serviceName, epoch);
        String stagingDir = String.format("%s%s/%s/%d/staging/",
                BASE_FINAL_PATH, nodeId, serviceName, epoch);
        String tarDir     = String.format("%s%s/%s/%d/",
                BASE_FINAL_PATH, nodeId, serviceName, epoch);
        String tarPath    = tarDir + "final_state.tar";

        // Clean and recreate staging dir
        Shell.runCommand("rm -rf " + stagingDir);
        Shell.runCommand("mkdir -p " + stagingDir);
        Shell.runCommand("mkdir -p " + tarDir);

        // Copy state into staging dir with retries
        String rsyncCmd = String.format("rsync -a %s %s", stateDir, stagingDir);
        boolean copied = false;
        for (int attempt = 1; attempt <= 10; attempt++) {
            int exitCode = Shell.runCommand(rsyncCmd, true);
            if (exitCode == 0) {
                copied = true;
                break;
            }
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager rsync attempt {1}/10 failed for {2}:{3}",
                    new Object[]{nodeId, attempt, serviceName, epoch});
        }
        if (!copied) {
            logger.log(Level.SEVERE,
                    "{0}:DockerSandboxManager failed to capture state for {1}:{2}",
                    new Object[]{nodeId, serviceName, epoch});
            return null;
        }

        // Remove previous archive, create fresh tar
        Shell.runCommand("rm -f " + tarPath);
        int exitCode = Shell.runCommand(
                String.format("tar -czf %s -C %s .", tarPath, stagingDir));
        if (exitCode != 0) {
            logger.log(Level.SEVERE,
                    "{0}:DockerSandboxManager tar failed for {1}:{2}",
                    new Object[]{nodeId, serviceName, epoch});
            return null;
        }

        logger.log(Level.INFO,
                "{0}:DockerSandboxManager captured state snapshot for {1}:{2} → {3}",
                new Object[]{nodeId, serviceName, epoch, tarPath});
        return tarPath;
    }

    /**
     * Restores the state directory from a Base64-encoded tar or a URL.
     * Handles both inline (≤500KB) and large (url:) state formats.
     */
    @Override
    public boolean restoreStateFromSnapshot(String serviceName, int epoch,
                                            String encodedState) {
        String stateDir = getStateDirectory(serviceName, epoch);
        String tarDir   = String.format("%s%s/%s/%d/",
                BASE_FINAL_PATH, nodeId, serviceName, epoch);
        String tarPath  = tarDir + "rcv_final_state.tar";

        Shell.runCommand("mkdir -p " + tarDir);
        Shell.runCommand("rm -f " + tarPath);
        Shell.runCommand("rm -rf " + stateDir);
        Shell.runCommand("mkdir -p " + stateDir);

        // Write the tar file from Base64 or URL
        if (encodedState.startsWith("url:")) {
            String url = encodedState.substring("url:".length());
            LargeCheckpointer.restoreCheckpointHandle(url, tarPath);
        } else {
            byte[] bytes = Base64.getDecoder().decode(encodedState);
            try {
                Files.write(Paths.get(tarPath), bytes,
                        StandardOpenOption.CREATE, StandardOpenOption.DSYNC);
            } catch (IOException e) {
                logger.log(Level.SEVERE,
                        "{0}:DockerSandboxManager failed to write tar for {1}:{2}: {3}",
                        new Object[]{nodeId, serviceName, epoch, e.getMessage()});
                return false;
            }
        }

        // Extract tar into state dir
        int exitCode = Shell.runCommand(
                String.format("tar -xzf %s -C %s", tarPath, stateDir));
        if (exitCode != 0) {
            logger.log(Level.SEVERE,
                    "{0}:DockerSandboxManager tar extract failed for {1}:{2}",
                    new Object[]{nodeId, serviceName, epoch});
            return false;
        }

        logger.log(Level.INFO,
                "{0}:DockerSandboxManager restored state for {1}:{2}",
                new Object[]{nodeId, serviceName, epoch});
        return true;
    }

    /**
     * Returns the absolute path to the state directory for a service epoch.
     * Format: /tmp/xdn2/state/<nodeId>/<serviceName>/e<epoch>/
     */
    @Override
    public String getStateDirectory(String serviceName, int epoch) {
        return String.format("%s%s/%s/e%d/", BASE_STATE_PATH, nodeId, serviceName, epoch);
    }

    /**
     * Produces the final state string for reconfiguration.
     * Inline Base64 for small states, URL via LargeCheckpointer for large ones.
     *
     * @param serviceName  name of the service
     * @param epoch        current epoch
     * @param tarPath      path returned by captureStateSnapshot()
     * @param servicePropertyJson serialized ServiceProperty JSON
     * @return formatted xdn:final:... string, or null on failure
     */
    public String buildFinalStateString(String serviceName, int epoch,
                                        String tarPath, String servicePropertyJson) {
        File archive = new File(tarPath);
        String prefix = String.format("%s%d:",
                edu.umass.cs.xdn2.service.ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX, epoch);

        if (archive.length() <= MAX_INLINE_STATE_BYTES) {
            try {
                byte[] bytes = Files.readAllBytes(Paths.get(tarPath));
                return String.format("%s::%s::%s",
                        prefix,
                        servicePropertyJson,
                        Base64.getEncoder().encodeToString(bytes));
            } catch (IOException e) {
                logger.log(Level.SEVERE,
                        "{0}:DockerSandboxManager failed to read tar for {1}:{2}: {3}",
                        new Object[]{nodeId, serviceName, epoch, e.getMessage()});
                return null;
            }
        } else {
            String url = largeCheckpointer.createCheckpointHandle(nodeId, tarPath);
            return String.format("%s::%s::url:%s", prefix, servicePropertyJson, url);
        }
    }

    // -------------------------------------------------------------------------
    // Readiness
    // -------------------------------------------------------------------------

    /**
     * Polls docker inspect health status until the container reports "healthy".
     * Returns true immediately if healthcheckCmd is null (no healthcheck defined).
     * Uses healthcheck parameters set at construction time from XdnConfig.
     */
    @Override
    public boolean waitUntilReady(String containerName, String healthcheckCmd) {
        if (healthcheckCmd == null || healthcheckCmd.isBlank()) {
            return true;
        }

        String inspectCmd = String.format(
                "docker inspect --format='{{.State.Health.Status}}' %s",
                containerName);

        for (int attempt = 1; attempt <= healthcheckRetries; attempt++) {
            try {
                Thread.sleep(healthcheckIntervalSeconds * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            var result = Shell.runCommandWithOutput(inspectCmd);
            String status = result.stdout.trim().replace("'", "");

            if ("healthy".equals(status)) {
                logger.log(Level.INFO,
                        "{0}:DockerSandboxManager container {1} is healthy after {2} attempts",
                        new Object[]{nodeId, containerName, attempt});
                return true;
            }

            logger.log(Level.INFO,
                    "{0}:DockerSandboxManager waiting for {1}: status={2} attempt={3}/{4}",
                    new Object[]{nodeId, containerName, status, attempt, healthcheckRetries});
        }

        logger.log(Level.WARNING,
                "{0}:DockerSandboxManager container {1} did not become healthy after {2} retries",
                new Object[]{nodeId, containerName, healthcheckRetries});
        return false;
    }

    // -------------------------------------------------------------------------
    // Observability
    // -------------------------------------------------------------------------

    @Override
    public String getContainerStatus(String containerName) {
        return dockerInspect(containerName, "{{.State.Status}}");
    }

    @Override
    public List<String> getContainerIds(List<String> containerNames) {
        List<String> ids = new ArrayList<>(containerNames.size());
        for (String name : containerNames) {
            String id = dockerInspect(name, "{{.Id}}");
            if (id != null) ids.add(id);
        }
        return ids;
    }

    @Override
    public List<String> getContainerCreatedAt(List<String> containerNames) {
        List<String> result = new ArrayList<>(containerNames.size());
        for (String name : containerNames) {
            String raw = dockerInspect(name, "{{.State.StartedAt}}");
            if (raw != null) {
                result.add(formatTimestamp(raw));
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Port allocation
    // -------------------------------------------------------------------------

    /**
     * Allocates an available host port using ServerSocket binding to verify
     * the port is genuinely free at the moment of check.
     */
    @Override
    public int allocatePort() {
        for (int attempt = 0; attempt < PORT_ALLOC_MAX_ATTEMPTS; attempt++) {
            int port = PORT_RANGE_MIN +
                    (int)(Math.random() * (PORT_RANGE_MAX - PORT_RANGE_MIN));
            try (ServerSocket s = new ServerSocket(port)) {
                // Successfully bound — port is free
                return port;
            } catch (IOException e) {
                // Port is taken, try another
            }
        }
        throw new RuntimeException(
                "DockerSandboxManager failed to allocate a free port after " +
                        PORT_ALLOC_MAX_ATTEMPTS + " attempts");
    }

    // -------------------------------------------------------------------------
    // Private: docker run
    // -------------------------------------------------------------------------

    private boolean runDockerContainer(
            String imageName,
            String containerName,
            String networkName,
            String hostName,
            Integer exposedPort,
            Integer publishedPort,
            Integer allocatedHttpPort,
            String mountSource,
            String mountTarget,
            Map<String, String> env,
            String healthcheckCmd) {

        // Remove any stale container with the same name
        Shell.runCommand("docker container rm --force " + containerName, true);

        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        cmd.add("run");
        cmd.add("-d");
        cmd.add("--restart");
        cmd.add("unless-stopped");
        cmd.add("--name=" + containerName);
        cmd.add("--hostname=" + hostName);
        cmd.add("--network=" + networkName);

        // Port publishing
        if (publishedPort != null && allocatedHttpPort != null) {
            cmd.add(String.format("--publish=%d:%d", allocatedHttpPort, publishedPort));
        } else if (publishedPort != null) {
            cmd.add(String.format("--publish=%d:%d", publishedPort, publishedPort));
        } else if (exposedPort != null) {
            cmd.add(String.format("--expose=%d", exposedPort));
        }

        // Bind mount for stateful components
        if (mountSource != null && !mountSource.isBlank()
                && mountTarget != null && !mountTarget.isBlank()) {
            cmd.add("--mount");
            cmd.add(String.format("type=bind,source=%s,target=%s",
                    mountSource, mountTarget));
        }

        // Run as current user for stateful components to avoid permission issues
        // (tested with MySQL, PostgreSQL, MariaDB)
        if (mountTarget != null && !mountTarget.isBlank()) {
            int uid = Utils.getUid();
            int gid = Utils.getGid();
            if (uid != 0) {
                cmd.add("-v");
                cmd.add("/etc/passwd:/etc/passwd:ro");
                cmd.add(String.format("--user=%d:%d", uid, gid));
                cmd.add("--cap-add=sys_nice");
            }
        }

        // Environment variables
        // Environment variables
        if (env != null && !env.isEmpty()) {
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager container={1} env vars: {2}",
                    new Object[]{nodeId, containerName, env});
            for (Map.Entry<String, String> entry : env.entrySet()) {
                cmd.add("--env");
                cmd.add(entry.getKey() + "=" + entry.getValue());
            }
        } else {
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager container={1} no env vars",
                    new Object[]{nodeId, containerName});
        }

        // Healthcheck flags — only if a command is defined
        if (healthcheckCmd != null && !healthcheckCmd.isBlank()) {
            cmd.add("--health-cmd=" + healthcheckCmd);
            cmd.add("--health-interval=" + healthcheckIntervalSeconds + "s");
            cmd.add("--health-timeout=" + healthcheckTimeoutSeconds + "s");
            cmd.add("--health-retries=" + healthcheckRetries);
        }

        cmd.add(imageName);

        int exitCode = Shell.runCommand(cmd, false);
        if (exitCode != 0) {
            logger.log(Level.SEVERE,
                    "{0}:DockerSandboxManager failed to start container {1} (exit={2})",
                    new Object[]{nodeId, containerName, exitCode});
            return false;
        }

        logger.log(Level.INFO,
                "{0}:DockerSandboxManager started container {1}",
                new Object[]{nodeId, containerName});
        return true;
    }

    // -------------------------------------------------------------------------
    // Private: helpers
    // -------------------------------------------------------------------------

    private String buildNetworkName(String serviceName) {
        return String.format("net::%s:%s", nodeId, serviceName);
    }

    /**
     * Infers the healthcheck command from the Docker image name.
     * Only covers known databases. Returns null for unknown images.
     */
    private static String inferHealthcheckCmd(String imageName) {
        if (imageName == null) return null;
        String lower = imageName.toLowerCase();
        if (lower.contains("mysql") || lower.contains("mariadb"))
            return "mysqladmin ping -h 127.0.0.1 --silent";
        if (lower.contains("postgres"))
            return "pg_isready -U postgres";
        return null;
    }

    /**
     * Runs docker inspect with the given Go template format string.
     * Returns the trimmed output, or null if the command fails.
     */
    private String dockerInspect(String containerName, String format) {
        ProcessBuilder pb = new ProcessBuilder(
                "docker", "container", "inspect",
                "--format", format, containerName);
        pb.redirectErrorStream(true);
        try {
            Process process = pb.start();
            String output;
            try (InputStream is = process.getInputStream()) {
                output = new String(is.readAllBytes(), StandardCharsets.ISO_8859_1).trim();
            }
            int exitCode = process.waitFor();
            if (exitCode == 0 && !output.isEmpty()) return output;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
            logger.log(Level.WARNING,
                    "{0}:DockerSandboxManager inspect failed for {1}: {2}",
                    new Object[]{nodeId, containerName, e.getMessage()});
        }
        return null;
    }

    /**
     * Formats an ISO 8601 timestamp into a human-readable "X ago" string.
     */
    private static String formatTimestamp(String iso8601) {
        try {
            Instant startedAt = Instant.parse(iso8601);
            Duration d = Duration.between(startedAt, Instant.now());
            if (d.toDays() > 365)
                return (d.toDays() / 365) + " years ago";
            if (d.toDays() > 30)
                return (d.toDays() / 30) + " months ago";
            if (d.toDays() > 0)
                return d.toDays() + " days ago";
            if (d.toHours() > 0)
                return d.toHours() + " hours ago";
            if (d.toMinutes() > 0)
                return d.toMinutes() + " minutes ago";
            return d.getSeconds() + " seconds ago";
        } catch (DateTimeParseException e) {
            return iso8601;
        }
    }
}