package edu.umass.cs.xdn2.sandbox;

import edu.umass.cs.xdn2.XdnConfig;
import edu.umass.cs.xdn2.service.ServiceInstance;

import java.util.List;

/**
 * SandboxManager is the abstract interface for managing isolated execution
 * environments (sandboxes) in XDN. Concrete implementations support different
 * runtimes such as Docker, Podman, gVisor, Firecracker, or QEMU.
 *
 * A SandboxManager instance is created once per node via SandboxManager.create()
 * and shared between DeterministicService and NonDeterministicService.
 *
 * Responsibilities:
 *   - Network lifecycle (create, delete)
 *   - Container lifecycle (start, stop, delete)
 *   - State management (snapshot, restore, state directory path)
 *   - Readiness polling (wait until container is healthy)
 *   - Observability (status, IDs, creation timestamps)
 *   - Port allocation
 */
public abstract class SandboxManager {

    protected final String nodeId;

    protected SandboxManager(String nodeId) {
        assert nodeId != null && !nodeId.isBlank() : "nodeId must be defined";
        this.nodeId = nodeId;
    }

    /**
     * Creates a SandboxManager instance based on the SANDBOX_TYPE in XdnConfig.
     *
     * @param config XDN configuration
     * @param nodeId ID of this node
     * @return concrete SandboxManager instance
     * @throws RuntimeException if SANDBOX_TYPE is unknown
     */
    public static SandboxManager create(XdnConfig config, String nodeId) {
        return switch (config.getSandboxType()) {
            case DOCKER -> new DockerSandboxManager(
                    nodeId,
                    config.getHealthcheckIntervalSeconds(),
                    config.getHealthcheckTimeoutSeconds(),
                    config.getHealthcheckRetries());

            // future: case PODMAN -> new PodmanSandboxManager(nodeId, ...);
            // future: case GVISOR -> new GVisorSandboxManager(nodeId, ...);
            default -> throw new RuntimeException(
                    "Unknown sandbox type: " + config.getSandboxType());
        };
    }

    // -------------------------------------------------------------------------
    // Network lifecycle
    // -------------------------------------------------------------------------

    /**
     * Creates an isolated network for the given service.
     *
     * @param serviceName name of the service
     * @return true iff the network was created successfully
     */
    public abstract boolean createNetwork(String serviceName);

    /**
     * Deletes the network associated with the given service.
     *
     * @param serviceName name of the service
     * @return true iff the network was deleted successfully
     */
    public abstract boolean deleteNetwork(String serviceName);

    // -------------------------------------------------------------------------
    // Container lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts all containers for the given service instance.
     * For single-component services: runs one docker run.
     * For multi-component services: runs one docker run per component.
     *
     * @param instance the service instance to start
     * @param epoch the epoch number of that service instance
     * @return true iff all containers started successfully
     */
    public abstract boolean startService(ServiceInstance instance, int epoch);

    /**
     * Starts the service with an explicit volume mount path, overriding the default
     * state directory. Used by PrimaryBackupManager to mount primaryLive/, backupLive1/,
     * or backupLive2/ depending on the node's role.
     *
     * @param instance  the service instance to start
     * @param epoch     the epoch number
     * @param mountPath the explicit directory to bind-mount into the container
     * @return true iff all containers started successfully
     */
    public abstract boolean startService(ServiceInstance instance, int epoch, String mountPath);
    public abstract boolean startService(ServiceInstance instance, int epoch, String mountPath, int allocatedPort);

    /**
     * Stops all containers for the given service instance without deleting
     * the state directory. The container can be restarted later.
     *
     * @param instance the service instance to stop
     * @return true iff all containers stopped successfully
     */
    public abstract boolean stopService(ServiceInstance instance);

    /**
     * Stops and removes a single container by name without affecting
     * other containers in the same service.
     *
     * @param containerName name of the container to stop and remove
     * @return true iff the container was successfully stopped and removed
     */
    public abstract boolean stopContainer(String containerName);

    /**
     * Stops and removes all containers for the given service instance,
     * including the state directory and network.
     *
     * @param instance the service instance to delete
     * @return true iff all containers and state were deleted successfully
     */
    public abstract boolean deleteService(ServiceInstance instance);

    // -------------------------------------------------------------------------
    // State management
    // -------------------------------------------------------------------------

    /**
     * Prepares a clean state directory for a fresh service creation.
     * Wipes any existing content and creates the directory.
     * Must NOT be called during reconfiguration — restoreStateFromSnapshot()
     * handles directory setup in that case.
     *
     * @param serviceName name of the service
     * @param epoch       current placement epoch
     * @return true iff the directory was successfully prepared
     */
    public abstract boolean prepareStateDirectory(String serviceName, int epoch);

    /**
     * Captures a full snapshot of the service's state directory as a tar archive.
     * Used during reconfiguration (getFinalState) and Paxos checkpointing
     * (DeterministicService.checkpoint).
     *
     * @param serviceName name of the service
     * @param epoch       current placement epoch
     * @return absolute path to the tar archive, or null on failure
     */
    public abstract String captureStateSnapshot(String serviceName, int epoch);

    /**
     * Restores the service's state directory from a Base64-encoded tar archive
     * or a URL pointing to a large checkpoint.
     * Used when a new replica joins (restore("xdn:final:...")).
     *
     * @param serviceName  name of the service
     * @param epoch        new placement epoch
     * @param encodedState Base64-encoded tar, or "url:<url>" for large states
     * @return true iff the state was restored successfully
     */
    public abstract boolean restoreStateFromSnapshot(String serviceName, int epoch,
                                                     String encodedState);

    /**
     * Returns the absolute path to the state directory for a service epoch.
     * This directory is bind-mounted into the container.
     *
     * @param serviceName name of the service
     * @param epoch       current placement epoch
     * @return absolute path ending with '/'
     */
    public abstract String getStateDirectory(String serviceName, int epoch);

    // -------------------------------------------------------------------------
    // Readiness
    // -------------------------------------------------------------------------

    /**
     * Polls until the container is ready to serve requests.
     *
     * If healthcheckCmd is non-null, polls via docker exec until the command
     * exits 0. The polling interval, timeout, and retry count are set at
     * construction time from XdnConfig.
     *
     * If healthcheckCmd is null, returns true immediately (no polling).
     *
     * @param containerName name of the container to poll
     * @param healthcheckCmd command to run inside the container, or null to skip
     * @return true iff the container became ready within the configured retries
     */
    public abstract boolean waitUntilReady(String containerName, String healthcheckCmd);

    /**
     * Polls an HTTP endpoint on the given host-published port until it
     * responds with a 2xx status, or returns true immediately if path is
     * null. Used for entry (non-stateful) components, whose readiness can't
     * be inferred from a known image name the way stateful DB images can.
     * Interval, timeout, and retry count are set at construction time from
     * XdnConfig, same as waitUntilReady().
     *
     * @param port host-published port to poll
     * @param path HTTP path to GET, or null to skip
     * @return true iff the endpoint became ready within the configured retries
     */
    public abstract boolean waitUntilHttpReady(int port, String path);

    // -------------------------------------------------------------------------
    // Observability
    // -------------------------------------------------------------------------

    /**
     * Returns the current status of a container (e.g. "running", "exited").
     *
     * @param containerName name of the container
     * @return status string, or null if the container does not exist
     */
    public abstract String getContainerStatus(String containerName);

    /**
     * Returns the Docker IDs of the given containers.
     *
     * @param containerNames list of container names to inspect
     * @return list of container IDs in the same order as containerNames
     */
    public abstract List<String> getContainerIds(List<String> containerNames);

    /**
     * Returns the creation timestamps of the given containers.
     *
     * @param containerNames list of container names to inspect
     * @return list of creation timestamps in the same order as containerNames
     */
    public abstract List<String> getContainerCreatedAt(List<String> containerNames);

    // -------------------------------------------------------------------------
    // Infrastructure
    // -------------------------------------------------------------------------

    /**
     * Allocates an available host port for a new service.
     * The port is chosen randomly from the ephemeral range and verified to be
     * free before returning.
     *
     * @return an available host port number
     * @throws RuntimeException if no port could be allocated after retries
     */
    public abstract int allocatePort();
}