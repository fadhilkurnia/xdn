package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.primarybackup.packets.ChangePrimaryPacket;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.json.JSONObject;

/** Test Replica Set, consisting of multiple replicas in a single primary-backup cluster. */
public class PrimaryBackupTestReplicaSet {

  private record PrimaryBackupNode(
      PrimaryBackupReplicaCoordinator<String> coordinator,
      PrimaryBackupManager<String> manager,
      MonotonicTestApp app,
      JSONMessenger<String> messenger) {}

  private final String serviceName;
  private final Map<String, PrimaryBackupNode> instances = new HashMap<>();

  public static PrimaryBackupTestReplicaSet initialize(int numInstances) {
    return new PrimaryBackupTestReplicaSet(numInstances);
  }

  private PrimaryBackupTestReplicaSet(int numInstances) {
    String CONTROL_PLANE_ID_PREFIX = "rc";
    String NODE_ID_PREFIX = "node";

    // Prepares the configuration
    Map<String, InetSocketAddress> controlPlaneAddress = new HashMap<>();
    controlPlaneAddress.put(
        CONTROL_PLANE_ID_PREFIX + "0", new InetSocketAddress("localhost", 3000));
    Map<String, InetSocketAddress> nodeAddresses = new HashMap<>();
    for (int i = 0; i < numInstances; i++) {
      nodeAddresses.put(NODE_ID_PREFIX + i, new InetSocketAddress("localhost", 2000 + i));
    }
    ReconfigurableNodeConfig<String> config =
        new DefaultNodeConfig<>(nodeAddresses, controlPlaneAddress);

    // Prepares the active nodes.
    Set<String> nodeIds = new HashSet<>();
    for (int i = 0; i < numInstances; i++) {
      String nodeId = NODE_ID_PREFIX + i;
      MonotonicTestApp app = new MonotonicTestApp();
      ReconfigurationPacketDemultiplexer pd =
          new ReconfigurationPacketDemultiplexer(config).setThreadName(nodeId);
      JSONMessenger<String> messenger = createNodeDefaultMessenger(nodeId, config, pd);
      PrimaryBackupReplicaCoordinator<String> coordinator =
          new PrimaryBackupReplicaCoordinator<>(app, nodeId, config, messenger);

      // Registers tha packet handler into the coordinator.
      pd.setAppRequestParser(coordinator.getRequestParser());
      pd.register(
          PrimaryBackupManager.getAllPrimaryBackupPacketTypes(),
          (parsedMessage, header) -> {
            try {
              coordinator.coordinateRequest(parsedMessage, null);
            } catch (RequestParseException | IOException e) {
              throw new RuntimeException(e);
            }
            return true;
          });

      nodeIds.add(nodeId);
      this.instances.put(
          nodeId,
          new PrimaryBackupNode(
              coordinator, coordinator.getPrimaryBackupManager(), app, messenger));
    }

    // Prepares a randomly generated service name. We need to ensure it is less than
    // 40 chars, thus we only take the last part of the generated UUID.
    String serviceUuid = UUID.randomUUID().toString();
    String[] serviceUuidParts = serviceUuid.split("-");
    String serviceUuidSuffix = serviceUuidParts[serviceUuidParts.length - 1];
    this.serviceName = "test-service-" + serviceUuidSuffix;

    // Creates the replica group in all the replica instances.
    int initialPlacementEpoch = 0;
    for (Map.Entry<String, PrimaryBackupNode> entry : instances.entrySet()) {
      AbstractReplicaCoordinator<String> coordinator = entry.getValue().coordinator();
      coordinator.createReplicaGroup(serviceName, initialPlacementEpoch, null, nodeIds);
    }
  }

  private static JSONMessenger<String> createNodeDefaultMessenger(
      String nodeId, NodeConfig<String> config, AbstractPacketDemultiplexer<Request> pd) {
    try {
      MessageNIOTransport<String, JSONObject> nioTransport =
          new MessageNIOTransport<>(
              nodeId, config, pd, true, SSLDataProcessingWorker.SSL_MODES.CLEAR);
      assert (nioTransport
              .getListeningSocketAddress()
              .equals(
                  new InetSocketAddress(config.getNodeAddress(nodeId), config.getNodePort(nodeId))))
          : "Unable to start NIOTransport at socket address";
      return new JSONMessenger<>(nioTransport);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int getNumInstances() {
    return instances.size();
  }

  public String getServiceName() {
    return this.serviceName;
  }

  public void setPrimaryNode(String nodeId) {
    // Validates the given nodeId.
    Set<String> nodeIds = instances.keySet();
    if (!nodeIds.contains(nodeId)) {
      throw new IllegalArgumentException(
          "Invalid node ID: " + nodeId + ". Current nodes: " + String.join(", ", nodeIds));
    }
    PrimaryBackupNode node = instances.get(nodeId);

    // Synchronously changes the primary.
    CountDownLatch latch = new CountDownLatch(1);
    ChangePrimaryPacket changePrimaryPacket = new ChangePrimaryPacket(serviceName, nodeId);
    boolean is_success =
        node.manager()
            .handlePrimaryBackupPacket(
                changePrimaryPacket,
                (request, handled) -> {
                  assert request instanceof ChangePrimaryPacket;
                  ChangePrimaryPacket reqInCallback = (ChangePrimaryPacket) request;
                  assert changePrimaryPacket.getRequestID() == reqInCallback.getRequestID();
                  assert handled;
                  latch.countDown();
                });
    assert is_success : "Failed to initiate primary change";
  }

  public String getPrimaryNode() {
    for (Map.Entry<String, PrimaryBackupNode> entry : instances.entrySet()) {
      String nodeId = entry.getKey();
      PrimaryBackupManager<String> manager = entry.getValue().manager();
      if (manager.isCurrentPrimary(serviceName)) {
        return nodeId;
      }
    }
    throw new IllegalStateException("Unknown primary of the cluster for " + serviceName);
  }

  public Set<String> getNodeIds() {
    return instances.keySet();
  }

  public void destroy() {
    for (Map.Entry<String, PrimaryBackupNode> entry : instances.entrySet()) {
      entry.getValue().coordinator.close();
    }

    // Removes the primary-backup consensus state.
    try {
      String command = "rm -rf /tmp/gigapaxos/pb_paxos_logs/";
      ProcessBuilder pb = new ProcessBuilder();
      pb.command(command.split("\\s+"));
      Process process = pb.start();
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new RuntimeException("failed to remove previous state");
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
