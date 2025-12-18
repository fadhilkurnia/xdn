package edu.umass.cs.primarybackup;

import edu.umass.cs.primarybackup.examples.MonotonicAppRequest;
import edu.umass.cs.primarybackup.packets.ChangePrimaryPacket;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import org.junit.Test;

// TODO: implement me!
public class PrimaryIntegrityTest {

  @Test
  public void PrimaryIntegrityTest_SendRequestsToPrimaryNode()
      throws InterruptedException, RequestParseException, IOException {

    String scenario =
        """

                Scenario for TestSendRequestToPrimaryNode:
                  1. Create and initialize 3 active replicas: AR0, AR1, AR2.
                  2. Create and initialize replica group using PrimaryBackupReplicaCoordinator with
                     MonotonicApp as the BackupableApplication (extended in the MonotonicTestApp).
                  3. AR1 is elected as Paxos' coordinator and as the primary given the option:
                     ENABLE_STARTUP_LEADER_ELECTION=false.
                  4. Send client requests only to AR0, the primary.
                  5. Assert that all the replicas have the same state at the end.

                Expectation:
                  - At the end, all the replicas must have the same state, even for non-
                    deterministic application, such as MonotonicApp.

                """;
    System.out.println(scenario);

    PrimaryIntegrityTestHelper.cleanPreviousState();
    System.out.print("\n\n\n\n ===== Step-0: Preparing config ... \n\n");
    ReconfigurableNodeConfig<String> config =
        new DefaultNodeConfig<>(
            PrimaryIntegrityTestHelper.getDefaultActiveReplicas(),
            PrimaryIntegrityTestHelper.getDefaultReconfigurators());
    PrimaryIntegrityTestHelper.printServers(config);
    Thread.sleep(1000);

    System.out.print("\n\n\n\n ===== Step-1: Initializing PrimaryBackup in 3 nodes ... \n\n");
    var servers = PrimaryIntegrityTestHelper.startThreeNodesWithMonotonicApp(config);
    var node1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).coordinator();
    var node2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).coordinator();
    var node3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).coordinator();
    var appAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).app();
    var appAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).app();
    var appAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).app();
    System.out.print("\n\n\n\n ===== PrimaryBackupManagers are initialized ===== \n\n");
    Thread.sleep(3000);

    System.out.print("\n\n\n\n ===== Step-2: Initializing Applications in 3 nodes ... \n\n");
    // FIXME: Current Paxos implementation discard Accept and Commit of not-yet-created replica-
    //  group. We need to add 500ms delay here to ensure that Paxos replica-group in node1 and
    //  node3 are created before node2 send Accept/Commit packets.
    //  With ENABLE_STARTUP_LEADER_ELECTION=false we know that node2 will be the Paxos'
    //  coordinator, thus we create replica-group in node2 after the replica-group in node1 and
    //  node3 are created.
    //  Without delay, a replica can get stuck because the replica cannot execute slot i without
    //  executing the previous slot i-1.
    //  .
    //  There are two potential approaches to fix this liveness issue:
    //  (1) Make the Paxos' coordinator more proactive.
    //      The coordinator can keep track the last slot acknowledged by the Paxos followers and
    //      always send BatchCommit since the last known acknowledged slot.
    //  (2) Make the Paxos' follower more proactive.
    //      When the follower (i.e., Acceptor) receives Commit of slot i while having no value
    //      for slot i-1, the follower proactively ask the coordinator to resend Commit for
    //      the slot i-1.
    int zeroPlacementEpoch = 0;
    String initialState = null;
    String serviceName = PrimaryIntegrityTestHelper.SERVICE_NAME;
    Set<String> nodes =
        Set.of(
            PrimaryIntegrityTestHelper.NODE_1_ID,
            PrimaryIntegrityTestHelper.NODE_2_ID,
            PrimaryIntegrityTestHelper.NODE_3_ID);
    node1.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    node3.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    Thread.sleep(500);
    node2.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    System.out.printf(" replica-group: %s \n", node1.getReplicaGroup(serviceName));
    System.out.print("\n\n\n\n ===== Applications are initialized ===== \n\n\n\n");
    Thread.sleep(3000);

    System.out.print("\n\n\n\n ===== Step-3: Sending app requests to the primary ... \n\n");
    assert node2.getPrimaryBackupManager().isCurrentPrimary(serviceName)
        : "With `ENABLE_STARTUP_LEADER_ELECTION=false`, node2 must deterministically "
            + "be the primary";
    for (int i = 0; i < 10; i++) {
      // preparing the request packet
      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);

      // handling the request packet
      node2.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handler) -> {
            System.out.printf("\n\nrequest is executed :) %s \n\n", executedRequest);
          });
      Thread.sleep(1000);
    }

    Thread.sleep(3000);
    System.out.print("\n\n\n ==========\n");
    System.out.printf(
        "++ monotonic-sequence at node 1: %s\n", appAtNode1.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 2: %s\n", appAtNode2.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 3: %s\n", appAtNode2.test_GetSequenceAsString());

    appAtNode1.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode2.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode3.test_AssertMonotonicallyIncreasingNumbers();
    assert appAtNode1.test_GetSequenceAsString() != null
        : "The end state must not be null as we sent several app requests";
    assert !appAtNode1.test_GetSequenceAsString().isEmpty()
        : "The end state must not be empty as we sent several app requests";
    assert Objects.equals(
            appAtNode1.test_GetSequenceAsString(), appAtNode2.test_GetSequenceAsString())
        : "State in node1 != node2. At the end all replicas must have the same state";
    assert Objects.equals(
            appAtNode2.test_GetSequenceAsString(), appAtNode3.test_GetSequenceAsString())
        : "State in node2 != node3. At the end all replicas must have the same state";

    PrimaryIntegrityTestHelper.killServers(servers);
  }

  @Test
  public void PrimaryIntegrityTest_SendRequestsToAllNodes()
      throws InterruptedException, IOException, RequestParseException {

    String scenario =
        """

                Scenario for TestSendRequestToAllNodes:
                  1. Create and initialize 3 active replicas: AR0, AR1, AR2.
                  2. Create and initialize replica group using PrimaryBackupReplicaCoordinator with
                     MonotonicApp as the BackupableApplication (extended in the MonotonicTestApp).
                  3. AR1 is elected as Paxos' coordinator and as the primary given the option:
                     ENABLE_STARTUP_LEADER_ELECTION=false.
                  4. Send client requests to AR0, AR1, and AR2, respectively.
                  5. Assert that all the replicas have the same state at the end.

                Expectation:
                  - Sending client requests to non-primary nodes should work. The entry node
                    will forward the requests to the primary.
                  - At the end, all the replicas must have the same state, even for non-
                    deterministic application, such as MonotonicApp.


                """;
    System.out.println(scenario);

    PrimaryIntegrityTestHelper.cleanPreviousState();

    System.out.print("\n\n ===== Step-0: Preparing config ... \n\n");
    ReconfigurableNodeConfig<String> config =
        new DefaultNodeConfig<>(
            PrimaryIntegrityTestHelper.getDefaultActiveReplicas(),
            PrimaryIntegrityTestHelper.getDefaultReconfigurators());
    PrimaryIntegrityTestHelper.printServers(config);
    Thread.sleep(1000);

    System.out.print("\n\n ===== Step-1: Initializing PrimaryBackup in 3 nodes ... \n\n");
    var servers = PrimaryIntegrityTestHelper.startThreeNodesWithMonotonicApp(config);
    var node1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).coordinator();
    var node2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).coordinator();
    var node3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).coordinator();
    var appAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).app();
    var appAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).app();
    var appAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).app();
    System.out.print("\n\n ===== PrimaryBackupManagers are initialized ===== \n\n");
    Thread.sleep(3000);

    System.out.print("\n\n ===== Step-2: Initializing Applications in 3 nodes ... \n\n");
    int zeroPlacementEpoch = 0;
    String initialState = null;
    String serviceName = PrimaryIntegrityTestHelper.SERVICE_NAME;
    Set<String> nodes =
        Set.of(
            PrimaryIntegrityTestHelper.NODE_1_ID,
            PrimaryIntegrityTestHelper.NODE_2_ID,
            PrimaryIntegrityTestHelper.NODE_3_ID);
    node1.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    node3.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    Thread.sleep(500);
    node2.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    System.out.printf(" replica-group: %s \n", node1.getReplicaGroup(serviceName));

    System.out.print("\n\n ===== Applications are initialized ===== \n\n\n\n");
    Thread.sleep(3000);

    System.out.print("\n\n ===== Step-3: Sending app requests to 3 nodes ... \n\n");
    for (int i = 0; i < 3; i++) {
      // preparing the request packet
      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);

      // handling the request packet
      node1.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handler) -> {
            System.out.printf("\n\nrequest is executed :) %s \n\n", executedRequest);
          });
      Thread.sleep(500);
    }
    for (int i = 0; i < 3; i++) {
      // preparing the request packet
      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);

      // handling the request packet
      node2.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handler) -> {
            System.out.printf("\n\nrequest is executed :) %s \n\n", executedRequest);
          });
      Thread.sleep(500);
    }
    for (int i = 0; i < 3; i++) {
      // preparing the request packet
      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);

      // handling the request packet
      node3.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handler) -> {
            System.out.printf("\n\nrequest is executed :) %s \n\n", executedRequest);
          });
      Thread.sleep(500);
    }

    Thread.sleep(3000);
    System.out.print("\n\n\n ========== Final result at the end of execution:\n");
    System.out.printf(
        "++ monotonic-sequence at node 1: %s\n", appAtNode1.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 2: %s\n", appAtNode2.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 3: %s\n", appAtNode2.test_GetSequenceAsString());

    appAtNode1.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode2.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode3.test_AssertMonotonicallyIncreasingNumbers();
    assert appAtNode1.test_GetSequenceAsString() != null
        : "The end state must not be null as we sent several app requests";
    assert !appAtNode1.test_GetSequenceAsString().isEmpty()
        : "The end state must not be empty as we sent several app requests";
    assert Objects.equals(
            appAtNode1.test_GetSequenceAsString(), appAtNode2.test_GetSequenceAsString())
        : "State in node1 != node2. At the end all replicas must have the same state";
    assert Objects.equals(
            appAtNode2.test_GetSequenceAsString(), appAtNode3.test_GetSequenceAsString())
        : "State in node2 != node3. At the end all replicas must have the same state";

    PrimaryIntegrityTestHelper.killServers(servers);
  }

  @Test
  public void PrimaryIntegrityTest_SendPrimaryChanges()
      throws InterruptedException, RequestParseException, IOException {

    String scenario =
        """

                Scenario for TestPrimaryChanges:
                  1. Create and initialize 3 active replicas: node1, node2, node3.
                  2. Create and initialize replica group using PrimaryBackupReplicaCoordinator with
                     MonotonicApp as the BackupableApplication (extended as the MonotonicTestApp).
                  3. node2 is the primary, by default with deterministic startup.
                  4. Send client requests to node2.
                  5. Make node3 as the new primary.
                  6. Continue send request to node2.
                  7. Assert that node3 indeed is the latest primary.
                  8. Assert that all the replicas have the same state at the end.

                """;
    System.out.println(scenario);

    PrimaryIntegrityTestHelper.cleanPreviousState();

    System.out.print("\n\n ===== Step-0: Preparing config ... \n\n");
    ReconfigurableNodeConfig<String> config =
        new DefaultNodeConfig<>(
            PrimaryIntegrityTestHelper.getDefaultActiveReplicas(),
            PrimaryIntegrityTestHelper.getDefaultReconfigurators());
    PrimaryIntegrityTestHelper.printServers(config);
    Thread.sleep(500);

    System.out.print("\n\n ===== Step-1: Initializing PrimaryBackup in 3 nodes ... \n\n");
    var servers = PrimaryIntegrityTestHelper.startThreeNodesWithMonotonicApp(config);
    var node1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).coordinator();
    var node2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).coordinator();
    var node3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).coordinator();
    var managerAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).manager();
    var managerAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).manager();
    var managerAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).manager();
    var appAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).app();
    var appAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).app();
    var appAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).app();
    Thread.sleep(3000);

    System.out.print("\n\n ===== Step-2: Initializing Applications in 3 nodes ... \n\n");
    int zeroPlacementEpoch = 0;
    String initialState = null;
    String serviceName = PrimaryIntegrityTestHelper.SERVICE_NAME;
    Set<String> nodes =
        Set.of(
            PrimaryIntegrityTestHelper.NODE_1_ID,
            PrimaryIntegrityTestHelper.NODE_2_ID,
            PrimaryIntegrityTestHelper.NODE_3_ID);
    node1.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    node3.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    Thread.sleep(500);
    node2.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    System.out.printf(">> replica-group: %s \n", node1.getReplicaGroup(serviceName));
    Thread.sleep(3000);

    System.out.print("\n\n ===== Step-3: Send app requests to current primary  ... \n\n");
    assert managerAtNode2.isCurrentPrimary(serviceName)
        : "With `ENABLE_STARTUP_LEADER_ELECTION=false`, node2 must deterministically "
            + "be the primary";
    for (int i = 0; i < 5; i++) {
      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);
      int finalI = i;
      node2.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handled) -> {
            assert executedRequest instanceof MonotonicAppRequest;
            assert handled;

            MonotonicAppRequest response = (MonotonicAppRequest) executedRequest;
            System.out.printf(
                "\n\nRequest-%d to %s is executed. response: %s \n\n",
                finalI, PrimaryIntegrityTestHelper.NODE_2_ID, response.getResponseValue());
          });

      Thread.sleep(10);
    }
    Thread.sleep(1000);

    System.out.print("\n\n   Changing primary to node 3   \n\n");
    ChangePrimaryPacket changePrimaryPacket =
        new ChangePrimaryPacket(serviceName, PrimaryIntegrityTestHelper.NODE_3_ID);
    node3.coordinateRequest(
        changePrimaryPacket,
        (executedPacket, handled) -> {
          assert executedPacket instanceof ChangePrimaryPacket
              : "executed packet is "
                  + executedPacket.getClass().getSimpleName()
                  + ", and not ChangePrimaryPacket";
          System.out.printf("\n\n %s is now the primary\n\n", PrimaryIntegrityTestHelper.NODE_3_ID);

          assert managerAtNode3.isCurrentPrimary(serviceName)
              : "After change primary, node3 must become the primary.";
        });

    System.out.println("Continue sending requests");
    for (int i = 0; i < 5; i++) {
      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);
      int finalI = i;
      node2.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handled) -> {
            assert executedRequest instanceof MonotonicAppRequest;
            assert handled;

            MonotonicAppRequest response = (MonotonicAppRequest) executedRequest;
            System.out.printf(
                "\n\nRequest-%d to %s is executed. response: %s \n\n",
                finalI, PrimaryIntegrityTestHelper.NODE_2_ID, response.getResponseValue());
          });

      Thread.sleep(10);
    }

    Thread.sleep(3000);

    assert managerAtNode3.isCurrentPrimary(serviceName)
        : "After change primary, node3 must become the primary.";

    System.out.print("\n\n\n ========== Final result at the end of execution:\n");
    System.out.printf(
        "++ monotonic-sequence at node 1: %s\n", appAtNode1.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 2: %s\n", appAtNode2.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 3: %s\n", appAtNode3.test_GetSequenceAsString());

    appAtNode1.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode2.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode3.test_AssertMonotonicallyIncreasingNumbers();
    assert appAtNode1.test_GetSequenceAsString() != null
        : "The end state must not be null as we sent several app requests";
    assert !appAtNode1.test_GetSequenceAsString().isEmpty()
        : "The end state must not be empty as we sent several app requests";
    assert appAtNode1.test_GetSequenceAsString().startsWith(appAtNode2.test_GetSequenceAsString())
        : "State in node2 is not a prefix of those in node1";
    assert appAtNode2.test_GetSequenceAsString().startsWith(appAtNode3.test_GetSequenceAsString())
        : "State in node3 is not a prefix of those in node2";

    Thread.sleep(1000);
    PrimaryIntegrityTestHelper.killServers(servers);
  }

  @Test
  public void PrimaryIntegrityTest_PrimaryChangesWithRequest()
      throws InterruptedException, RequestParseException, IOException {

    String scenario =
        """

                Scenario for TestPrimaryChangesWithRequest:
                  1. Create and initialize 3 active replicas: node1, node2, node3.
                  2. Create and initialize replica group using PrimaryBackupReplicaCoordinator with
                     MonotonicApp as the BackupableApplication (extended as the MonotonicTestApp).
                  3. node2 is the primary, by default with deterministic startup.
                  4. Send client requests to node2.
                  5. Make node1 as the new primary.
                  6. Send client requests to node1.
                  7. Assert that node1 indeed is the latest primary.
                  8. Assert that all the replicas have the same state at the end..

                """;
    System.out.println(scenario);

    PrimaryIntegrityTestHelper.cleanPreviousState();

    System.out.print("\n\n ===== Step-0: Preparing config ... \n\n");
    ReconfigurableNodeConfig<String> config =
        new DefaultNodeConfig<>(
            PrimaryIntegrityTestHelper.getDefaultActiveReplicas(),
            PrimaryIntegrityTestHelper.getDefaultReconfigurators());
    PrimaryIntegrityTestHelper.printServers(config);
    Thread.sleep(500);

    System.out.print("\n\n ===== Step-1: Initializing PrimaryBackup in 3 nodes ... \n\n");
    var servers = PrimaryIntegrityTestHelper.startThreeNodesWithMonotonicApp(config);
    var node1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).coordinator();
    var node2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).coordinator();
    var node3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).coordinator();
    var managerAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).manager();
    var managerAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).manager();
    var managerAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).manager();
    var appAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).app();
    var appAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).app();
    var appAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).app();
    Thread.sleep(3000);

    System.out.print("\n\n ===== Step-2: Initializing Applications in 3 nodes ... \n\n");
    int zeroPlacementEpoch = 0;
    String initialState = null;
    String serviceName = PrimaryIntegrityTestHelper.SERVICE_NAME;
    Set<String> nodes =
        Set.of(
            PrimaryIntegrityTestHelper.NODE_1_ID,
            PrimaryIntegrityTestHelper.NODE_2_ID,
            PrimaryIntegrityTestHelper.NODE_3_ID);
    node1.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    node3.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    Thread.sleep(500);
    node2.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    System.out.printf(">> replica-group: %s \n", node1.getReplicaGroup(serviceName));
    Thread.sleep(1000);

    System.out.print("\n\n ===== Step-3: Send app requests before primary changes  ... \n\n");
    assert managerAtNode2.isCurrentPrimary(serviceName)
        : "With `ENABLE_STARTUP_LEADER_ELECTION=false`, node2 must deterministically "
            + "be the primary";
    for (int i = 0; i < 5; i++) {
      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);
      int finalI = i;
      node2.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handled) -> {
            assert executedRequest instanceof MonotonicAppRequest;
            assert handled;

            MonotonicAppRequest response = (MonotonicAppRequest) executedRequest;
            System.out.printf(
                "\n\nRequest-%d to %s is executed. response: %s \n\n",
                finalI, PrimaryIntegrityTestHelper.NODE_2_ID, response.getResponseValue());
          });
    }
    Thread.sleep(1000);

    System.out.print("\n\n\n ========== Result before primary changes:\n");
    System.out.printf(
        "++ monotonic-sequence at node 1: %s\n", appAtNode1.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 2: %s\n", appAtNode2.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 3: %s\n", appAtNode3.test_GetSequenceAsString());

    System.out.print("\n\n   Changing primary to node 1   \n\n");
    ChangePrimaryPacket changePrimaryPacket =
        new ChangePrimaryPacket(serviceName, PrimaryIntegrityTestHelper.NODE_1_ID);
    node1.coordinateRequest(
        changePrimaryPacket,
        (executedPacket, handled) -> {
          assert executedPacket instanceof ChangePrimaryPacket
              : "executed packet is "
                  + executedPacket.getClass().getSimpleName()
                  + ", and not ChangePrimaryPacket";
          System.out.printf("\n\n %s is now the primary\n\n", PrimaryIntegrityTestHelper.NODE_1_ID);

          assert managerAtNode1.isCurrentPrimary(serviceName)
              : "After change primary, node3 must become the primary.";
        });

    Thread.sleep(500);

    for (int j = 0; j < 5; j++) {
      MonotonicAppRequest appRequestForNewPrimary =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);
      int finalJ = j;
      node1.coordinateRequest(
          ReplicableClientRequest.wrap(appRequestForNewPrimary),
          (executedRequest, isRequestHandled) -> {
            assert executedRequest instanceof MonotonicAppRequest;
            assert isRequestHandled;

            MonotonicAppRequest response = (MonotonicAppRequest) executedRequest;
            System.out.printf(
                """

                                        Request-%d to %s is executed. response: %s

                                        """,
                finalJ, PrimaryIntegrityTestHelper.NODE_1_ID, response.getResponseValue());
          });
      Thread.sleep(50);
    }
    Thread.sleep(2000);

    assert managerAtNode1.isCurrentPrimary(serviceName)
        : "After change primary, node1 must become the primary.";

    System.out.print("\n\n\n ========== Final result at the end of execution:\n");
    System.out.printf(
        "++ monotonic-sequence at node 1: %s\n", appAtNode1.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 2: %s\n", appAtNode2.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 3: %s\n", appAtNode3.test_GetSequenceAsString());

    appAtNode1.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode2.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode3.test_AssertMonotonicallyIncreasingNumbers();
    assert appAtNode1.test_GetSequenceAsString() != null
        : "The end state must not be null as we sent several app requests";
    assert !appAtNode1.test_GetSequenceAsString().isEmpty()
        : "The end state must not be empty as we sent several app requests";
    assert Objects.equals(
            appAtNode1.test_GetSequenceAsString(), appAtNode2.test_GetSequenceAsString())
        : "State in node1 != node2. At the end all replicas must have the same state";
    assert Objects.equals(
            appAtNode2.test_GetSequenceAsString(), appAtNode3.test_GetSequenceAsString())
        : "State in node2 != node3. At the end all replicas must have the same state";

    Thread.sleep(3000);
    PrimaryIntegrityTestHelper.killServers(servers);
  }

  @Test
  public void PrimaryIntegrityTest_SendRequestWhilePrimaryChanges()
      throws InterruptedException, RequestParseException, IOException {

    String scenario =
        """

                Scenario for TestSendRequestWhilePrimaryChanges:
                  1. Create and initialize 3 active replicas: node1, node2, node3.
                  2. Create and initialize replica group using PrimaryBackupReplicaCoordinator with
                     MonotonicApp as the BackupableApplication (extended in the MonotonicTestApp).
                  3. node2 will be the primary.
                  4. Send client requests to node2.
                  5. While still sending requests to node2, make node3 as the new primary.
                  6. Send client requests to node3 as well.
                     Doing step 4-6 will make node2, the old primary, to temporarily diverge from
                     the other nodes. Thus, node2 needs to restart to make its state consistent with
                     other nodes.
                  7. Assert that all the replicas have the same state at the end.

                Expectation:
                  - Old primary should restart and become backup of the new primary.
                  - Despite primary changes, and temporary state divergence in the old primary,
                    at the end, all the replicas must have the same consistent state.

                """;
    System.out.println(scenario);

    PrimaryIntegrityTestHelper.cleanPreviousState();

    System.out.print("\n\n ===== Step-0: Preparing config ... \n\n");
    ReconfigurableNodeConfig<String> config =
        new DefaultNodeConfig<>(
            PrimaryIntegrityTestHelper.getDefaultActiveReplicas(),
            PrimaryIntegrityTestHelper.getDefaultReconfigurators());
    PrimaryIntegrityTestHelper.printServers(config);
    Thread.sleep(500);

    System.out.print("\n\n ===== Step-1: Initializing PrimaryBackup in 3 nodes ... \n\n");
    var servers = PrimaryIntegrityTestHelper.startThreeNodesWithMonotonicApp(config);
    var node1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).coordinator();
    var node2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).coordinator();
    var node3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).coordinator();
    var managerAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).manager();
    var managerAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).manager();
    var managerAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).manager();
    var appAtNode1 = servers.get(PrimaryIntegrityTestHelper.NODE_1_ID).app();
    var appAtNode2 = servers.get(PrimaryIntegrityTestHelper.NODE_2_ID).app();
    var appAtNode3 = servers.get(PrimaryIntegrityTestHelper.NODE_3_ID).app();
    Thread.sleep(3000);

    System.out.print("\n\n ===== Step-2: Initializing Applications in 3 nodes ... \n\n");
    int zeroPlacementEpoch = 0;
    String initialState = null;
    String serviceName = PrimaryIntegrityTestHelper.SERVICE_NAME;
    Set<String> nodes =
        Set.of(
            PrimaryIntegrityTestHelper.NODE_1_ID,
            PrimaryIntegrityTestHelper.NODE_2_ID,
            PrimaryIntegrityTestHelper.NODE_3_ID);
    node1.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    node3.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    Thread.sleep(500);
    node2.createReplicaGroup(serviceName, zeroPlacementEpoch, initialState, nodes);
    System.out.printf(">> replica-group: %s \n", node1.getReplicaGroup(serviceName));
    Thread.sleep(3000);

    System.out.print("\n\n ===== Step-3: Send app requests while primary changes  ... \n\n");
    assert managerAtNode2.isCurrentPrimary(serviceName)
        : "With `ENABLE_STARTUP_LEADER_ELECTION=false`, node2 must deterministically "
            + "be the primary";
    int numRequestSentToOldPrimary = 10;
    int numRequestSentToNewPrimary = 10;
    for (int i = 0; i < numRequestSentToOldPrimary; i++) {

      MonotonicAppRequest appRequest =
          new MonotonicAppRequest(
              serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);
      int finalI = i;
      node2.coordinateRequest(
          ReplicableClientRequest.wrap(appRequest),
          (executedRequest, handled) -> {
            assert executedRequest instanceof MonotonicAppRequest;
            assert handled;

            MonotonicAppRequest response = (MonotonicAppRequest) executedRequest;
            System.out.printf(
                "\n\nRequest-%d to %s is executed. response: %s \n\n",
                finalI, PrimaryIntegrityTestHelper.NODE_2_ID, response.getResponseValue());
          });

      if (i == 5) {
        System.out.print("\n\n   Changing primary to node 3   \n\n");
        ChangePrimaryPacket changePrimaryPacket =
            new ChangePrimaryPacket(serviceName, PrimaryIntegrityTestHelper.NODE_3_ID);
        node3.coordinateRequest(
            changePrimaryPacket,
            (executedPacket, handled) -> {
              assert executedPacket instanceof ChangePrimaryPacket
                  : "executed packet is "
                      + executedPacket.getClass().getSimpleName()
                      + ", and not ChangePrimaryPacket";
              System.out.printf(
                  "\n\n %s is now the primary\n\n", PrimaryIntegrityTestHelper.NODE_3_ID);

              assert managerAtNode3.isCurrentPrimary(serviceName)
                  : "After change primary, node3 must become the primary.";

              // sending request to the new primary
              for (int j = 0; j < numRequestSentToNewPrimary; j++) {
                MonotonicAppRequest appRequestForNewPrimary =
                    new MonotonicAppRequest(
                        serviceName, MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND);
                int finalJ = j;
                try {
                  node3.coordinateRequest(
                      ReplicableClientRequest.wrap(appRequestForNewPrimary),
                      (executedRequest, isRequestHandled) -> {
                        assert executedRequest instanceof MonotonicAppRequest;
                        assert isRequestHandled;

                        MonotonicAppRequest response = (MonotonicAppRequest) executedRequest;
                        System.out.printf(
                            """

                                                        Request-%d to %s is executed. response: %s

                                                        """,
                            finalJ,
                            PrimaryIntegrityTestHelper.NODE_3_ID,
                            response.getResponseValue());
                      });
                  Thread.sleep(20);
                } catch (IOException | RequestParseException | InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });
      }
      Thread.sleep(5);
    }

    // We need to wait sufficiently, allowing time for the non-primary nodes to
    // restart and roll forward from the latest checkpoint.
    Thread.sleep(5000);

    assert managerAtNode3.isCurrentPrimary(serviceName)
        : "After change primary, node3 must become the primary.";

    System.out.print("\n\n\n ========== Final result at the end of execution:\n");
    System.out.printf(
        "++ monotonic-sequence at node 1: %s\n", appAtNode1.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 2: %s\n", appAtNode2.test_GetSequenceAsString());
    System.out.printf(
        "++ monotonic-sequence at node 3: %s\n", appAtNode3.test_GetSequenceAsString());

    appAtNode1.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode2.test_AssertMonotonicallyIncreasingNumbers();
    appAtNode3.test_AssertMonotonicallyIncreasingNumbers();
    assert appAtNode1.test_GetSequenceAsString() != null
        : "The end state must not be null as we sent several app requests";
    assert !appAtNode1.test_GetSequenceAsString().isEmpty()
        : "The end state must not be empty as we sent several app requests";
    assert Objects.equals(
            appAtNode1.test_GetSequenceAsString(), appAtNode2.test_GetSequenceAsString())
        : "State in node1 != node2. At the end all replicas must have the same state";
    assert Objects.equals(
            appAtNode2.test_GetSequenceAsString(), appAtNode3.test_GetSequenceAsString())
        : "State in node2 != node3. At the end all replicas must have the same state";

    int totalRequest = numRequestSentToOldPrimary + numRequestSentToNewPrimary;
    int totalExecution = appAtNode1.test_GetSequenceSize();
    int missingExecution = totalExecution - totalRequest;
    assert totalRequest == totalExecution
        : String.format("%d out of %d requests are missing", missingExecution, totalRequest);
    Thread.sleep(3000);
    PrimaryIntegrityTestHelper.killServers(servers);
  }
}
