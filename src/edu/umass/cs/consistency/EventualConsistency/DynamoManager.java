package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.consistency.EventualConsistency.Domain.CheckpointLog;
import edu.umass.cs.consistency.EventualConsistency.Domain.DAG;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
import edu.umass.cs.consistency.EventualConsistency.Domain.RequestInformation;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.Repliconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.*;

import static edu.umass.cs.consistency.EventualConsistency.Domain.DAG.addChildNode;
import static edu.umass.cs.consistency.EventualConsistency.Domain.DAG.createDominantChildGraphNode;

public class DynamoManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Repliconfigurable myApp;
    private final HashMap<String, ReplicatedDynamoStateMachine> replicatedQuorums;
    private final int REQUESTS_COUNT_BEFORE_CHECKPOINT;
    private final int CHECKPOINT_VERSION_THRESHOLD;
    // Changed only when state transfer is to be done
    public static AtomicBoolean pseudo_failure = new AtomicBoolean(false);
    public static AtomicBoolean state_transfer = new AtomicBoolean(false);
    private int countRequestsReceived;
    // maps the quorumID to DAG associated
    private HashMap<String, DAG> requestDAG = new HashMap<>();
    private HashMap<String, HashMap<Integer, Integer>> checkpointVC = new HashMap<>();
    private int noOfCheckpoints = -1;
    private final Stringifiable<NodeIDType> unstringer;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    //    Maps the reqestID to QuorumRequestAndCallback object
    private HashMap<Long, DynamoRequestAndCallback> requestsReceived = new HashMap<Long, DynamoRequestAndCallback>();
    private HashMap<Long, RecoveryCheck> recoveryCheckHashMap = new HashMap<>();
    private ArrayList<String> quorums = new ArrayList<String>();
    public static final Logger log = Logger.getLogger(DynamoManager.class.getName());

    public static final Class<?> application = DynamoApp.class;
    private ScheduledExecutorService pruningScheduler;


    public static final String getDefaultServiceName() {
        return application.getSimpleName() + "0";
    }

    public DynamoManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
                         InterfaceNIOTransport<NodeIDType, JSONObject> niot, Repliconfigurable instance,
                         String logFolder, boolean enableNullCheckpoints) {
        this.myID = this.integerMap.put(id);
        this.unstringer = unstringer;
        this.myApp = (Repliconfigurable) instance;
        this.replicatedQuorums = new HashMap<>();
        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));
        FileHandler fileHandler = null;
        try {
            fileHandler = new FileHandler("output/dynamoManager" + myID + ".log", true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Properties properties = PaxosConfig.getAsProperties();
        REQUESTS_COUNT_BEFORE_CHECKPOINT = Integer.parseInt(properties.getProperty("DAG.REQUESTS_COUNT_BEFORE_CHECKPOINT"));
        CHECKPOINT_VERSION_THRESHOLD = Integer.parseInt(properties.getProperty("DAG.CHECKPOINT_VERSION_THRESHOLD"));
        fileHandler.setFormatter(new SimpleFormatter());
        log.addHandler(fileHandler);
        log.setLevel(Level.FINE);
        initializePruningScheduler();
        countRequestsReceived = 0;
    }

    private void initializePruningScheduler() {
        pruningScheduler = Executors.newScheduledThreadPool(1);
        Runnable prune = new Runnable() {
            public synchronized void run() {
                for (String quorumID : replicatedQuorums.keySet()) {
                    log.log(Level.INFO, "Pruning for quorumID {0} checkpointVC {1} MVC {2}", new Object[]{quorumID, checkpointVC.get(quorumID), replicatedQuorums.get(quorumID).getMinorVectorClock()});
                    if (GraphNode.isDominant(checkpointVC.get(quorumID), replicatedQuorums.get(quorumID).getMinorVectorClock())) {
                        log.info("Before pruning " + requestDAG.get(quorumID).getAllVC());
                        log.info("Pruning with VC: " + replicatedQuorums.get(quorumID).getMinorVectorClock());
                        requestDAG.get(quorumID).pruneRequests(replicatedQuorums.get(quorumID).getMinorVectorClock());
                        log.info("After pruning " + requestDAG.get(quorumID).getAllVC());
                    } else {
                        log.info("Cannot prune as checkpoint vector clock is minor to MVC");
                    }
                }
            }
        };
        pruningScheduler.scheduleAtFixedRate(prune, 100, 200, TimeUnit.MINUTES);
    }

    private int distance(HashMap<Integer, Integer> vectorClock, String quorumId) {
        int distance = 0;
        for (int key : vectorClock.keySet()) {
            distance += (int) Math.pow(vectorClock.get(key) - checkpointVC.get(quorumId).get(key), 2);
        }
        return (int) Math.sqrt(distance);
    }

    private HashMap<Integer, Integer> toCheckpoint(String quorumId, ArrayList<GraphNode> latestNodes, DynamoRequestPacket.DynamoPacketType type) {
        HashMap<Integer, Integer> minLatestNode = new HashMap<>();
        if (countRequestsReceived - this.noOfCheckpoints * REQUESTS_COUNT_BEFORE_CHECKPOINT >= REQUESTS_COUNT_BEFORE_CHECKPOINT || type == DynamoRequestPacket.DynamoPacketType.STOP) {
            minLatestNode = requestDAG.get(quorumId).getMinimumLatestNode(replicatedQuorums.get(quorumId).getInitialVectorClock(),
                    latestNodes);
            return minLatestNode;
        }
        return null;
    }

    private void checkpoint(String quorumId, DynamoRequestPacket.DynamoPacketType type) throws JSONException {
        countRequestsReceived++;
        ArrayList<GraphNode> latestNodes = requestDAG.get(quorumId).getLatestNodes();
        if (latestNodes != null) {
            HashMap<Integer, Integer> nextCheckpoint = toCheckpoint(quorumId, latestNodes, type);
            DAGLogger dagLogger = this.replicatedQuorums.get(quorumId).getDagLogger();
            if (nextCheckpoint != null) {
                noOfCheckpoints += Math.floorDiv(countRequestsReceived, REQUESTS_COUNT_BEFORE_CHECKPOINT) - this.noOfCheckpoints;
                if (latestNodes.size() == 1) {
                    dagLogger.checkpoint(this.myApp.checkpoint(this.myApp.toString()), noOfCheckpoints, nextCheckpoint, quorumId, new ArrayList<>(), myID);
                } else {
                    dagLogger.checkpoint(this.myApp.checkpoint(this.myApp.toString()), noOfCheckpoints, nextCheckpoint, quorumId, latestNodes, myID);
                }
                checkpointVC.put(quorumId, new HashMap<>());
                for (int key : nextCheckpoint.keySet()) {
                    checkpointVC.get(quorumId).put(key, nextCheckpoint.get(key));
                }
                log.log(Level.INFO, "Checkpoint Vector Clock changed to {0}", new Object[]{nextCheckpoint});
            }
        }
        log.log(Level.INFO, "Checkpoint Vector Clock currently is {0} for quorumId {1}", new Object[]{checkpointVC.get(quorumId), quorumId});
    }

    public static class RecoveryCheck {
        private int noOfAcknowledgement;
        private final String quorumId;
        private int checkpointVersion;
        private int serverId;
        private final int RECOVERY_THRESHOLD_SERVERS = 1;

        RecoveryCheck(String quorumId, int checkpointVersion, int serverId) {
            this.quorumId = quorumId;
            this.checkpointVersion = checkpointVersion;
            this.serverId = serverId;
            this.noOfAcknowledgement = 0;
        }

        public boolean ackReceived(int checkpointVersion, int serverId) {
            this.noOfAcknowledgement++;
            if (checkpointVersion > this.checkpointVersion) {
                this.checkpointVersion = checkpointVersion;
                this.serverId = serverId;
            }
            return this.noOfAcknowledgement > RECOVERY_THRESHOLD_SERVERS;
        }

        public int getNoOfAcknowledgement() {
            return noOfAcknowledgement;
        }

        public void setNoOfAcknowledgement(int noOfAcknowledgement) {
            this.noOfAcknowledgement = noOfAcknowledgement;
        }

        public String getQuorumId() {
            return quorumId;
        }

        public int getCheckpointVersion() {
            return checkpointVersion;
        }

        public void setCheckpointVersion(int checkpointVersion) {
            this.checkpointVersion = checkpointVersion;
        }

        public int getServerId() {
            return serverId;
        }
    }

    public static class DynamoRequestAndCallback {
        protected DynamoRequestPacket dynamoRequestPacket;
        final ExecutedCallback callback;
        protected Integer numOfAcksReceived;
        private ArrayList<GraphNode> responseGraphNodes;
        private HashMap<Long, String> responseRequests;

        DynamoRequestAndCallback(DynamoRequestPacket dynamoRequestPacket, ExecutedCallback callback) {
            this.dynamoRequestPacket = dynamoRequestPacket;
            this.callback = callback;
            this.responseGraphNodes = new ArrayList<>();
            this.responseRequests = new HashMap<>();
            this.numOfAcksReceived = 0;
        }

        @Override
        public String toString() {
            return this.dynamoRequestPacket + " [" + callback + "]";
        }

        public void reset() {
            this.numOfAcksReceived = 0;
        }

        public void setResponse(HashMap<Integer, Integer> vectorClock, String value) {
            dynamoRequestPacket.setResponsePacket(new DynamoRequestPacket.DynamoPacket(vectorClock, value));
        }

        public Integer incrementAck(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm) {
            this.numOfAcksReceived += 1;
            if (qp.getType() == DynamoRequestPacket.DynamoPacketType.GET_ACK) {
                this.addToArrayListForReconcile(qp);
            }
            return this.numOfAcksReceived;
        }

        public void addToArrayListForReconcile(DynamoRequestPacket responsePacket) {
            responseGraphNodes.add(new GraphNode(responsePacket.getResponsePacket().getVectorClock()));
            responseRequests.putAll(responsePacket.getResponsePacket().getAllRequests());
        }

        public ArrayList<GraphNode> getResponseGraphNodes() {
            return responseGraphNodes;
        }

        public void setResponseGraphNodes(ArrayList<GraphNode> responseGraphNodes) {
            this.responseGraphNodes = responseGraphNodes;
        }

        public HashMap<Long, String> getResponseRequests() {
            return responseRequests;
        }

        public void setResponseRequests(HashMap<Long, String> responseRequests) {
            this.responseRequests = responseRequests;
        }

        public Integer getNumOfAcksReceived() {
            return this.numOfAcksReceived;
        }

        public DynamoRequestPacket getResponsePacket() {
            this.dynamoRequestPacket.setPacketType(DynamoRequestPacket.DynamoPacketType.RESPONSE);
            return this.dynamoRequestPacket;
        }
    }

    private void performRecovery(ReplicatedDynamoStateMachine rqsm) {
        DynamoRequestPacket qp = new DynamoRequestPacket(DynamoRequestPacket.DynamoPacketType.RECOVERY, rqsm.getQuorumID());
        recoveryCheckHashMap.put(qp.getRequestID(), new RecoveryCheck(rqsm.getQuorumID(), noOfCheckpoints, myID));
        for (int i = 0; i < rqsm.getQuorumMembers().size(); i++) {
            if (rqsm.getQuorumMembers().get(i) != this.myID) {
                qp.setSource(this.myID);
                qp.setDestination(rqsm.getQuorumMembers().get(i));
                this.sendRequest(qp, qp.getDestination());
            }
        }
    }

    private void handleDynamoPacket(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm, ExecutedCallback callback) {

        DynamoRequestPacket.DynamoPacketType packetType = qp.getType();
        log.log(Level.INFO, qp.toString());
        if (!DynamoRequestPacket.DynamoPacketType.getHelpers().contains(packetType)) {
            try {
                log.log(Level.INFO, "Checking for recovery by " + myID + " with version " + noOfCheckpoints + " due to " + qp.getSource() + " with version " + qp.getCheckpointVersion());
                if (qp.getCheckpointVersion() - noOfCheckpoints >= CHECKPOINT_VERSION_THRESHOLD) {
//                    pseudo_failure.set(true);
                    log.log(Level.INFO, "Sending out for recovery by " + myID + " with version " + noOfCheckpoints + " due to " + qp.getSource() + " with version " + qp.getCheckpointVersion());
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            performRecovery(rqsm);
                        }
                    }).start();
                } else {
                    checkpoint(rqsm.getQuorumID(), qp.getType());
                }
            } catch (JSONException e) {
                log.log(Level.SEVERE, "Could not checkpoint");
                throw new RuntimeException(e);
            }
        } else {
            log.log(Level.INFO, "Ignoring: " + qp);
        }

        if (pseudo_failure.get()) {
            log.log(Level.SEVERE, "State transfer in progress, ignoring request");
            return;
        }

        switch (packetType) {
            case PUT:
                // client -> server
                handlePutRequest(qp, rqsm, callback);
                break;
            case PUT_FWD:
                // server -> read_quorum_server
                handlePutForward(qp, rqsm);
                break;
            case GET:
                // read_quorum_server -> server
                handleGetRequest(qp, rqsm, callback);
                break;
            case GET_FWD:
                // server -> write_quorum_server
                handleGetForward(qp, rqsm);
                break;
            case PUT_ACK:
                // write_quorum_server -> node
                handlePutAck(qp, rqsm);
                break;
            case GET_ACK:
                // read_quorum_server -> node
                handleGetAck(qp, rqsm);
                break;
            case RECOVERY:
                handleRecovery(qp);
                break;
            case RECOVERY_ACK:
                handleRecoveryAck(qp);
                break;
            case STATE_TRANSMIT_INIT:
                handleStateTransmitInit(qp, rqsm);
                break;
            case STATE_TRANSMIT:
                pseudo_failure.set(true);
                state_transfer.set(true);
                log.log(Level.SEVERE, "Pseudo failure initiated");
                handleStateTransmit(qp, rqsm);
                break;
            case STOP:
                handleStop(rqsm);
                break;
            default:
                log.log(Level.INFO, "Request of type {0} unattended", new Object[]{packetType});
                break;
        }

    }

    private class CheckPseudoFailure implements Runnable {
        private final String quorumId;
        private final ScheduledExecutorService executor;

        // Constructor
        CheckPseudoFailure(String quorumId) {
            this.quorumId = quorumId;
            this.executor = Executors.newScheduledThreadPool(1); // Initialize ExecutorService
        }

        // Run method to check pseudo_failure and restore state if necessary
        @Override
        public void run() {
            while (true) {
                if (!state_transfer.get() || !pseudo_failure.get()) {
                    DynamoManager.log.log(Level.INFO, "state transfer procedure is completed, stopping the check.");
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (!pseudo_failure.get()) {
                state_transfer.set(false);
                log.log(Level.SEVERE, "Pseudo failure cancelled");
                this.stop();
                return;
            }
            try {
                // Reset the state in RAM from the one stored on disk
                DynamoManager.log.log(Level.INFO, "Reset initiated from received state");
                CheckpointLog receivedCheckpointLog = DynamoManager.this.replicatedQuorums.get(quorumId).getDagLogger().restore();
                DynamoManager.this.requestDAG.remove(quorumId);

                DynamoManager.this.requestDAG.put(quorumId, new DAG(receivedCheckpointLog.getVectorClock()));
                if (!receivedCheckpointLog.getLatestNodes().isEmpty()) {
                    DynamoManager.this.requestDAG.get(quorumId).addChildrenNodes(
                            receivedCheckpointLog.getLatestNodes(),
                            DynamoManager.this.requestDAG.get(quorumId).getRootNode()
                    );
                }
                ArrayList<GraphNode> latestNodes = new ArrayList<>();
                for (HashMap<Integer, Integer> vectorClock : receivedCheckpointLog.getLatestNodes()) {
                    latestNodes.add(new GraphNode(vectorClock));
                }
                DynamoManager.this.myApp.restore(myApp.toString(), receivedCheckpointLog.getState());
                DynamoManager.this.noOfCheckpoints = receivedCheckpointLog.getNoOfCheckpoints();
                pseudo_failure.set(false);
                log.log(Level.INFO, "State reinstantiation completed");
                log.log(Level.INFO, "Pseudo failure stopped");
                this.stop();
            } catch (JSONException e) {
                pseudo_failure.set(false);
                log.log(Level.SEVERE, "Pseudo failure cancelled");
                Thread.currentThread().interrupt();
            }
        }

        public void start() {
            executor.submit(this);
        }

        // Method to stop the task gracefully
        public void stop() {
            executor.shutdownNow();  // Stop the executor and interrupt the task
            try {
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private synchronized void handleStop(ReplicatedDynamoStateMachine rqsm) {
        log.log(Level.INFO, "Received stop packet");
        DynamoRequestPacket dynamoRequestPacket = (DynamoRequestPacket) this.myApp.getStopRequest(null, 0);
        GraphNode requestGraphNode = createDominantChildGraphNode(this.requestDAG.get(rqsm.getQuorumID()).getLatestNodes(), rqsm.getMaxVectorClock());
        requestGraphNode.addRequest(dynamoRequestPacket);
    }

    private void handleRecovery(DynamoRequestPacket qp) {
        log.log(Level.INFO, "Received recovery packet from: " + qp.getSource());
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.RECOVERY_ACK);
        qp.setDestination(qp.getSource());
        qp.setSource(this.myID);
        this.sendRequest(qp, qp.getDestination());
    }

    private synchronized void handleRecoveryAck(DynamoRequestPacket qp) {
        if (recoveryCheckHashMap.containsKey(qp.getRequestID()) && recoveryCheckHashMap.get(qp.getRequestID()).ackReceived(qp.getCheckpointVersion(), qp.getSource())) {
            if (!(recoveryCheckHashMap.get(qp.getRequestID()).getServerId() == myID)) {
                log.log(Level.INFO, "Sending state transfer init to server: " + recoveryCheckHashMap.get(qp.getRequestID()).getServerId());
                qp.setPacketType(DynamoRequestPacket.DynamoPacketType.STATE_TRANSMIT_INIT);
                qp.setDestination(recoveryCheckHashMap.get(qp.getRequestID()).getServerId());
                qp.setSource(this.myID);
                this.sendRequest(qp, qp.getDestination());
            } else {
                log.log(Level.INFO, "Own checkpoint version is greatest");
            }
            recoveryCheckHashMap.remove(qp.getRequestID());
        } else {
            log.log(Level.INFO, "Majority Acknowledgements already received");
        }
    }

    private void handleStateTransmitInit(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm) {
        try {
            if (this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger().restore() != null) {
                int port = this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger().checkpointTransferRequest();
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("Address", this.messenger.getNodeConfig().getNodeAddress(this.integerMap.get(myID)).getHostAddress());
                jsonObject.put("Port", port);
                jsonObject.put("FileName", this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger().getCheckpointLogFilePath());
                qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(jsonObject.toString()));
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "State init not successful");
        }
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.STATE_TRANSMIT);
        qp.setDestination(qp.getSource());
        qp.setSource(this.myID);
        this.sendRequest(qp, qp.getDestination());
    }

    private void handleStateTransmit(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm) {
        try {
            JSONObject jsonObject = new JSONObject(qp.getResponsePacket().getValue());
            this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger().getTransferredCheckpoint(jsonObject.getString("Address"),
                    jsonObject.getInt("Port"), jsonObject.getString("FileName"));
        } catch (JSONException | IOException e) {
            log.log(Level.SEVERE, "State transmit packet could not be unpacked");
            pseudo_failure.set(false);
            log.log(Level.SEVERE, "Pseudo failure cancelled");
        }
        CheckPseudoFailure checkPseudoFailure = new CheckPseudoFailure(rqsm.getQuorumID());
        checkPseudoFailure.start();

    }

    private synchronized void handlePutRequest(DynamoRequestPacket qp,
                                               ReplicatedDynamoStateMachine rqsm,
                                               ExecutedCallback callback) {
        log.info("PUT request for : " + qp.getRequestValue() + " vc " + qp.getRequestVectorClock() + " id " + myID);
        log.info("Before PUT " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());

        // Record the received PUT request
        this.requestsReceived.putIfAbsent(
                qp.getRequestID(),
                new DynamoRequestAndCallback(qp, callback));

        // Get the client PUT request.
        // FIXME: this is inefficient.
        Request request = getInterfaceRequest(this.myApp, qp.toString());

        // Execute the Put request
        this.myApp.execute(request, false);

        // Create a new empty graph node
        GraphNode requestGraphNode = createDominantChildGraphNode(
                this.requestDAG.get(rqsm.getQuorumID()).getLatestNodes(),   // all graph leaf nodes
                rqsm.getInitialVectorClock());                              // zero vector clock (if no checkpoint)

        if (requestGraphNode != null) {
            // Increase this replica's component in the vector clock
            requestGraphNode.getVectorClock()
                    .put(myID, requestGraphNode.getVectorClock().get(myID) + 1);

            // Get all requests for our peers
            HashMap<Integer, HashMap<Long, String>> allRequests =
                    this.requestDAG.get(rqsm.getQuorumID())
                            .getRequestsPerMember(
                                    requestGraphNode.getVectorClock(),
                                    replicatedQuorums
                                            .get(rqsm.getQuorumID())
                                            .getMemberVectorClocks());

            // FIXME: why there is the case when the timestamp can go backward?
            rqsm.updateMemberVectorClock(myID, requestGraphNode.getVectorClock());

            requestGraphNode.addRequest(qp);

            this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger()
                    .rollForward(requestGraphNode);

            // Prepare PUT_FWD packet and send it to other replicas
            qp.setRequestVectorClock(requestGraphNode.getVectorClock());
            qp.setPacketType(DynamoRequestPacket.DynamoPacketType.PUT_FWD);
            log.info("After PUT " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
            for (int i = 0; i < rqsm.getQuorumMembers().size(); i++) {
                if (rqsm.getQuorumMembers().get(i) != this.myID) {
                    qp.setSource(this.myID);
                    qp.setDestination(rqsm.getQuorumMembers().get(i));
                    qp.setAllRequests(allRequests.get(rqsm.getQuorumMembers().get(i)));
                    this.sendRequest(qp, qp.getDestination());
                }
            }
        } else {
            log.log(Level.SEVERE, "Ignoring PUT request: " + qp);
        }
    }

    public synchronized void handlePutForward(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm) {
        log.info("PUT_FWD request for : " + qp.getRequestValue() + " vc " + qp.getRequestVectorClock() + " id " + myID);
        log.info("Bef PUT_FWD " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());

        // Create a graph node with the received timestamp
        GraphNode graphNode = new GraphNode(qp.getRequestVectorClock());

        // Get nodes that are not dominant to the received timestamp
        // FIXME: it is possible having a graph node to be the child of multiple
        //  nodes in a sequence.
        ArrayList<GraphNode> latestNodes =
                this.requestDAG.get(rqsm.getQuorumID())
                        .latestNodesWithVectorClockAsDominant(graphNode, true);

        rqsm.updateMemberVectorClock(qp.getSource(), qp.getRequestVectorClock());
        log.log(Level.INFO, "Latest Nodes returned: {0}", new Object[]{latestNodes});
        if (latestNodes != null) {
            HashMap<Long, String> executedRequests =
                    this.requestDAG.get(rqsm.getQuorumID())
                            .getAllExecutedRequests(graphNode.getVectorClock(), null);
            log.info(myID + " executed: " + executedRequests + " received " + qp.getAllRequests());

            // get the set difference
            qp.getAllRequests().keySet().removeAll(executedRequests.keySet());

            // FIXME: this execution of requests inside the set difference doesn't
            //  consider causal ordering constraint, isn't it?
            graphNode.setRequests(executeRequests(qp.getAllRequests(), qp));

            graphNode.addRequests(qp);
            log.info("After PUT_FWD " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
            Request request = getInterfaceRequest(this.myApp, qp.toString());
            this.myApp.execute(request, false);
            addChildNode(latestNodes, graphNode);
            this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger().rollForward(graphNode);
            rqsm.updateMemberVectorClock(myID, qp.getRequestVectorClock());
            qp.setPacketType(DynamoRequestPacket.DynamoPacketType.PUT_ACK);
            int dest = qp.getDestination();
            qp.setDestination(qp.getSource());
            qp.setSource(dest);
            qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(qp.getRequestVectorClock(), "-1"));
            this.sendRequest(qp, qp.getDestination());
        } else {
            log.log(Level.SEVERE, "Ignoring PUT_FWD request: " + qp.toString());
        }
    }

    public void handlePutAck(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm) {
        log.log(Level.INFO, "PUT_ACK for {0} received. Sent by : {1}", new Object[]{qp.getRequestID(), qp.getSource()});
        if (this.requestsReceived.containsKey(qp.getRequestID()) && this.requestsReceived.get(qp.getRequestID()).incrementAck(qp, rqsm) == rqsm.getWriteQuorum() - 1) {
            this.requestsReceived.get(qp.getRequestID()).setResponse(qp.getRequestVectorClock(), qp.getResponsePacket().getValue());
            sendResponse(qp.getRequestID());
        }
    }

    private void handleGetRequest(DynamoRequestPacket qp,
                                  ReplicatedDynamoStateMachine rqsm, ExecutedCallback callback) {
        log.info("GET request for " + qp.getRequestValue() + " received by " + myID);
        log.info("GET" + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new DynamoRequestAndCallback(qp, callback));
        qp.setRequestVectorClock(qp.getRequestVectorClock());
        qp.setPacketType(DynamoRequestPacket.DynamoPacketType.GET_FWD);
        for (int i = 0; i < rqsm.getQuorumMembers().size(); i++) {
            if (rqsm.getQuorumMembers().get(i) != this.myID) {
                qp.setSource(this.myID);
                qp.setDestination(rqsm.getQuorumMembers().get(i));
                this.sendRequest(qp, qp.getDestination());
            }
        }
    }

    public synchronized void handleGetForward(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm) {
        log.info("GET_FWD request for : " + qp.getRequestValue() + " vc " + qp.getRequestVectorClock() + " id " + myID);
        log.info("Before GET_FWD " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);

        GraphNode requestGraphNode =
                createDominantChildGraphNode(
                        this.requestDAG.get(rqsm.getQuorumID()).getLatestNodes(),
                        rqsm.getInitialVectorClock());

        if (requestGraphNode != null) {
            requestGraphNode.getVectorClock().put(myID, requestGraphNode.getVectorClock().get(myID) + 1);
            rqsm.updateMemberVectorClock(myID, requestGraphNode.getVectorClock());
            HashMap<Long, String> allRequests = this.requestDAG.get(rqsm.getQuorumID()).getAllExecutedRequests(requestGraphNode.getVectorClock(), replicatedQuorums.get(rqsm.getQuorumID()).getMemberVectorClocks().get(qp.getSource()));
            requestGraphNode.addRequest(qp);
            assert request != null;
            log.info("After GET_FWD " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
            this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger().rollForward(requestGraphNode);
            qp.setResponsePacket(new DynamoRequestPacket.DynamoPacket(requestGraphNode.getVectorClock(), ((DynamoRequestPacket) request).getResponsePacket().getValue()));
            qp.getResponsePacket().setAllRequests(allRequests);
            qp.setPacketType(DynamoRequestPacket.DynamoPacketType.GET_ACK);
            int dest = qp.getDestination();
            qp.setDestination(qp.getSource());
            qp.setSource(dest);
            this.sendRequest(qp, qp.getDestination());
        } else {
            log.log(Level.SEVERE, "Ignoring GET_FWD request: " + qp.toString());
        }
    }

    public synchronized void handleGetAck(DynamoRequestPacket qp, ReplicatedDynamoStateMachine rqsm) {
        rqsm.updateMemberVectorClock(qp.getSource(), qp.getResponsePacket().getVectorClock());
        if (this.requestsReceived.containsKey(qp.getRequestID()) && this.requestsReceived.get(qp.getRequestID()).incrementAck(qp, rqsm) == rqsm.getReadQuorum() - 1) {
            log.info("Before GET_ACK from " + qp.getSource() + " VC " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
            ArrayList<GraphNode> responseGraphNodes = new ArrayList<>(this.requestsReceived.get(qp.getRequestID()).getResponseGraphNodes());
            GraphNode requestGraphNode = createDominantChildGraphNode(this.requestDAG.get(rqsm.getQuorumID()).getLatestNodes(), rqsm.getInitialVectorClock());
            if (requestGraphNode != null) {
                log.info("After GET_ACK createDominant from " + qp.getSource() + " VC " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
                requestGraphNode.getVectorClock().put(myID, requestGraphNode.getVectorClock().get(myID) + 1);
                responseGraphNodes.add(requestGraphNode);
                requestGraphNode.setVectorClock(DAG.getDominantVC(responseGraphNodes).getVectorClock());
                rqsm.updateMemberVectorClock(myID, requestGraphNode.getVectorClock());
                HashMap<Long, String> executedRequests = this.requestDAG.get(rqsm.getQuorumID()).getAllExecutedRequests(requestGraphNode.getVectorClock(), null);
                HashMap<Long, String> obtainedRequests = this.requestsReceived.get(qp.getRequestID()).getResponseRequests();
                obtainedRequests.keySet().removeAll(executedRequests.keySet());
                requestGraphNode.setRequests(executeRequests(obtainedRequests, qp));
                requestGraphNode.addRequest(qp);
                log.info("After GET_ACK from " + qp.getSource() + " VC " + this.requestDAG.get(rqsm.getQuorumID()).getAllVC());
                Request request = getInterfaceRequest(this.myApp, qp.toString());
                this.myApp.execute(request, false);
                this.replicatedQuorums.get(rqsm.getQuorumID()).getDagLogger().rollForward(requestGraphNode);
                this.requestsReceived.get(qp.getRequestID()).setResponse(requestGraphNode.getVectorClock(), qp.getResponsePacket().getValue());
                sendResponse(qp.getRequestID());
            } else {
                log.log(Level.SEVERE, "Ignoring GET_ACK request: " + qp.toString());
            }
        }
    }

    private ArrayList<RequestInformation> executeRequests(HashMap<Long, String> hashMap,
                                                          DynamoRequestPacket dynamoRequestPacket) {

        ArrayList<RequestInformation> requestInformationArrayList = new ArrayList<>();
        DynamoRequestPacket dummyRequestPacket =
                new DynamoRequestPacket(
                        dynamoRequestPacket.getRequestID(),
                        DynamoRequestPacket.DynamoPacketType.PUT,
                        dynamoRequestPacket);

        for (Long key : hashMap.keySet()) {
            String[] strings = hashMap.get(key).split(" ");
            if (strings[0].equals("PUT")) {
                dummyRequestPacket.setRequestValue(strings[1]);
                Request request = getInterfaceRequest(this.myApp, dummyRequestPacket.toString());
                this.myApp.execute(request, false);
            }
            requestInformationArrayList.add(new RequestInformation(key, hashMap.get(key)));
        }
        return requestInformationArrayList;
    }

    public void sendResponse(Long requestID) {
        DynamoRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
        if (requestAndCallback != null && requestAndCallback.callback != null) {

            requestAndCallback.callback.executed(requestAndCallback.getResponsePacket()
                    , true);
            this.requestsReceived.remove(requestID);

        } else {
            log.log(Level.WARNING, "QuorumManager.handleResponse received " +
                            "an ACK request {0} that does not match any enqueued request.",
                    new Object[]{requestID});
        }
    }

    private void sendRequest(DynamoRequestPacket qp,
                             int nodeID) {
        qp.setCheckpointVersion(noOfCheckpoints);
        log.log(Level.INFO, qp.toString());
        GenericMessagingTask<NodeIDType, ?> gTask = null;
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask<>(this.integerMap.get(nodeID),
                    qp);
            this.messenger.send(gTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Request getInterfaceRequest(Replicable app, String value) {
        try {
            return app.getRequest(value);
        } catch (RequestParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String propose(String quorumID, Request request,
                          ExecutedCallback callback) {

        DynamoRequestPacket dynamoRequestPacket = this.getDynamoRequestPacket(request);
        boolean matched = false;

        ReplicatedDynamoStateMachine rqsm = this.getInstance(quorumID);

        if (rqsm != null) {
            matched = true;
            assert dynamoRequestPacket != null;
            dynamoRequestPacket.setQuorumID(quorumID);
//            dynamoRequestPacket.setCheckpointVersion(rqsm.getVersion());
            this.handleDynamoPacket(dynamoRequestPacket, rqsm, callback);
        } else {
            System.out.println("The given quorumID has no state machine associated");
        }


        return matched ? rqsm.getQuorumID() : null;
    }

    private DynamoRequestPacket getDynamoRequestPacket(Request request) {
        try {
            return (DynamoRequestPacket) request;
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        return null;
    }

    private ReplicatedDynamoStateMachine getInstance(String quorumID) {
        return this.replicatedQuorums.get(quorumID);
    }

    public boolean createReplicatedQuorumForcibly(String quorumID, int version,
                                                  Set<NodeIDType> nodes, Replicable app,
                                                  String state) {
        return this.createReplicatedQuorumFinal(quorumID, version, nodes, app, state) != null;
    }

    private synchronized ReplicatedDynamoStateMachine createReplicatedQuorumFinal(
            String quorumID, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState) {
        ReplicatedDynamoStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm != null)
            return rqsm;
        try {
            rqsm = new ReplicatedDynamoStateMachine(quorumID, version, myID,
                    this.integerMap.put(nodes), app != null ? app : this.myApp,
                    initialState, this);
            quorums.add(quorumID);
            System.out.println("Creating new Replicated Quorum State Machine: " + rqsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(quorumID, rqsm);
        this.integerMap.put(nodes);
        this.initializeDAG(quorumID, rqsm);
        this.checkpointVC.put(quorumID, rqsm.getInitialVectorClock());
        if (rqsm.getDagLogger().getCheckpointLog() != null && !rqsm.getDagLogger().getCheckpointLog().getState().isEmpty()) {
            this.myApp.restore(myApp.toString(), rqsm.getDagLogger().getCheckpointLog().getState());
            this.noOfCheckpoints = rqsm.getDagLogger().getCheckpointLog().getNoOfCheckpoints();
        }
        return rqsm;
    }

    public Set<NodeIDType> getReplicaGroup(String quorumID) {
        ReplicatedDynamoStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(rqsm.getQuorumMembersArray());
    }

    public boolean deleteReplicatedQuorum(String quorumID, int epoch) {
        ReplicatedDynamoStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm == null)
            return true;
        if (rqsm.getVersion() > epoch) {
            return false;
        }
        return this.removeInstance(quorumID);
    }

    private boolean removeInstance(String quorumID) {
        return this.replicatedQuorums.remove(quorumID) != null;
    }

    private void putInstance(String quorumID, ReplicatedDynamoStateMachine rqsm) {
        this.replicatedQuorums.put(quorumID, rqsm);
    }

    private synchronized void initializeDAG(String quorumID, ReplicatedDynamoStateMachine rqsm) {
        this.requestDAG.put(quorumID, new DAG(rqsm.getInitialVectorClock()));
        if (rqsm.getDagLogger().getCheckpointLog() != null && !rqsm.getDagLogger().getCheckpointLog().getLatestNodes().isEmpty()) {
            this.requestDAG.get(quorumID).addChildrenNodes(rqsm.getDagLogger().getCheckpointLog().getLatestNodes(), this.requestDAG.get(quorumID).getRootNode());
        }
        try {
            for (GraphNode graphNode : rqsm.getDagLogger().readFromRollForwardFile()) {
                DynamoManager.log.log(Level.INFO, "req: " + graphNode.getRequests().size());
                for (RequestInformation requestInformation : graphNode.getRequests()) {
                    DynamoManager.log.log(Level.INFO, requestInformation.getRequestQuery());
                    if (!requestInformation.getRequestQuery().split(" ")[0].equals("PUT") || !requestInformation.getRequestQuery().split(" ")[0].equals("PUT_FWD")) {
                        continue;
                    }
                    DynamoRequestPacket qp = new DynamoRequestPacket(requestInformation.getRequestID(),
                            requestInformation.getRequestQuery().split(" ")[1], DynamoRequestPacket.DynamoPacketType.PUT, quorumID);
                    Request request = getInterfaceRequest(this.myApp, qp.toString());
                    this.myApp.execute(request, false);
                }
                addChildNode(this.requestDAG.get(quorumID).latestNodesWithVectorClockAsDominant(graphNode, false), graphNode);
            }
            log.log(Level.INFO, "Roll forward complete");
        } catch (Exception e) {
            log.log(Level.SEVERE, e.toString());
            log.log(Level.SEVERE, "Roll forward could not be performed");
        }
    }

    public Integer getVersion(String quorumID) {
        ReplicatedDynamoStateMachine rqsm = this.getInstance(quorumID);
        if (rqsm != null)
            return (int) rqsm.getVersion();
        return -1;
    }
}
