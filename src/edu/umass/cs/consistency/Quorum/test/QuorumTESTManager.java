package edu.umass.cs.consistency.Quorum.test;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.consistency.Quorum.QuorumApp;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QuorumTESTManager<NodeIDType> {

    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Replicable myApp;
    private final HashMap<String, TESTrqsm> replicatedQuorums;

    //    Maps the version of this node for a quorumID
    private HashMap<String, Integer> version = new HashMap<>();
    private final Stringifiable<NodeIDType> unstringer;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    //    Maps the reqestID to QuorumRequestAndCallback object
    private HashMap<Long, QuorumRequestAndCallback> requestsReceived = new HashMap<Long, QuorumRequestAndCallback>();
    private ArrayList<String> quorums = new ArrayList<String>();
    private final LargeCheckpointer largeCheckpointer;
    private static final Logger log = Logger.getLogger(ReconfigurationConfig.class.getName());

    public static final Class<?> application = QuorumApp.class;
    public static final String getDefaultServiceName() {
        return application.getSimpleName() + "0";
    }
    public QuorumTESTManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
                         InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable instance,
                         String logFolder, boolean enableNullCheckpoints) {
        this.myID = this.integerMap.put(id);

        this.unstringer = unstringer;

        this.largeCheckpointer = new LargeCheckpointer(logFolder,
                id.toString());
        this.myApp = LargeCheckpointer.wrap(instance, largeCheckpointer);

        this.replicatedQuorums = new HashMap<>();

        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));
    }
    public static class QuorumRequestAndCallback {
        protected QuorumTESTRequestPacket quorumRequestPacket;
        final ExecutedCallback callback;
        protected Integer numOfAcksReceived = 0;
        protected Integer version = -1;
        protected String value = null;

        QuorumRequestAndCallback(QuorumTESTRequestPacket quorumRequestPacket, ExecutedCallback callback){
            this.quorumRequestPacket = quorumRequestPacket;
            this.callback = callback;
        }

        @Override
        public String toString(){
            return this.quorumRequestPacket +" ["+ callback+"]";
        }
        public void reset(int version, String value){
            this.numOfAcksReceived = 0;
            this.quorumRequestPacket.setVersion(version);
            this.quorumRequestPacket.setResponseValue(value);
        }
        public Integer incrementAck(QuorumTESTRequestPacket qp){
            this.numOfAcksReceived += 1;
            if (qp.getType() == QuorumTESTRequestPacket.QuorumTESTPacketType.READACK && qp.getVersion() >= this.version){
                this.value = qp.getResponseValue();
                this.version = qp.getVersion();
            }
            return this.numOfAcksReceived;
        }
        public Integer getNumOfAcksReceived(){
            return this.numOfAcksReceived;
        }
        public QuorumTESTRequestPacket getPacket(){
//            this.quorumRequestPacket.setVersion(this.version);
//            this.quorumRequestPacket.addResponse("value",this.value.toString());
//            this.quorumRequestPacket.setPacketType(QuorumRequestPacket.QuorumPacketType.RESPONSE);
            return this.quorumRequestPacket;
        }
        public QuorumTESTRequestPacket.QuorumTESTPacketType getPacketType(){
            return this.quorumRequestPacket.getType();
        }

        public Integer getVersion() {
            return version;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }

        public String  getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
    private void handleQuorumPacket(QuorumTESTRequestPacket qp, TESTrqsm rqsm, ExecutedCallback callback){

        QuorumTESTRequestPacket.QuorumTESTPacketType packetType = qp.getType();

        switch(packetType) {
            case READ: case WRITE:
                // client -> node
                handleRequest(qp, rqsm, callback);
                break;
            case READFORWARD:
                // node -> read_quorum_node
                handleReadForward(qp);
                break;
            case READACK:
                // read_quorum_node -> node
                handleReadAck(qp, rqsm);
                break;
            case WRITEFORWARD:
                // node -> write_quorum_node
                handleWriteForward(qp);
                break;
            case WRITEACK:
                // write_quorum_node -> node
                handleWriteAck(qp, rqsm);
                break;
            case QUORUM_PACKET:
                break;
            default:
                break;
        }

    }
    private void handleRequest(QuorumTESTRequestPacket qp,
                               TESTrqsm rqsm, ExecutedCallback callback){
        System.out.println(qp.getSummary());
        this.requestsReceived.putIfAbsent(qp.getRequestID(), new QuorumRequestAndCallback(qp, callback));
//        Send request to all the quorum members
        for (int i = 0; i < rqsm.getQuorumMembers().size()-1; i++) {
            qp.setPacketType(QuorumTESTRequestPacket.QuorumTESTPacketType.READFORWARD);
            qp.setSource(this.myID);
            qp.setDestination(rqsm.getQuorumMembers().get(i));
            this.sendRequest(qp, qp.getDestination());
        }
    }
    public void handleReadForward(QuorumTESTRequestPacket qp){
//        return the value from underlying app and the version from version hashmap
//        System.out.println(qp.toString());
        Request request = getInterfaceRequest(this.myApp, qp.toString());
        this.myApp.execute(request, false);
        qp.setPacketType(QuorumTESTRequestPacket.QuorumTESTPacketType.READACK);
        int dest = qp.getDestination();
        qp.setDestination(qp.getSource());
        qp.setSource(dest);
        qp.setVersion(version.get(qp.getQuorumID()));
        assert request != null;
        qp.setResponseValue(((QuorumTESTRequestPacket)request).getResponseValue());
//        System.out.println(qp.toString());
        this.sendRequest(qp, qp.getDestination());
    }
    public void handleWriteForward(QuorumTESTRequestPacket qp){
//        return the value from underlying app and the version from version hashmap
        if(qp.getVersion() > this.version.get(qp.getQuorumID())){
            try{
                Request request = getInterfaceRequest(this.myApp, qp.toString());
                this.myApp.execute(request, false);
                assert request != null;
                qp.setResponseValue(((QuorumTESTRequestPacket) request).getResponseValue());
                version.put(qp.getQuorumID(), qp.getVersion());
            }
            catch (Exception e){
                qp.setResponseValue(e.toString());
            }
        }
        else {
            qp.setResponseValue("Already executed");
        }
        qp.setPacketType(QuorumTESTRequestPacket.QuorumTESTPacketType.WRITEACK);
        int dest = qp.getDestination();
        qp.setDestination(qp.getSource());
        qp.setSource(dest);
        this.sendRequest(qp, qp.getDestination());
    }
    public void handleReadAck(QuorumTESTRequestPacket qp, TESTrqsm rqsm){
//        append in the hashmap and check the total
        if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp) >= rqsm.getReadQuorum()){
            QuorumTESTRequestPacket writePacket = this.requestsReceived.get(qp.getRequestID()).getPacket();
            if(writePacket.getType() == QuorumTESTRequestPacket.QuorumTESTPacketType.WRITE) {
                writePacket.setVersion(this.requestsReceived.get(qp.getRequestID()).getVersion()+1);
            }
            else if(writePacket.getType() == QuorumTESTRequestPacket.QuorumTESTPacketType.READ) {
                writePacket.setVersion(this.requestsReceived.get(qp.getRequestID()).getVersion());
                writePacket.setRequestValue(this.requestsReceived.get(qp.getRequestID()).getValue());
            }
            sendWriteRequests(writePacket, rqsm);
        }
    }
    public void handleWriteAck(QuorumTESTRequestPacket qp, TESTrqsm rqsm){
//        append in the hashmap and check the total
//        if write request send executed, for read attach the value string to response
        if (this.requestsReceived.get(qp.getRequestID()).incrementAck(qp) >= rqsm.getWriteQuorum()){
            sendResponse(qp);
        }
    }
    public void sendWriteRequests(QuorumTESTRequestPacket qp, TESTrqsm rqsm){
        this.requestsReceived.get(qp.getRequestID()).reset(qp.getVersion(), qp.getRequestValue());
        for (int i = 0; i < rqsm.getQuorumMembers().size()-1; i++) {
            qp.setSource(this.myID);
            qp.setDestination(rqsm.getQuorumMembers().get(i));
            qp.setPacketType(QuorumTESTRequestPacket.QuorumTESTPacketType.WRITEFORWARD);
            this.sendRequest(qp, qp.getDestination());
        }
    }
    public void sendResponse(QuorumTESTRequestPacket qp){
        QuorumRequestAndCallback requestAndCallback = this.requestsReceived.get(qp.getRequestID());
        if (requestAndCallback != null && requestAndCallback.callback != null) {

            requestAndCallback.callback.executed(qp
                    , true);
            this.requestsReceived.remove(qp.getRequestID());

        } else {
            // can't find the request being queued in outstanding
            log.log(Level.WARNING, "QuorumManager.handleResponse received " +
                            "an ACK request {0} that does not match any enqueued request.",
                    new Object[]{qp.getRequestID()});
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
    public int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }
    private void sendRequest(QuorumTESTRequestPacket qp,
                             int nodeID){
        GenericMessagingTask<NodeIDType,?> gTask = null;
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(nodeID),
                    qp);
            this.messenger.send(gTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String propose(String quorumID, Request request,
                          ExecutedCallback callback) {
        QuorumTESTRequestPacket quorumRequestPacket = this.getQuorumRequestPacket(request);
        System.out.println("In propose");
        boolean matched = false;

        TESTrqsm rqsm = this.getInstance(quorumID);

        if (rqsm != null) {
            matched = true;
            assert quorumRequestPacket != null;
            quorumRequestPacket.setQuorumID(quorumID);
            this.handleQuorumPacket(quorumRequestPacket, rqsm, callback);
        } else {
            System.out.println("The given quorumID has no state machine associated");
        }
        return matched ? rqsm.getQuorumID() : null;
    }
    private QuorumTESTRequestPacket getQuorumRequestPacket(Request request){
        try {
            return (QuorumTESTRequestPacket) request;
        }
        catch (Exception e){
            System.out.println(e.toString());
        }
        return null;
    }

    public boolean createReplicatedQuorumForcibly(String quorumID, int version,
                                                  Set<NodeIDType> nodes, Replicable app,
                                                  String state){
        return this.createReplicatedQuorumFinal(quorumID, version, nodes, app, state) != null;
    }

    private synchronized TESTrqsm createReplicatedQuorumFinal(
            String quorumID, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState){
        TESTrqsm rqsm = this.getInstance(quorumID);
        if (rqsm != null)
            return rqsm;
        try {
            rqsm = new TESTrqsm(quorumID, version, myID,
                    this.integerMap.put(nodes), app != null ? app : this.myApp,
                    initialState, this);
            quorums.add(quorumID);
            System.out.println("Creating new Replicated Quorum State Machine: "+ rqsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(quorumID, rqsm);
        this.integerMap.put(nodes);
        this.version.put(quorumID, 0);
        return rqsm;
    }
    public Set<NodeIDType> getReplicaGroup(String quorumID) {
        TESTrqsm rqsm = this.getInstance(quorumID);
        if (rqsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(rqsm.getQuorumMembersArray());
    }
    public boolean deleteReplicatedQuorum(String quorumID, int epoch){
        TESTrqsm rqsm = this.getInstance(quorumID);
        if(rqsm == null)
            return true;
        if(rqsm.getVersion() > epoch) {
            return false;
        }
        return this.removeInstance(quorumID);
    }
    private boolean removeInstance(String quorumID) {
        return this.replicatedQuorums.remove(quorumID) != null;
    }
    private void putInstance(String quorumID, TESTrqsm rcsm){
        this.replicatedQuorums.put(quorumID, rcsm);
    }

    private TESTrqsm getInstance(String quorumID){
        return this.replicatedQuorums.get(quorumID);
    }

    public Integer getVersion(String quorumID) {
        TESTrqsm rqsm = this.getInstance(quorumID);
        if ( rqsm != null)
            return (int) rqsm.getVersion();
        return -1;
    }
    private void checkpoint(HashMap<String, Integer> hashMap) {
        try {
            FileOutputStream fileOut = new FileOutputStream("/tmp/quorum_logs/checkpoint.log");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(hashMap);
            out.close();
            fileOut.close();
            System.out.println("Checkpoint created successfully.");
        } catch (Exception e) {
            System.err.println("Error during checkpoint: " + e.getMessage());
        }
    }
    private void rollback(HashMap<String, Integer> hashMap) {
        try {
            FileInputStream fileIn = new FileInputStream("/tmp/quorum_logs/checkpoint.log");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            HashMap<String, Integer> checkpointedHashMap = (HashMap<String, Integer>) in.readObject();
            in.close();
            fileIn.close();
            // Restore the state
            hashMap.clear();
            hashMap.putAll(checkpointedHashMap);
            System.out.println("Rollback successful.");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error during rollback: " + e.getMessage());
        }
    }

    public ArrayList<String> getQuorums(){
        return this.quorums;
    }
}
