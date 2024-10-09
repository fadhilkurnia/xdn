package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.chainreplication.chainutil.ReplicatedChainException;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.InterfaceNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static edu.umass.cs.consistency.ClientCentric.CCRequestPacket.CCPacketType.*;

public class CCManager<NodeIDType> {
    private final PaxosMessenger<NodeIDType> messenger;
    private final int myID;
    private final Replicable myApp;
//    private final FailureDetection<NodeIDType> FD;
    private final HashMap<String, CCReplicatedStateMachine> replicatedSM = new HashMap<>();
    private HashMap<Long, HashMap<Integer, Timestamp>> cvc = new HashMap<>();
    private final HashMap<String, ArrayList<Write>> writesByServer = new HashMap<>();
    private final HashMap<String, HashMap<Integer, Timestamp>> wvc = new HashMap<>();
    private HashMap<Long, MRRequestAndCallback> requestsReceived = new HashMap<Long, MRRequestAndCallback>();
    private ArrayList<String> serviceNames = new ArrayList<String>();
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    public final Stringifiable<NodeIDType> unstringer;
    private final LargeCheckpointer largeCheckpointer;
    public static final Logger log = Logger.getLogger(CCManager.class.getName());

    public static final Class<?> application = CCApp.class;
    public CCManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
                     InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable instance,
                     String logFolder, boolean enableNullCheckpoints) {
        this.myID = this.integerMap.put(id);

        this.unstringer = unstringer;

        this.largeCheckpointer = new LargeCheckpointer(logFolder,
                id.toString());

        this.myApp = LargeCheckpointer.wrap(instance, largeCheckpointer);

        this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));
        FileHandler fileHandler = null;
        try {
            fileHandler = new FileHandler("output/ccManager"+myID+".log", true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        fileHandler.setFormatter(new SimpleFormatter());
        log.addHandler(fileHandler);
        log.setLevel(Level.FINE);
    }
    /**
    This class is used to represent a write.
     **/
    public static class Write{
        private String statement;
        private Timestamp ts;
        private int node;
        Write(Timestamp ts, String statement, int node){
            this.statement = statement;
            this.ts = ts;
            this.node = node;
        }
//        write a toString for this class
        @Override
        public String toString(){
            try {
                return this.toJSONObjectImpl().toString();
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }

        protected JSONObject toJSONObjectImpl() throws JSONException {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("ts", this.ts);
            jsonObject.put("statement", this.statement);
            jsonObject.put("node", this.node);
            return jsonObject;
        }
        public String getStatement(){
            return this.statement;
        }

        public Timestamp getTs() {
            return ts;
        }

        public int getNode() {
            return node;
        }
    }

    /**
     * Comparator for the Priority queue that consists of objects of type Write
     */
    class WriteComparator implements Comparator<Write>{
        public int compare(Write w1, Write w2) {
            int compare = w1.ts.compareTo(w2.ts);
            if (compare > 0)
                return 1;
            else if (compare < 0)
                return -1;
            return 0;
        }
    }

    /**
     * Maps the request to the appropriate callback.
     * Fields:
     * 1. mrRequestPacket - Original request packet
     * 2. callback - Associated callback
     * 3. requestSent - Set of nodes to which a request is sent
     * 4. pq - priority queue which will contain all the response writes ordered by their timestamp
     */
    public class MRRequestAndCallback {
        protected CCRequestPacket CCRequestPacket;
        final ExecutedCallback callback;
        protected Set<Integer> requestSent = new HashSet<>();
        private HashMap<Integer, Timestamp> cvc = new HashMap<>();
        private int cvcGetAckReceived;
        private int cvcPutAckReceived;
        PriorityQueue<Write> pq = new PriorityQueue<Write>(new WriteComparator());

        MRRequestAndCallback(CCRequestPacket CCRequestPacket, ExecutedCallback callback){
            this.CCRequestPacket = new CCRequestPacket(CCRequestPacket.getClientID(), CCRequestPacket.getRequestID(), CCRequestPacket.getPacketType(), CCRequestPacket);
            this.callback = callback;
        }
        MRRequestAndCallback(CCRequestPacket CCRequestPacket, ExecutedCallback callback, HashMap<Integer, Timestamp> cvc){
            this.CCRequestPacket = new CCRequestPacket(CCRequestPacket.getClientID(), CCRequestPacket.getRequestID(), CCRequestPacket.getPacketType(), CCRequestPacket);
            this.callback = callback;
            this.cvcGetAckReceived = 0;
            this.cvcPutAckReceived = 0;
            this.cvc = cvc;
        }

        @Override
        public String toString(){
            return this.CCRequestPacket +" ["+ callback+"]";
        }
//        Once an ack is received the response writes are added to the priority queue. Return true if the requestSent set is empty.
        public boolean ackReceived(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm){
            System.out.println(requestSent);
            removeReqFromSet(CCRequestPacket.getSource());
            if(!CCRequestPacket.getResponseWrites().isEmpty()) {
                this.addWrites(CCRequestPacket.getResponseWrites().get(CCRequestPacket.getSource()));
            }
            if (this.requestSent.isEmpty()){
                return true;
            }
            return false;
        }
        public int putCvc(CCRequestPacket CCRequestPacket){
            cvcGetAckReceived++;
            for (int member: cvc.keySet()){
                Timestamp finalTS = cvc.get(member).compareTo(CCRequestPacket.getResponseVectorClock().get(member)) > 0 ? cvc.get(member) : CCRequestPacket.getResponseVectorClock().get(member);
                cvc.put(member, finalTS);
            }
            return cvcGetAckReceived;
        }

        public int incrementPutAck(){
            cvcPutAckReceived++;
            return cvcPutAckReceived;
        }

        public HashMap<Integer, Timestamp> getCvc() {
            return cvc;
        }

        public boolean isWrite(){
            System.out.println("type: "+this.CCRequestPacket.getPacketType());
            return this.CCRequestPacket.getPacketType() == MR_WRITE || this.CCRequestPacket.getPacketType() == MW_WRITE;
        }
        public void addCurrentIfNeeded(Integer nodeID, Timestamp ts){
            if(this.CCRequestPacket.getPacketType() == MW_WRITE || this.CCRequestPacket.getPacketType() == MR_WRITE){
                this.CCRequestPacket.addResponseWrites(nodeID, ts, this.CCRequestPacket.getRequestValue());
            }
        }
        public void setResponse(HashMap<Integer, Timestamp> responseVectorClock, String responseValue){
            this.CCRequestPacket.setResponseVectorClock(responseVectorClock);
            this.CCRequestPacket.setResponseValue(responseValue);
        }
        public void removeReqFromSet(Integer node){
            this.requestSent.remove(node);
        }
        public void addReqToSet(Integer node){
            this.requestSent.add(node);
        }
        public PriorityQueue<Write> getPq() {
            return this.pq;
        }
        public void addWrites(ArrayList<Write> arrayListToAdd){
            this.pq.addAll(arrayListToAdd);
        }
        public CCRequestPacket getMrRequestPacket() {
            return this.CCRequestPacket;
        }

        public Integer getRequestSentLength() {
            System.out.println(this.requestSent);
            return requestSent.size();
        }
    }
    private void handlePacket(CCRequestPacket qp, CCReplicatedStateMachine mrsm, ExecutedCallback callback){
        CCRequestPacket.CCPacketType packetType = qp.getType();
        try {
            if (packetType == MR_READ || packetType == MR_WRITE || packetType == MW_WRITE) {
                getCVC(qp, mrsm, callback);
            }
            switch (packetType) {
                case CVC_GET:
                    handleCVCGet(qp, mrsm);
                    break;
                case CVC_GET_ACK:
                    handleCVCGetAck(qp, mrsm);
                    break;
                case CVC_PUT:
                    handleCVCPut(qp, mrsm);
                    break;
                case CVC_PUT_ACK:
                    handleCVCPutAck(qp, mrsm);
                    break;
                case MW_READ:
                    handleReadRequestMW(qp, mrsm, callback);
                    break;
                case FWD:
                    handleFwdRequest(qp, mrsm);
                    break;
                case FWD_ACK:
                    handleFwdAck(qp, mrsm);
                    break;
                default:
                    break;
            }
        }
        catch (NullPointerException e){
            log.log(Level.WARNING, "Received Acknowledgement for requestID {0} after requirement was satisfied from Node {1}.", new Object[]{qp.getRequestID(), qp.getSource()});
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private void getCVC(CCRequestPacket ccRequestPacket, CCReplicatedStateMachine mrsm, ExecutedCallback callback){
        this.requestsReceived.putIfAbsent(ccRequestPacket.getRequestID(), new MRRequestAndCallback(ccRequestPacket, callback, cvc.get(ccRequestPacket.getClientID())));
        ccRequestPacket.setPacketType(CVC_GET);
        for (int i = 0; i < mrsm.getMembers().size(); i++) {
            if (mrsm.getMembers().get(i) != this.myID) {
                log.log(Level.INFO, "Sending CVC_GET from {0} to {1} for requestID {2}", new Object[]{myID, mrsm.getMembers().get(i), ccRequestPacket.getRequestID()});
                ccRequestPacket.setSource(this.myID);
                ccRequestPacket.setDestination(mrsm.getMembers().get(i));
                this.sendRequest(ccRequestPacket, ccRequestPacket.getDestination());
            }
        }
    }

    private void handleCVCGet(CCRequestPacket ccRequestPacket, CCReplicatedStateMachine mrsm){
        initializeCVC(ccRequestPacket, mrsm);
        log.log(Level.INFO, "Setting response vector clock as {0} for requestID {1}", new Object[]{cvc.get(ccRequestPacket.getClientID()), ccRequestPacket.getRequestID()});
        ccRequestPacket.setResponseVectorClock(cvc.get(ccRequestPacket.getClientID()));
        ccRequestPacket.setPacketType(CVC_GET_ACK);
        ccRequestPacket.setDestination(ccRequestPacket.getSource());
        ccRequestPacket.setSource(myID);
        this.sendRequest(ccRequestPacket, ccRequestPacket.getDestination());
    }

    private void handleCVCGetAck(CCRequestPacket ccRequestPacket, CCReplicatedStateMachine mrsm) throws NullPointerException{
        if(this.requestsReceived.get(ccRequestPacket.getRequestID()).putCvc(ccRequestPacket) == mrsm.getReadQuorum() - 1){
            this.cvc.put(ccRequestPacket.getClientID(), this.requestsReceived.get(ccRequestPacket.getRequestID()).getCvc());
            for (int i = 0; i < mrsm.getMembers().size(); i++) {
                if (mrsm.getMembers().get(i) != this.myID & cvc.get(ccRequestPacket.getClientID()).get(mrsm.getMembers().get(i)).compareTo(wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i))) > 0) {
                    ccRequestPacket.setPacketType(FWD);
                    ccRequestPacket.setWritesFrom(this.wvc.get(mrsm.getServiceName()).get(mrsm.getMembers().get(i)));
                    log.log(Level.INFO, "Getting writes from {0}", new Object[]{mrsm.getMembers().get(i)});
                    ccRequestPacket.setSource(this.myID);
                    ccRequestPacket.setDestination(mrsm.getMembers().get(i));
                    this.requestsReceived.get(ccRequestPacket.getRequestID()).addReqToSet(ccRequestPacket.getDestination());
                    this.sendRequest(ccRequestPacket, ccRequestPacket.getDestination());
                }
            }
            if(this.requestsReceived.get(ccRequestPacket.getRequestID()).getRequestSentLength() == 0){
                log.info("WVC up to date");
                updateVectorClocksAndExecute(ccRequestPacket, mrsm);
            }
        }
    }
    private void handleCVCPut(CCRequestPacket ccRequestPacket, CCReplicatedStateMachine mrsm){
        if(!cvc.containsKey(ccRequestPacket.getClientID())){
            cvc.put(ccRequestPacket.getClientID(), ccRequestPacket.getRequestVectorClock());
        }
        else{
            for (int member : cvc.get(ccRequestPacket.getClientID()).keySet()) {
                Timestamp finalTS = cvc.get(ccRequestPacket.getClientID()).get(member).compareTo(ccRequestPacket.getRequestVectorClock().get(member)) > 0 ? cvc.get(ccRequestPacket.getClientID()).get(member) : ccRequestPacket.getRequestVectorClock().get(member);
                cvc.get(ccRequestPacket.getClientID()).put(member, finalTS);
            }
        }
        log.log(Level.INFO, "Updated CVC to {0}", new Object[]{cvc.get(ccRequestPacket.getClientID())});
        ccRequestPacket.setDestination(ccRequestPacket.getSource());
        ccRequestPacket.setSource(myID);
        ccRequestPacket.setPacketType(CVC_PUT_ACK);
        this.sendRequest(ccRequestPacket, ccRequestPacket.getDestination());
    }

    private void handleCVCPutAck(CCRequestPacket ccRequestPacket, CCReplicatedStateMachine mrsm) throws NullPointerException{
        if(this.requestsReceived.get(ccRequestPacket.getRequestID()).incrementPutAck() == mrsm.getWriteQuorum() - 1){
            sendResponse(ccRequestPacket.getRequestID(), this.requestsReceived.get(ccRequestPacket.getRequestID()).getMrRequestPacket());
        }
    }
    /**
     * For monotonic writes the read request is directly executed
     * @param CCRequestPacket
     * @param mrsm
     * @param callback
     */
    private void handleReadRequestMW(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm, ExecutedCallback callback){
        log.log(Level.INFO, "Read MW request received with requestID {0}", new Object[]{CCRequestPacket.getRequestID()});
        this.requestsReceived.putIfAbsent(CCRequestPacket.getRequestID(), new MRRequestAndCallback(CCRequestPacket, callback));
        CCRequestPacket.setResponseVectorClock(this.wvc.get(mrsm.getServiceName()));
        CCRequestPacket.setPacketType(FWD_ACK);
        CCRequestPacket.setSource(this.myID);
        this.requestsReceived.get(CCRequestPacket.getRequestID()).addReqToSet(CCRequestPacket.getSource());
        handlePacket(CCRequestPacket, mrsm, callback);
    }


    /**
     * Adds writes from own write set to the response
     * @param CCRequestPacket
     * @param mrsm
     */
    private void handleFwdRequest(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm){
        CCRequestPacket.setPacketType(FWD_ACK);
        if (CCRequestPacket.getWritesTo().equals(new Timestamp(0))){
            CCRequestPacket.setWritesTo(new Timestamp(System.currentTimeMillis()));
        }
        for (Write write: this.writesByServer.get(mrsm.getServiceName())){
            if((write.getTs().compareTo(CCRequestPacket.getWritesFrom()) >= 0) & (write.getTs().compareTo(CCRequestPacket.getWritesTo()) <= 0)){
                log.log(Level.INFO, "Adding to response writes: {0}", new Object[]{write.getStatement()});
                CCRequestPacket.addResponseWrites(this.myID, write.getTs(), write.getStatement());
            }
        }
        CCRequestPacket.setDestination(CCRequestPacket.getSource());
        CCRequestPacket.setSource(myID);
        this.sendRequest(CCRequestPacket, CCRequestPacket.getDestination());
    }

    /**
     * If writes from all nodes are received, executes the writes in the priority queue. Updates vector clock and write set if needed
     * @param CCRequestPacket
     * @param mrsm
     */
    private void handleFwdAck(CCRequestPacket CCRequestPacket, CCReplicatedStateMachine mrsm){
        if(this.requestsReceived.get(CCRequestPacket.getRequestID()).ackReceived(CCRequestPacket, mrsm)){
            for (Write write : this.requestsReceived.get(CCRequestPacket.getRequestID()).getPq()){
                log.log(Level.INFO, "Executing the forwarded write {0}", new Object[]{write.getStatement()});
                CCRequestPacket.setPacketType(MW_WRITE);
                System.out.println(write.getStatement());
                CCRequestPacket.setRequestValue(write.getStatement());
                this.wvc.get(mrsm.getServiceName()).put(write.getNode(), write.getTs());
                Request request = getInterfaceRequest(this.myApp, CCRequestPacket.toString());
                this.myApp.execute(request, false);
            }
            updateVectorClocksAndExecute(CCRequestPacket, mrsm);
        }
    }

    private void updateVectorClocksAndExecute(CCRequestPacket ccRequestPacket, CCReplicatedStateMachine mrsm){
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        Request request = getInterfaceRequest(this.myApp, this.requestsReceived.get(ccRequestPacket.getRequestID()).getMrRequestPacket().toString());
        this.myApp.execute(request, false);
        assert request != null;
        this.requestsReceived.get(ccRequestPacket.getRequestID()).setResponse(this.wvc.get(mrsm.getServiceName()), ((CCRequestPacket)request).getResponseValue());
        if(this.requestsReceived.get(ccRequestPacket.getRequestID()).isWrite()) {
            this.wvc.get(mrsm.getServiceName()).put(this.myID, ts);
            log.log(Level.INFO, "WVC updated to {0}", new Object[]{wvc});
            this.cvc.get(ccRequestPacket.getClientID()).put(this.myID, ts);
            log.log(Level.INFO, "WVC updated to {0}", new Object[]{cvc});
            this.writesByServer.get(mrsm.getServiceName()).add(new Write(ts, this.requestsReceived.get(ccRequestPacket.getRequestID()).getMrRequestPacket().getRequestValue(), this.myID));
            ccRequestPacket.setPacketType(CVC_PUT);
            ccRequestPacket.setRequestVectorClock(this.cvc.get(ccRequestPacket.getClientID()));
            for (int i = 0; i < mrsm.getMembers().size(); i++) {
                if (mrsm.getMembers().get(i) != this.myID) {
                    ccRequestPacket.setSource(this.myID);
                    ccRequestPacket.setDestination(mrsm.getMembers().get(i));
                    this.sendRequest(ccRequestPacket, ccRequestPacket.getDestination());
                }
            }
            return;
        }
        else {
            sendResponse(ccRequestPacket.getRequestID(), this.requestsReceived.get(ccRequestPacket.getRequestID()).getMrRequestPacket());
        }
    }
    public void sendResponse(Long requestID, CCRequestPacket mrResponsePacket){
        MRRequestAndCallback requestAndCallback = this.requestsReceived.get(requestID);
        if (requestAndCallback != null && requestAndCallback.callback != null) {
            mrResponsePacket.setPacketType(CCRequestPacket.CCPacketType.RESPONSE);
            mrResponsePacket.setSource(this.myID);
            mrResponsePacket.setResponseVectorClock(cvc.get(mrResponsePacket.getClientID()));
            log.log(Level.INFO, "Sending response for requestID {0}", new Object[]{requestID});
            requestAndCallback.callback.executed(mrResponsePacket
                    , true);
            this.requestsReceived.remove(requestID);
        } else {
            // can't find the request being queued in outstanding
            log.log(Level.WARNING, "QuorumManager.handleResponse received " +
                            "an ACK request {0} that does not match any enqueued request.",
                    new Object[]{requestID});
        }
    }
    private void sendRequest(CCRequestPacket CCRequestPacket,
                             int nodeID){
        GenericMessagingTask<NodeIDType,?> gTask = null;
        System.out.println("sending req:--"+ CCRequestPacket +" to node "+nodeID);
        try {
            // forward to nodeID
            gTask = new GenericMessagingTask(this.integerMap.get(nodeID),
                    CCRequestPacket);
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
    public boolean createReplicatedQuorumForcibly(String serviceName, int version,
                                                  Set<NodeIDType> nodes, Replicable app,
                                                  String state){
        return this.createReplicatedQuorumFinal(serviceName, version, nodes, app, state) != null;
    }
    private synchronized CCReplicatedStateMachine createReplicatedQuorumFinal(
            String serviceName, int version, Set<NodeIDType> nodes,
            Replicable app, String initialState){
        CCReplicatedStateMachine mrrsm = this.getInstance(serviceName);
        if (mrrsm != null)
            return mrrsm;
        try {
            mrrsm = new CCReplicatedStateMachine(serviceName, version, myID,
                    this.integerMap.put(nodes), app != null ? app : this.myApp,
                    initialState, this);
            serviceNames.add(serviceName);
            System.out.println("Creating new Replicated Monotonic Reads State Machine: "+ mrrsm);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ReplicatedChainException(e.getMessage());
        }

        this.putInstance(serviceName, mrrsm);
        this.integerMap.put(nodes);
//        this.FD.sendKeepAlive(nodes);
        this.putVectorClock(serviceName, mrrsm);
        this.initializeWriteSet(serviceName);
        return mrrsm;
    }
    public boolean deleteReplicatedQuorum(String serviceName, int epoch){
        CCReplicatedStateMachine mrsm = this.getInstance(serviceName);
        if(mrsm == null)
            return true;
        if(mrsm.getVersion() > epoch) {
            return false;
        }
        return this.removeInstance(serviceName);
    }
    public String propose(String serviceName, Request request,
                          ExecutedCallback callback) {
        CCRequestPacket ccRequestPacket = this.getMRRequestPacket(request);

        boolean matched = false;

        CCReplicatedStateMachine mrsm = this.getInstance(serviceName);
        if (mrsm != null) {
            matched = true;
            assert ccRequestPacket != null;
            ccRequestPacket.setServiceName(serviceName);
            initializeCVC(ccRequestPacket, mrsm);
            this.handlePacket(ccRequestPacket, mrsm, callback);
        } else {
            System.out.println("The given quorumID "+serviceName+" has no state machine associated");
        }


        return matched ? mrsm.getServiceName() : null;
    }

    private void initializeCVC(CCRequestPacket ccRequestPacket, CCReplicatedStateMachine mrsm){
        if(!cvc.containsKey(ccRequestPacket.getClientID())){
            cvc.put(ccRequestPacket.getClientID(), mrsm.getInitialVectorClock());
        }
    }
    private CCRequestPacket getMRRequestPacket(Request request){
        try {
            return (CCRequestPacket) request;
        }
        catch (Exception e){
            System.out.println(e.toString());
        }
        return null;
    }
    private CCReplicatedStateMachine getInstance(String quorumID){
        return this.replicatedSM.get(quorumID);
    }
    private void putInstance(String quorumID, CCReplicatedStateMachine mrrsm){
        this.replicatedSM.put(quorumID, mrrsm);
    }
    private void putVectorClock(String serviceName, CCReplicatedStateMachine mrrsm){
        this.wvc.put(serviceName, new HashMap<Integer, Timestamp>());
        for (int i = 0; i < mrrsm.getMembers().size(); i++) {
            this.wvc.get(serviceName).put(mrrsm.getMembers().get(i), new Timestamp(0));
        }
        System.out.println("wvc initialized: "+ this.wvc);
    }
    public void initializeWriteSet(String serviceName){
        this.writesByServer.put(serviceName, new ArrayList<>());
    }
    private boolean removeInstance(String serviceName) {
        return this.replicatedSM.remove(serviceName) != null;
    }
    public Integer getVersion(String quorumID) {
        CCReplicatedStateMachine mrsm = this.getInstance(quorumID);
        if ( mrsm != null)
            return (int) mrsm.getVersion();
        return -1;
    }
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        CCReplicatedStateMachine mrsm = this.getInstance(serviceName);
        if (mrsm == null)
            return null;
        return this.integerMap.getIntArrayAsNodeSet(mrsm.getMembersArray());
    }
    public static final String getDefaultServiceName() {
        return application.getSimpleName() + "0";
    }
}
