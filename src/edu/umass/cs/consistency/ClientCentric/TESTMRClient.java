package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.umass.cs.consistency.ClientCentric.TESTMR.passed;

public class TESTMRClient extends ReconfigurableAppClientAsync<CCRequestPacket> {
    public String[] types = new String[]{"C", "U"};
    public String[] items = new String[]{"CIRCLE01", "TRIANGLE01", "CIRCLE02", "TRIANGLE02", "CIRCLE03"};
    public String[] coords = new String[]{"1,1", "3,2", "4,4", "9,4", "8,5", "3,7", "6,3", "8,0"};
    public int[] ports = new int[]{2000, 2001, 2002};
    private HashMap<Integer, ArrayList<CCManager.Write>> requestWrites = new HashMap<>();
    private HashMap<Integer, Timestamp> requestVectorClock = new HashMap<Integer, Timestamp>();
    private final Long clientID;
    static final Logger log = Logger.getLogger(TESTMRClient.class
            .getName());

    public TESTMRClient() throws IOException {
        super();
        this.clientID = (long) (Math.random() * Integer.MAX_VALUE);
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
//        System.out.println("In getRequest of client");
        try {
//            System.out.println(stringified);
            return new CCRequestPacket(new JSONObject(stringified));
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(CCRequestPacket.CCPacketType.values()));
    }

    public CCRequestPacket makeWriteRequest(TESTMRClient mrc, CCRequestPacket.CCPacketType packetType) {
        int type = (int) (Math.random() * (mrc.types.length));
        int item = (int) (Math.random() * (mrc.items.length));
        int coord = (int) (Math.random() * (mrc.coords.length));
        String command = mrc.types[type] + " " + mrc.items[item] + " " + mrc.coords[coord];
        return new CCRequestPacket(this.clientID, (long) (Math.random() * Integer.MAX_VALUE), packetType, CCManager.getDefaultServiceName(),
                command, mrc.requestVectorClock, mrc.requestWrites);
    }

    public CCRequestPacket makeReadRequest(TESTMRClient mrc, CCRequestPacket.CCPacketType packetType) {
        return new CCRequestPacket(this.clientID, (long) (Math.random() * Integer.MAX_VALUE), packetType, CCManager.getDefaultServiceName(),
                "read_request", mrc.requestVectorClock, mrc.requestWrites);
    }

    public void updateWrites(CCRequestPacket response, TESTMRClient mrc) {
//        System.out.println("Response: "+response);
        mrc.requestVectorClock = response.getResponseVectorClock();
        if (response.getResponseWrites().containsKey(response.getSource())) {
            for (CCManager.Write write : response.getResponseWrites().get(response.getSource())) {
                if (!mrc.requestWrites.containsKey(response.getSource())) {
                    mrc.requestWrites.put(response.getSource(), new ArrayList<>());
                }
                mrc.requestWrites.get(response.getSource()).add(write);
            }
        }
    }
    public boolean checkRequestVectorClock(HashMap<Integer, Timestamp> receivedRVC){
        for (Integer key: receivedRVC.keySet()){
            if (receivedRVC.get(key).compareTo(this.requestVectorClock.get(key)) < 0){
                log.log(Level.WARNING, "Received: {0}, Given: {1}",new Object[]{receivedRVC.get(key), this.requestVectorClock.get(key)});
                return false;
            }
        }
        return true;
    }
    public void sendAppRequest(TESTMRClient mrClient, CCRequestPacket request, int port) throws IOException {
        mrClient.sendRequest(request,
                new InetSocketAddress("localhost", port),
                new Callback<Request, CCRequestPacket>() {
                    long createTime = System.currentTimeMillis();

                    @Override
                    public CCRequestPacket processResponse(Request response) {
                        assert (response instanceof CCRequestPacket) :
                                response.getSummary();

                        log.log(Level.INFO, "Response for request ["
                                + request.getSummary()
                                + " "
                                + request.getRequestValue()
                                + "] = "
                                + ((CCRequestPacket) response).getResponseValue()
                                + " received in "
                                + (System.currentTimeMillis() - createTime)
                                + "ms");
                        System.out
                                .println("Response for request ["
                                        + request.getSummary()
                                        + " "
                                        + request.getRequestValue()
                                        + "] = "
                                        + ((CCRequestPacket) response).getResponseValue()
                                        + " received in "
                                        + (System.currentTimeMillis() - createTime)
                                        + "ms");
                        if(request.getRequestType() != CCRequestPacket.CCPacketType.MW_READ) {
                            passed.set(passed.get() && mrClient.checkRequestVectorClock(((CCRequestPacket) response).getResponseVectorClock()));
                            mrClient.updateWrites(((CCRequestPacket) response), mrClient);
                        }
                        return (CCRequestPacket) response;
                    }
                });
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        Result result = JUnitCore.runClasses(TESTMRClient.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }

    }
}
