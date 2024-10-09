package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.ClientCentric.TESTMR;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.umass.cs.consistency.EventualConsistency.TESTDynamo.passed;

public class TESTDynamoClient extends ReconfigurableAppClientAsync<DynamoRequestPacket> {
    public final int[] ports = new int[]{2000, 2001, 2002};
    private final String[] items = new String[]{"table", "chair", "pen"};
    private final AtomicInteger responseCounter = new AtomicInteger(0);
    private String stateReceived = null;
    private HashMap<String, HashMap<Integer, Integer>> objectToVectorClock = new HashMap<>();
    private HashMap<String, Integer> objectToNumberOfEntries = new HashMap<>();
    static final Logger log = Logger.getLogger(TESTDynamoClient.class.getName());
    public TESTDynamoClient() throws IOException {
        super();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        try {
            return new DynamoRequestPacket(new JSONObject(stringified));
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }

    public static DynamoRequestPacket makeStopRequest() {
        return new DynamoRequestPacket(DynamoRequestPacket.DynamoPacketType.STOP, DynamoManager.getDefaultServiceName());
    }
    public static DynamoRequestPacket makePutRequest(TESTDynamoClient dc, int item) {
        JSONObject jsonObject = new JSONObject();
        String putString;
        if(item == -1) {
            int randomNum = (int) (Math.random() * ((dc.items.length - 1) + 1));
            putString = dc.items[randomNum];
        }
        else {
            putString = dc.items[item];
        }
        try {
            int randomNum = 1 + (int) (Math.random() * 5);
            jsonObject.put("key", putString);
            jsonObject.put("value", randomNum);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return new DynamoRequestPacket((long) (Math.random() * Integer.MAX_VALUE),
                jsonObject.toString(), DynamoRequestPacket.DynamoPacketType.PUT, DynamoManager.getDefaultServiceName());
    }

    public static DynamoRequestPacket makeGetRequest(TESTDynamoClient dc, int item) {
        JSONObject jsonObject = new JSONObject();
        String getString;
        if(item == -1) {
            int randomNum = (int) (Math.random() * ((dc.items.length - 1) + 1));
            getString = dc.items[randomNum];
        }
        else {
            getString = dc.items[item];
        }
        try {
            jsonObject.put("key", getString);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return new DynamoRequestPacket((long) (Math.random() * Integer.MAX_VALUE),
                jsonObject.toString(), DynamoRequestPacket.DynamoPacketType.GET, DynamoManager.getDefaultServiceName());
    }
    public boolean isDominant(HashMap<Integer, Integer> vectorClock, String object, boolean isGETRequest){
        if(!objectToVectorClock.containsKey(object)){
            if(!isGETRequest) putInObjectVectorClock(vectorClock, object);
            return true;
        }
        int difference = 0;
        for (int key : vectorClock.keySet()) {
            difference += vectorClock.get(key) - objectToVectorClock.get(object).get(key);
        }
        if(difference < 0){
            return false;
        }
        if(!isGETRequest) putInObjectVectorClock(vectorClock, object);
        return true;
    }
    private void putInObjectVectorClock(HashMap<Integer, Integer> vectorClock, String object){
        HashMap<Integer, Integer> vc = new HashMap<>();
        for(int key: vectorClock.keySet()){
            vc.put(key, vectorClock.get(key));
        }
        objectToVectorClock.put(object, vc);
    }
    public boolean checkNumberOfRequests(String responseFromApp, String object, boolean isGetRequest){
        if (isGetRequest) {
            return Integer.parseInt(responseFromApp) == objectToNumberOfEntries.get(object);
        }
        return true;
    }
    public void sendAppRequest(DynamoRequestPacket request, int port) throws IOException, InterruptedException, JSONException {
        if(request.getRequestType() == DynamoRequestPacket.DynamoPacketType.PUT){
            JSONObject jsonObject = new JSONObject(request.getRequestValue());
            objectToNumberOfEntries.put(jsonObject.getString("key"), objectToNumberOfEntries.getOrDefault(request.getRequestValue(), 0) + jsonObject.getInt("value"));
        }
        this.sendRequest(request,
                new InetSocketAddress("localhost", port),
                new Callback<Request, DynamoRequestPacket>() {

                    long createTime = System.currentTimeMillis();

                    @Override
                    public DynamoRequestPacket processResponse(Request response) {
                        assert (response instanceof DynamoRequestPacket) :
                                response.getSummary();

                        log.log(Level.INFO, "Response for request ["
                                + request.getSummary()
                                + " "
                                + request.getRequestValue()
                                + "] = "
                                + ((DynamoRequestPacket) response).getResponsePacket()
                                + " received in "
                                + (System.currentTimeMillis() - createTime)
                                + "ms");
                        System.out
                                .println("Response for request ["
                                        + request.getSummary()
                                        + " "
                                        + request.getRequestValue()
                                        + "] = "
                                        + ((DynamoRequestPacket) response).getResponsePacket()
                                        + " received in "
                                        + (System.currentTimeMillis() - createTime)
                                        + "ms");
                        passed.set(passed.get() & isDominant(((DynamoRequestPacket) response).getResponsePacket().getVectorClock(), request.getRequestValue(), request.getRequestType() == DynamoRequestPacket.DynamoPacketType.GET));
                        passed.set(passed.get() & checkNumberOfRequests(((DynamoRequestPacket) response).getResponsePacket().getValue(), request.getRequestValue(), request.getRequestType() == DynamoRequestPacket.DynamoPacketType.GET));
                        return (DynamoRequestPacket) response;
                    }
                });
    }


    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(TESTDynamoClient.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
        Result result01 = JUnitCore.runClasses(TESTMR.class);
        for (Failure failure : result01.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
    }
}
