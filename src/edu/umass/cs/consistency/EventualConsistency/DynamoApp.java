package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.Repliconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamoApp implements Repliconfigurable {
    public String name = "DynamoReplicationApp";
    private HashMap<String, Integer> cart = new HashMap<>();
    private Logger log = Logger.getLogger(DynamoApp.class.getName());

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        try {
            return new DynamoRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            throw new RequestParseException(je);
        }
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }

    private Set<IntegerPacketType> getGETRequestTypes() {
        ArrayList<DynamoRequestPacket.DynamoPacketType> GETTypes = new ArrayList<>();
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET_FWD);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET_ACK);
        return new HashSet<IntegerPacketType>(GETTypes);
    }

    private Set<IntegerPacketType> getPUTRequestTypes() {
        ArrayList<DynamoRequestPacket.DynamoPacketType> GETTypes = new ArrayList<>();
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT_FWD);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT_ACK);
        return new HashSet<IntegerPacketType>(GETTypes);
    }

    @Override
    public boolean execute(Request request) {
        if (request instanceof DynamoRequestPacket) {
            if (getGETRequestTypes().contains(((DynamoRequestPacket) request).getType())) {
                log.log(Level.INFO, "GET request for index: " + ((DynamoRequestPacket) request).getRequestValue());
                if (!((DynamoRequestPacket) request).getRequestValue().isEmpty()) {
                    try {
                        JSONObject jsonObject = new JSONObject(((DynamoRequestPacket) request).getRequestValue());
                        ((DynamoRequestPacket) request).getResponsePacket().setValue(String.valueOf(this.cart.get(jsonObject.getString("key"))));
                    } catch (Exception e) {
                        ((DynamoRequestPacket) request).getResponsePacket().setValue("0");
                    }
                } else {
                    ((DynamoRequestPacket) request).getResponsePacket().setValue(this.cart.toString());
                }
            } else if (getPUTRequestTypes().contains(((DynamoRequestPacket) request).getType())) {
                log.log(Level.INFO, "In Dynamo App for PUT request");
                try {
                    JSONObject jsonObject = new JSONObject(((DynamoRequestPacket) request).getRequestValue());
                    this.cart.put(jsonObject.getString("key"), this.cart.getOrDefault(jsonObject.getString("key"), 0) + jsonObject.getInt("value"));
                } catch (JSONException e) {
                    log.log(Level.WARNING, "Check the request value");
                    throw new RuntimeException(e);
                }
            }
            log.log(Level.INFO, "After execution: " + this.cart.toString());
            return true;
        }
        log.log(Level.WARNING, "Unknown request type: " + request.getRequestType());
        return false;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {
        JSONObject jsonObject = new JSONObject(this.cart);
        return jsonObject.toString();
    }

    private void unjsonstringifyState(String state) throws JSONException {
        JSONObject jsonStateObject = new JSONObject(state);

        Iterator keys = jsonStateObject.keys();
        while (keys.hasNext()) {
            String key = keys.next().toString();
            Integer value = jsonStateObject.getInt(key);
            this.cart.put(key, value);
        }

    }

    @Override
    public boolean restore(String name, String state) {
        this.cart = new HashMap<String, Integer>();
        if (state != null) {
            try {
                unjsonstringifyState(state);
            } catch (Exception e) {
                System.out.println("Exception encountered: " + e);
                e.printStackTrace();
            }
        }
        return true;
    }

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        DynamoManager.log.log(Level.INFO, "....Get stop request called...");
        return new DynamoRequestPacket(DynamoRequestPacket.DynamoPacketType.STOP, name);
    }

    @Override
    public String getFinalState(String name, int epoch) {
        DynamoManager.log.log(Level.INFO, "....Get final state called...");
        return this.checkpoint(name);
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        DynamoManager.log.log(Level.INFO, "....Put initial state called...");
        this.restore(name, state);
    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        DynamoManager.log.log(Level.INFO, "....Delete final state called...");
        cart.clear();
        return true;
    }

    @Override
    public Integer getEpoch(String name) {
        return 0;
    }
}
