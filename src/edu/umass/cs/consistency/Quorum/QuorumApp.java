package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.consistency.EventualConsistency.DynamoRequestPacket;
import edu.umass.cs.gigapaxos.examples.adder.StatefulAdderApp;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class QuorumApp extends StatefulAdderApp {
    public String name = "QuorumReplicationApp";
    protected HashMap<String, Integer> cart = new HashMap<String, Integer>();
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
//        System.out.println("In get request of app");
        QuorumRequestPacket quorumRequestPacket = null;
        try {
            quorumRequestPacket = new QuorumRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            ReconfigurationConfig.getLogger().info(
                    "Unable to parse request " + stringified);
            throw new RequestParseException(je);
        }
        return quorumRequestPacket;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(QuorumRequestPacket.QuorumPacketType.values()));
    }

    @Override
    public boolean execute(Request request) {
        System.out.println("In execute request of Dynamo Replication");
        if (request instanceof QuorumRequestPacket) {
            if (((QuorumRequestPacket) request).getType() == QuorumRequestPacket.QuorumPacketType.READ) {
                System.out.println("GET request for index: "+((QuorumRequestPacket) request).getRequestValue());
                try {
                    ((QuorumRequestPacket) request).setResponseValue(this.cart.get(((QuorumRequestPacket) request).getRequestValue()).toString());
                }
                catch (Exception e){
                    ((QuorumRequestPacket) request).setResponseValue("Exception: "+e);
                }
            }
            else{
                System.out.println("In Dynamo App for PUT request");
                try {
                    this.cart.merge(((QuorumRequestPacket) request).getRequestValue(), 1, Integer::sum);
                    ((QuorumRequestPacket) request).setResponseValue("Executed: "+this.cart.toString());
                } catch (Exception e) {
                    System.out.println("Check the request value");
                    ((QuorumRequestPacket) request).setResponseValue("Exception: "+e);
                    throw new RuntimeException(e);
                }
            }

            System.out.println("After execution: "+this.cart.toString());
            return true;
        }
        else System.err.println("Unknown request type: " + request.getRequestType());
        return false;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        System.out.println("In other execute");
        return this.execute(request);
    }

    @Override
    public String checkpoint(String name) {
        return this.total+"";
    }

    @Override
    public boolean restore(String name, String state) {
        if(state == null){
            this.total = 0;
        }
        else{
            try{
                this.total = Integer.valueOf(state);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        return true;
    }

}
