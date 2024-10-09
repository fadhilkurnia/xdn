package edu.umass.cs.consistency.lazyReplication;

import edu.umass.cs.gigapaxos.examples.adder.StatefulAdderApp;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.linwrites.SimpleAppRequest;
import edu.umass.cs.reconfiguration.examples.noop.NoopAppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LazyReplicationApp extends StatefulAdderApp {
    public String name = "LazyReplicationApp";
    protected int total = 0;

    @Override
    public Request getRequest(String stringified)
            throws RequestParseException {
        NoopAppRequest request = null;
        if (stringified.equals(Request.NO_OP)) {
            return this.getNoopRequest();
        }
        try {
            request = new NoopAppRequest(new JSONObject(stringified));
        } catch (JSONException je) {
            System.out.println("Request not parsed");
            throw new RequestParseException(je);
        }
        return request;
    }
    private Request getNoopRequest() {
        return new NoopAppRequest(null, 0, 0, Request.NO_OP,
                AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
    }
    private static AppRequest.PacketType[] types = {
            AppRequest.PacketType.DEFAULT_APP_REQUEST,
            AppRequest.PacketType.ANOTHER_APP_REQUEST };
    @Override
    public Set<IntegerPacketType> getRequestTypes() {

        return new HashSet<IntegerPacketType>(Arrays.asList(types));
    }

    @Override
    public boolean execute(Request request) {
        System.out.println("In execute request of Lazy Replication");
        if (request instanceof AppRequest) {
            String requestValue = ((AppRequest) request).getValue();
            try {
//                total += Integer.valueOf(requestValue);
                total += 1;
            } catch(NumberFormatException nfe) {
                nfe.printStackTrace();
            }
            System.out.println("Total changed to: "+ this.total);
            ((AppRequest) request).setResponse("total="+this.total);
        }
        else System.err.println("Unknown request type: " + request.getRequestType());
        return true;
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
