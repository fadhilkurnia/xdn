package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
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

public class CCApp implements Replicable {
    public String name = "CCApp";
    private HashMap<String, String> board = new HashMap<>();
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
//        System.out.println("In get request of app");
        CCRequestPacket CCRequestPacket = null;
        try {
            CCRequestPacket = new CCRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            ReconfigurationConfig.getLogger().info(
                    "Unable to parse request " + stringified);
            throw new RequestParseException(je);
        }
        return CCRequestPacket;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(CCRequestPacket.CCPacketType.values()));
    }

    @Override
    public boolean execute(Request request) {
        System.out.println("In execute request of MR Replication");
        if (request instanceof CCRequestPacket) {
//            Request will be of type(C for create and U for update): C CIRCLE01 1,1; C TRIANGLE02 2,4; U CIRCLE01 1,4
            if (((CCRequestPacket) request).getType() == CCRequestPacket.CCPacketType.MR_WRITE || ((CCRequestPacket) request).getType() == CCRequestPacket.CCPacketType.MW_WRITE) {
                String req = ((CCRequestPacket) request).getRequestValue();
                System.out.println("WRITE request "+req);
                String[] reqArray = req.split(" ");
                try {
                    if (reqArray[0].equals("U") & !this.board.containsKey(reqArray[1])) {
                        ((CCRequestPacket) request).setResponseValue("Update cannot be performed as the element does not exist");
                    }
                    else {
                        this.board.put(reqArray[1], reqArray[2]);
                        ((CCRequestPacket) request).setResponseValue("EXECUTED");
                    }
                }
                catch (Exception e){
                    ((CCRequestPacket) request).setResponseValue(e.toString());
                }
            }
            else if (((CCRequestPacket) request).getType() == CCRequestPacket.CCPacketType.MR_READ || ((CCRequestPacket) request).getType() == CCRequestPacket.CCPacketType.MW_READ){
                System.out.println("In MR App for READ request");
                ((CCRequestPacket) request).setResponseValue(this.board+"");
            }

            System.out.println("After execution: "+this.board.toString());
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
        System.out.println("In checkpoint");
        return this.board.toString();
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println("In restore");
        this.board = new HashMap<String, String>();
        if(state != null){
            try{
                for (String keyValuePair : state.split(", ")) {
                    String[] keyValue = keyValuePair.split("=");
                    this.board.put(keyValue[0], keyValue[1]);
                }
            }
            catch (Exception e){
                System.out.println("Exception");
                e.printStackTrace();
            }
        }
        return true;
    }
}

