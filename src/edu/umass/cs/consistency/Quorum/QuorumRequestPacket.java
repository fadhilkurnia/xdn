package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.chainreplication.chainpackets.ChainPacket;
import edu.umass.cs.chainreplication.chainpackets.ChainRequestPacket;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.linwrites.SimpleAppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.UUID;

public class QuorumRequestPacket extends JSONPacket implements ReplicableRequest, ClientRequest  {
    public final long requestID;
    private String responseValue = null;
    private String requestValue = null;
    private int destination = -1;
    private int source = -1;
    private InetSocketAddress clientSocketAddress = null;
    private QuorumPacketType packetType;
    private String quorumID = null;
    private int version = -1;
    private int slot = 0;

    @Override
    public boolean needsCoordination() {
        return true;
    }

    /**
     * To avoid potential conflict with existing {@link PaxosPacket} PaxosPacketType and {@Link ChainPacket} ChainPacketType,
     * ChainPacketType starts from 1200 to 1300
     */
    public enum QuorumPacketType implements IntegerPacketType {

        REQUEST("QUORUM_REQ", 1201),
        READ("QUORUM_READ", 1202),
        READFORWRITE("QUORUM_READ_FOR_WRITE", 1203),
        WRITE("QUORUM_WRITE", 1204),
        READFORWARD("QUORUM_READ_FWD", 1205),
        READFORWRITEFORWARD("QUORUM_READ_FOR_WRITE_FWD", 1206),
        WRITEFORWARD("QUORUM_WRITE_FWD", 1207),
        READACK("QUORUM_READ_ACK", 1208),
        READFORWRITEACK("QUORUM_READ_FOR_WRITE_ACK", 1209),
        WRITEACK("QUORUM_WRITE_ACK", 1210),
        RESPONSE("RESPONSE", 12011),

        QUORUM_PACKET("QUORUM_PACKET", 1299),
        ;
        private final String label;
        private final int number;
        private static HashMap<String, QuorumPacketType> labels = new HashMap<String, QuorumPacketType>();
        private static HashMap<Integer, QuorumPacketType> numbers = new HashMap<Integer, QuorumPacketType>();
        QuorumPacketType(String s, int t) {
            this.label = s;
            this.number = t;
        }
        static {
            for (QuorumPacketType type: QuorumPacketType.values()) {
                if (!QuorumPacketType.labels.containsKey(type.label)
                        && !QuorumPacketType.numbers.containsKey(type.number)) {
                    QuorumPacketType.labels.put(type.label, type);
                    QuorumPacketType.numbers.put(type.number, type);
                } else {
                    assert(false): "Duplicate or inconsistent enum type for ChainPacketType";
                }
            }
        }
        @Override
        public int getInt() {
            return this.number;
        }
        public String getLabel(){
            return this.label;
        }
        public static QuorumPacketType getQuorumPacket(int type){
            return QuorumPacketType.numbers.get(type);
        }
    }
    public QuorumRequestPacket(long reqID, QuorumPacketType reqType,
                               QuorumRequestPacket req){
        super(reqType);

        this.packetType = reqType;
        this.requestID = reqID;
        if(req == null)
            return;
        this.responseValue = req.responseValue;
        this.requestValue = req.requestValue;
        this.quorumID = req.quorumID;
        this.destination = req.destination;
        this.source = req.source;
        this.clientSocketAddress  = req.clientSocketAddress;
    }
    public QuorumRequestPacket(JSONObject json) throws JSONException{
        super(json);
        System.out.println("In quorum request packet constructor");
        System.out.println(json);
        this.packetType = QuorumPacketType.getQuorumPacket(json.getInt("type"));
        this.requestID = json.getLong("requestID");
        this.requestValue = json.getString("requestValue");
        this.responseValue = json.getString("responseValue");
        this.source = json.getInt("source");
        this.destination = json.getInt("destination");
        this.slot = json.getInt("slot");
        this.version = json.getInt("version");
        this.quorumID = json.getString("quorumID");
    }

    public QuorumRequestPacket(long reqID, String value,
                               QuorumPacketType reqType, String quorumID){
        super(reqType);
        this.packetType = reqType;
        this.requestID = reqID;
        this.requestValue = value;
        this.quorumID = quorumID;
    }

    public void setQuorumID(String quorumID){
        this.quorumID = quorumID;
    }
    public void setVersion(Integer version){
        this.version = version;
    }
    public int getVersion() {
        return version;
    }

    public String getQuorumID() {
        return quorumID;
    }

    public String getResponseValue() {
        return responseValue;
    }

    public void setResponseValue(String responseValue) {
        this.responseValue = responseValue;
    }

    public String getRequestValue() {
        return requestValue;
    }

    public void setRequestValue(String requestValue) {
        this.requestValue = requestValue;
    }

    public void setPacketType(QuorumPacketType packetType){
        this.packetType = packetType;
    }
    @Override
    public InetSocketAddress getClientAddress() {
        return this.clientSocketAddress;
    }

    public void setDestination(int destination) {
        this.destination = destination;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public int getDestination() {
        return destination;
    }

    public int getSource() {
        return source;
    }

    @Override
    public ClientRequest getResponse() {
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! getResponse is called!!!!!!!!!!!!");

        QuorumRequestPacket reply = new QuorumRequestPacket(this.requestID,
                QuorumPacketType.RESPONSE, this);
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Response value is "+responseValue+"!!!!!!!!!!!!");
        reply.responseValue = this.responseValue;
        return reply;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return this.getType();
    }
    public QuorumPacketType getType() {
        return this.packetType;
    }

    @Override
    public String getServiceName() {
        return this.quorumID;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        System.out.println("In json implementation");
        JSONObject json = new JSONObject();
//        convert this in enums
        json.put("quorumID", this.quorumID);
        json.put("type", this.packetType.getInt());
        json.put("requestValue", this.requestValue);
        json.put("packetType", this.packetType);
        json.put("version", this.version);
        json.put("slot", this.slot);
        json.put("requestID", this.requestID);
        json.putOpt("responseValue", this.responseValue);
        json.put("clientSocketAddress", this.clientSocketAddress);
        json.put("destination", this.destination);
        json.put("source", this.source);

        return json;
    }


    @Override
    public String toString() {
        try {
            return this.toJSONObjectImpl().toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] toBytes() {
        System.out.println("converting to bytes");
        //to be implemented
        return null;
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }
}
