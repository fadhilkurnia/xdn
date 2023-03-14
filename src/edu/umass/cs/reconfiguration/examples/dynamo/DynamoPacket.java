package edu.umass.cs.reconfiguration.examples.dynamo;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

public class DynamoPacket implements Request, ReplicableRequest, ReconfigurableRequest, ClientRequest {

    /**
     * Packet type class for DynamoPacket.
     */
    public enum PacketType implements IntegerPacketType {
        /**
         * FORWARD_WRITE_REQUEST wrap a client write request for
         * broadcast to all the active replicas.
         */
        FORWARD_WRITE_REQUEST(901),

        /**
         * FORWARD_WRITE_RESPONSE is the response of FORWARD_WRITE_REQUEST
         * from an active replica which receives the broadcast back to the
         * broadcaster (sender).
         */
        FORWARD_WRITE_RESPONSE(902),

        /**
         * FORWARD_READ_REQUEST wrap a client read request for
         * broadcast to all the active replicas.
         */
        FORWARD_READ_REQUEST(903),

        /**
         * FORWARD_READ_RESPONSE is the response of FORWARD_READ_REQUEST
         * from an active replica which receives the broadcast back to the
         * broadcaster (sender).
         */
        FORWARD_READ_RESPONSE(904),

        STOP(909);

        private final int number;

        PacketType(int i) {
            this.number = i;
        }

        @Override
        public int getInt() {
            return this.number;
        }
    }

    public class Payload {
        public String key;
        public String value;
    }

    private final PacketType type;
    private final String name;
    private final int epoch;
    private final long id;

    private final int senderID;
    private AppRequest appRequest;

    public Payload payload;

    public DynamoPacket(int senderID, PacketType type, AppRequest appRequest, Integer epoch) {
        this.name = appRequest.getServiceName();
        this.epoch = epoch;
        this.id = appRequest.getRequestID();
        this.type = type;
        this.appRequest = appRequest;
        this.senderID = senderID;
        this.payload = new Payload();
    }

    public static DynamoPacket getStopPacket(String name, int epoch) {
        return new DynamoPacket(
                0,
                PacketType.STOP,
                new AppRequest(name, "stop-packet", AppRequest.PacketType.ADMIN_APP_REQUEST, true),
                epoch
        );

    }

    @Override
    public IntegerPacketType getRequestType() {
        return this.type;
    }

    @Override
    public String getServiceName() {
        return this.name;
    }

    @Override
    public long getRequestID() {
        return this.id;
    }

    @Override
    public int getEpochNumber() {
        return this.epoch;
    }

    @Override
    public boolean isStop() {
        return false;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public AppRequest getAppRequest() {
        return this.appRequest;
    }

    public Integer getSenderID() {
        return this.senderID;
    }

    private ClientRequest response;

    @Override
    public ClientRequest getResponse() {
        return this.response;
    }

    public void setResponse(ClientRequest response) {
        this.response = response;
    }
}
