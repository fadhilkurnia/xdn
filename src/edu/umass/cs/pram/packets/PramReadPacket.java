package edu.umass.cs.pram.packets;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PramReadPacket extends PramPacket implements Byteable {

    private final long requestID;
    private final ClientRequest clientReadOnlyRequest;

    public PramReadPacket(ClientRequest readOnlyRequest) {
        this(System.currentTimeMillis(), readOnlyRequest);
    }

    private PramReadPacket(long requestID, ClientRequest readOnlyRequest) {
        assert readOnlyRequest != null : "The provided request cannot be null";
        assert (readOnlyRequest instanceof BehavioralRequest r && r.isReadOnlyRequest()) :
                "The provided request must be a ReadOnlyRequest";
        this.clientReadOnlyRequest = readOnlyRequest;
        this.requestID = requestID;
    }

    public ClientRequest getClientReadRequest() {
        return clientReadOnlyRequest;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PramPacketType.PRAM_READ_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.clientReadOnlyRequest.getServiceName();
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public byte[] toBytes() {
        byte[] encodedRequest = this.clientReadOnlyRequest.toBytes();
        int payloadSize = CodedOutputStream.computeInt64Size(1, this.requestID)
                + CodedOutputStream.computeByteArraySize(2, encodedRequest);

        byte[] serialized = new byte[Integer.BYTES + payloadSize];
        ByteBuffer.wrap(serialized, 0, Integer.BYTES)
                .putInt(this.getRequestType().getInt());

        CodedOutputStream output = CodedOutputStream.newInstance(serialized,
                Integer.BYTES, payloadSize);
        try {
            output.writeInt64(1, this.requestID);
            output.writeByteArray(2, encodedRequest);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize PramReadPacket", e);
        }

        return serialized;
    }

    public static PramReadPacket createFromBytes(byte[] encodedPacket, AppRequestParser appRequestParser) {
        assert encodedPacket != null && encodedPacket.length >= Integer.BYTES
                : "Encoded packet cannot be empty";
        assert appRequestParser != null : "AppRequestParser cannot be null";

        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        if (packetType != PramPacketType.PRAM_READ_PACKET.getInt()) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded PramReadPacket: unexpected type " + packetType);
            return null;
        }

        long requestId = 0;
        byte[] encodedRequest = null;

        CodedInputStream input = CodedInputStream.newInstance(
                encodedPacket, Integer.BYTES, encodedPacket.length - Integer.BYTES);
        try {
            int tag;
            while ((tag = input.readTag()) != 0) {
                switch (tag) {
                    case 8 -> requestId = input.readInt64();
                    case 18 -> encodedRequest = input.readByteArray();
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded PramReadPacket: " + e.getMessage());
            return null;
        }

        if (encodedRequest == null) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded PramReadPacket: missing request");
            return null;
        }

        Request clientRequest;
        try {
            clientRequest = appRequestParser.getRequest(encodedRequest, null);
        } catch (RequestParseException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded PramReadPacket: " + e.getMessage());
            return null;
        }

        assert (clientRequest instanceof ClientRequest)
                : "The request inside PramPacket must implement ClientRequest interface";
        assert (clientRequest instanceof BehavioralRequest r && r.isReadOnlyRequest())
                : "The client request inside PramReadPacket must be ReadOnlyRequest";

        return new PramReadPacket(requestId, (ClientRequest) clientRequest);
    }
}
