package edu.umass.cs.causal.packets;

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
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CausalReadAckPacket extends CausalPacket implements Byteable {

    private final long packetId;
    private final String serviceName;
    private final String senderNodeId;
    private final ClientRequest clientReadRequest;

    public CausalReadAckPacket(String serviceName,
                               String senderNodeId,
                               ClientRequest clientReadRequest) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName,
                senderNodeId,
                clientReadRequest);
    }

    private CausalReadAckPacket(long packetId,
                                 String serviceName,
                                 String senderNodeId,
                                 ClientRequest clientReadRequest) {
        assert packetId != 0 : "Invalid packet id";
        assert serviceName != null : "Service name cannot be null";
        assert senderNodeId != null : "Sender node id cannot be null";
        assert clientReadRequest != null : "Client read request cannot be null";
        assert (clientReadRequest instanceof BehavioralRequest br && br.isReadOnlyRequest()) :
                "Client request carried by CausalReadAckPacket must be read-only";
        this.packetId = packetId;
        this.serviceName = serviceName;
        this.senderNodeId = senderNodeId;
        this.clientReadRequest = clientReadRequest;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return CausalPacketType.CAUSAL_READ_ACK_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    public String getSenderNodeId() {
        return senderNodeId;
    }

    public ClientRequest getClientReadRequest() {
        return clientReadRequest;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public byte[] toBytes() {
        byte[] encodedRequest = this.clientReadRequest.toBytes();
        int payloadSize = CodedOutputStream.computeInt64Size(1, this.packetId)
                + CodedOutputStream.computeStringSize(2, this.serviceName)
                + CodedOutputStream.computeStringSize(3, this.senderNodeId)
                + CodedOutputStream.computeByteArraySize(4, encodedRequest);

        byte[] serialized = new byte[Integer.BYTES + payloadSize];
        ByteBuffer.wrap(serialized, 0, Integer.BYTES)
                .putInt(this.getRequestType().getInt());

        CodedOutputStream output = CodedOutputStream.newInstance(serialized,
                Integer.BYTES, payloadSize);
        try {
            output.writeInt64(1, this.packetId);
            output.writeString(2, this.serviceName);
            output.writeString(3, this.senderNodeId);
            output.writeByteArray(4, encodedRequest);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize CausalReadAckPacket", e);
        }

        return serialized;
    }

    @Override
    public String toString() {
        return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CausalReadAckPacket that)) return false;
        return packetId == that.packetId
                && Objects.equals(serviceName, that.serviceName)
                && Objects.equals(senderNodeId, that.senderNodeId)
                && Objects.equals(clientReadRequest, that.clientReadRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetId, serviceName, senderNodeId, clientReadRequest);
    }

    public static CausalReadAckPacket createFromBytes(byte[] encodedPacket,
                                                      AppRequestParser appRequestParser) {
        assert encodedPacket != null && encodedPacket.length >= Integer.BYTES
                : "Encoded packet cannot be empty";
        assert appRequestParser != null : "AppRequestParser cannot be null";

        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        if (packetType != CausalPacketType.CAUSAL_READ_ACK_PACKET.getInt()) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded CausalReadAckPacket: unexpected type " + packetType);
            return null;
        }

        long packetId = 0;
        String serviceName = null;
        String senderId = null;
        byte[] encodedRequest = null;

        CodedInputStream input = CodedInputStream.newInstance(
                encodedPacket, Integer.BYTES, encodedPacket.length - Integer.BYTES);
        try {
            int tag;
            while ((tag = input.readTag()) != 0) {
                switch (tag) {
                    case 8 -> packetId = input.readInt64();
                    case 18 -> serviceName = input.readStringRequireUtf8();
                    case 26 -> senderId = input.readStringRequireUtf8();
                    case 34 -> encodedRequest = input.readByteArray();
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded CausalReadAckPacket: " + e.getMessage());
            return null;
        }

        if (serviceName == null || senderId == null || encodedRequest == null) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded CausalReadAckPacket: missing fields");
            return null;
        }

        Request request;
        try {
            request = appRequestParser.getRequest(encodedRequest, null);
        } catch (RequestParseException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded CausalReadAckPacket: " + e.getMessage());
            return null;
        }

        assert (request instanceof ClientRequest)
                : "Payload inside CausalReadAckPacket must be ClientRequest";
        assert (request instanceof BehavioralRequest br && br.isReadOnlyRequest())
                : "Client request inside CausalReadAckPacket must be read-only";

        return new CausalReadAckPacket(packetId, serviceName, senderId, (ClientRequest) request);
    }
}
