package edu.umass.cs.eventual.packets;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LazyWriteAfterPacket is the best-effort fast path: after executing and acknowledging a client
 * write, the source replica broadcasts the encoded write, stamped with its per-source sequence
 * number, so peers can apply it eagerly. Peers apply a write-after only when it is contiguous
 * with their vector clock; gapped or duplicate packets are dropped and the periodic anti-entropy
 * checkpoint transfer recovers the missed writes.
 */
public class LazyWriteAfterPacket extends LazyPacket implements Byteable {

    private final long packetId;
    private final String senderId;
    private final long senderSeqNum;
    private final int epoch;
    private final ClientRequest clientRequest;   // parsed request (receive side / execution)
    private final byte[] encodedClientRequest;   // wire encoding (send side)
    private final String serviceName;

    public LazyWriteAfterPacket(String senderId, long senderSeqNum, int epoch,
                                ClientRequest clientRequest, byte[] encodedClientRequest) {
        this(UUID.randomUUID().getLeastSignificantBits(), senderId, senderSeqNum, epoch,
                clientRequest, encodedClientRequest);
    }

    private LazyWriteAfterPacket(long packetId, String senderId, long senderSeqNum, int epoch,
                                 ClientRequest clientRequest, byte[] encodedClientRequest) {
        assert packetId != 0 : "A likely invalid packetID given";
        assert senderId != null : "The sender cannot be null";
        assert clientRequest != null : "The provided ClientRequest cannot be null";
        this.packetId = packetId;
        this.senderId = senderId;
        this.senderSeqNum = senderSeqNum;
        this.epoch = epoch;
        this.clientRequest = clientRequest;
        this.encodedClientRequest = encodedClientRequest;
        this.serviceName = clientRequest.getServiceName();
    }

    @Override
    public IntegerPacketType getRequestType() {
        return LazyPacketType.LAZY_WRITE_AFTER;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public String getSenderId() {
        return senderId;
    }

    public long getSenderSeqNum() {
        return senderSeqNum;
    }

    public int getEpoch() {
        return epoch;
    }

    public ClientRequest getClientRequest() {
        return clientRequest;
    }

    @Override
    public byte[] toBytes() {
        byte[] encodedRequest = this.encodedClientRequest != null
                ? this.encodedClientRequest : this.clientRequest.toBytes();
        String serviceName = this.getServiceName();

        int payloadSize = CodedOutputStream.computeInt64Size(1, this.packetId)
                + CodedOutputStream.computeStringSize(2, this.senderId)
                + CodedOutputStream.computeStringSize(3, serviceName)
                + CodedOutputStream.computeByteArraySize(4, encodedRequest)
                + CodedOutputStream.computeInt64Size(5, this.senderSeqNum)
                + CodedOutputStream.computeInt32Size(6, this.epoch);

        byte[] serialized = new byte[Integer.BYTES + payloadSize];
        ByteBuffer.wrap(serialized, 0, Integer.BYTES).putInt(this.getRequestType().getInt());

        CodedOutputStream output = CodedOutputStream.newInstance(serialized, Integer.BYTES, payloadSize);
        try {
            output.writeInt64(1, this.packetId);
            output.writeString(2, this.senderId);
            output.writeString(3, serviceName);
            output.writeByteArray(4, encodedRequest);
            output.writeInt64(5, this.senderSeqNum);
            output.writeInt32(6, this.epoch);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize LazyWriteAfterPacket", e);
        }

        return serialized;
    }

    public static LazyWriteAfterPacket createFromBytes(byte[] encodedPacket,
                                                       AppRequestParser appRequestParser) {
        assert encodedPacket != null && encodedPacket.length > 0 : "Encoded packet cannot be empty";
        assert appRequestParser != null : "AppRequestParser cannot be null";

        if (encodedPacket.length < Integer.BYTES) {
            Logger.getGlobal().log(Level.SEVERE, "Receiving an invalid encoded LazyWriteAfterPacket: header too small");
            return null;
        }

        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        if (packetType != LazyPacketType.LAZY_WRITE_AFTER.getInt()) {
            Logger.getGlobal().log(Level.SEVERE, "Receiving an invalid encoded LazyWriteAfterPacket: unexpected type " + packetType);
            return null;
        }

        long packetId = 0;
        String senderId = null;
        String serviceName = null;
        byte[] encodedClientRequest = null;
        long senderSeqNum = 0;
        int epoch = 0;

        CodedInputStream input = CodedInputStream.newInstance(
                encodedPacket, Integer.BYTES, encodedPacket.length - Integer.BYTES);
        try {
            int tag;
            while ((tag = input.readTag()) != 0) {
                switch (tag) {
                    case 8 -> packetId = input.readInt64();
                    case 18 -> senderId = input.readStringRequireUtf8();
                    case 26 -> serviceName = input.readStringRequireUtf8();
                    case 34 -> encodedClientRequest = input.readByteArray();
                    case 40 -> senderSeqNum = input.readInt64();
                    case 48 -> epoch = input.readInt32();
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE, "Receiving an invalid encoded LazyWriteAfterPacket: " + e.getMessage());
            return null;
        }

        if (senderId == null || encodedClientRequest == null) {
            Logger.getGlobal().log(Level.SEVERE, "Receiving an invalid encoded LazyWriteAfterPacket: missing fields");
            return null;
        }

        Request clientRequest;
        try {
            clientRequest = appRequestParser.getRequest(encodedClientRequest, null);
        } catch (RequestParseException e) {
            Logger.getGlobal().log(Level.SEVERE, "Receiving an invalid encoded LazyWriteAfterPacket: " + e.getMessage());
            return null;
        }

        assert (clientRequest instanceof ClientRequest) :
                "The request inside LazyWriteAfterPacket must implement ClientRequest interface";

        ClientRequest concreteRequest = (ClientRequest) clientRequest;
        if (serviceName != null && !serviceName.equals(concreteRequest.getServiceName())) {
            Logger.getGlobal().log(Level.WARNING,
                    "Service name mismatch when decoding LazyWriteAfterPacket");
        }

        return new LazyWriteAfterPacket(packetId, senderId, senderSeqNum, epoch, concreteRequest,
                encodedClientRequest);
    }
}
