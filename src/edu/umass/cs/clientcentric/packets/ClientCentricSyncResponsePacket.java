package edu.umass.cs.clientcentric.packets;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientCentricSyncResponsePacket extends ClientCentricPacket implements Byteable {

    private final long packetId;
    private final String senderId;
    private final String serviceName;
    private final long fromSequenceNumber;
    private final List<byte[]> encodedRequests;

    public ClientCentricSyncResponsePacket(String senderId, String serviceName,
                                           long fromSequenceNumber,
                                           List<byte[]> encodedRequests) {
        this(System.currentTimeMillis(), senderId, serviceName, fromSequenceNumber, encodedRequests);
    }

    private ClientCentricSyncResponsePacket(long packetId, String senderId, String serviceName,
                                            long fromSequenceNumber,
                                            List<byte[]> encodedRequests) {
        assert senderId != null && !senderId.isEmpty() : "Invalid sender id";
        assert serviceName != null && !serviceName.isEmpty() : "Invalid service name";
        assert fromSequenceNumber >= 0 : "Invalid sequence number, must be >= 0";
        this.packetId = packetId;
        this.senderId = senderId;
        this.serviceName = serviceName;
        this.fromSequenceNumber = fromSequenceNumber;
        this.encodedRequests = encodedRequests == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(encodedRequests));
    }

    @Override
    public IntegerPacketType getRequestType() {
        return ClientCentricPacketType.CLIENT_CENTRIC_SYNC_RES_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    public String getSenderId() {
        return senderId;
    }

    public long getStartingSequenceNumber() {
        return fromSequenceNumber;
    }

    public List<byte[]> getEncodedRequests() {
        return encodedRequests;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public byte[] toBytes() {
        int payloadSize = CodedOutputStream.computeInt64Size(1, this.packetId)
                + CodedOutputStream.computeStringSize(2, this.senderId)
                + CodedOutputStream.computeStringSize(3, this.serviceName)
                + CodedOutputStream.computeInt64Size(4, this.fromSequenceNumber);
        for (byte[] req : this.encodedRequests) {
            payloadSize += CodedOutputStream.computeByteArraySize(5, req);
        }

        byte[] serialized = new byte[Integer.BYTES + payloadSize];
        ByteBuffer.wrap(serialized, 0, Integer.BYTES)
                .putInt(this.getRequestType().getInt());

        CodedOutputStream output = CodedOutputStream.newInstance(serialized,
                Integer.BYTES, payloadSize);
        try {
            output.writeInt64(1, this.packetId);
            output.writeString(2, this.senderId);
            output.writeString(3, this.serviceName);
            output.writeInt64(4, this.fromSequenceNumber);
            for (byte[] req : this.encodedRequests) {
                output.writeByteArray(5, req);
            }
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ClientCentricSyncResponsePacket", e);
        }

        return serialized;
    }

    public static ClientCentricSyncResponsePacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length >= Integer.BYTES
                : "Encoded packet cannot be empty";

        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        if (packetType != ClientCentricPacketType.CLIENT_CENTRIC_SYNC_RES_PACKET.getInt()) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded ClientCentricSyncResponsePacket: unexpected type "
                            + packetType);
            return null;
        }

        long packetId = 0;
        String senderId = null;
        String serviceName = null;
        Long fromSequenceNumber = null;
        List<byte[]> requests = new ArrayList<>();

        CodedInputStream input = CodedInputStream.newInstance(
                encodedPacket, Integer.BYTES, encodedPacket.length - Integer.BYTES);
        try {
            int tag;
            while ((tag = input.readTag()) != 0) {
                switch (tag) {
                    case 8 -> packetId = input.readInt64();
                    case 18 -> senderId = input.readStringRequireUtf8();
                    case 26 -> serviceName = input.readStringRequireUtf8();
                    case 32 -> fromSequenceNumber = input.readInt64();
                    case 42 -> requests.add(input.readByteArray());
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded ClientCentricSyncResponsePacket: " + e.getMessage());
            return null;
        }

        if (senderId == null || serviceName == null || fromSequenceNumber == null) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded ClientCentricSyncResponsePacket: missing fields");
            return null;
        }

        return new ClientCentricSyncResponsePacket(packetId, senderId, serviceName,
                fromSequenceNumber, requests);
    }
}
