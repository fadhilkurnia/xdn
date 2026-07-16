package edu.umass.cs.eventual.packets;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import edu.umass.cs.clientcentric.VectorTimestamp;
import edu.umass.cs.eventual.VectorClockCodec;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LazySyncPacket is the periodic anti-entropy heartbeat: it advertises the sender's
 * applied-write vector clock and an optional digest of its current state. A receiver uses it to
 * decide whether it is the frontier and must ship a checkpoint back to the sender.
 */
public class LazySyncPacket extends LazyPacket implements Byteable {

    private final long packetId;
    private final String senderId;
    private final String serviceName;
    private final VectorTimestamp vectorClock;
    private final byte[] stateDigest; // nullable
    private final int epoch;

    public LazySyncPacket(String senderId, String serviceName, VectorTimestamp vectorClock,
                          byte[] stateDigest, int epoch) {
        this(UUID.randomUUID().getLeastSignificantBits(), senderId, serviceName, vectorClock,
                stateDigest, epoch);
    }

    private LazySyncPacket(long packetId, String senderId, String serviceName,
                           VectorTimestamp vectorClock, byte[] stateDigest, int epoch) {
        assert senderId != null : "The sender cannot be null";
        assert serviceName != null : "The service name cannot be null";
        assert vectorClock != null : "The vector clock cannot be null";
        this.packetId = packetId;
        this.senderId = senderId;
        this.serviceName = serviceName;
        this.vectorClock = vectorClock;
        this.stateDigest = stateDigest;
        this.epoch = epoch;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return LazyPacketType.LAZY_SYNC;
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

    public VectorTimestamp getVectorClock() {
        return vectorClock;
    }

    public byte[] getStateDigest() {
        return stateDigest;
    }

    public int getEpoch() {
        return epoch;
    }

    @Override
    public byte[] toBytes() {
        byte[] encodedVc = VectorClockCodec.encode(this.vectorClock);

        int payloadSize = CodedOutputStream.computeInt64Size(1, this.packetId)
                + CodedOutputStream.computeStringSize(2, this.senderId)
                + CodedOutputStream.computeStringSize(3, this.serviceName)
                + CodedOutputStream.computeByteArraySize(4, encodedVc)
                + (this.stateDigest != null
                ? CodedOutputStream.computeByteArraySize(5, this.stateDigest) : 0)
                + CodedOutputStream.computeInt32Size(6, this.epoch);

        byte[] serialized = new byte[Integer.BYTES + payloadSize];
        ByteBuffer.wrap(serialized, 0, Integer.BYTES).putInt(this.getRequestType().getInt());

        CodedOutputStream output =
                CodedOutputStream.newInstance(serialized, Integer.BYTES, payloadSize);
        try {
            output.writeInt64(1, this.packetId);
            output.writeString(2, this.senderId);
            output.writeString(3, this.serviceName);
            output.writeByteArray(4, encodedVc);
            if (this.stateDigest != null) {
                output.writeByteArray(5, this.stateDigest);
            }
            output.writeInt32(6, this.epoch);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize LazySyncPacket", e);
        }

        return serialized;
    }

    public static LazySyncPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0 : "Encoded packet cannot be empty";

        if (encodedPacket.length < Integer.BYTES) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazySyncPacket: header too small");
            return null;
        }

        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        if (packetType != LazyPacketType.LAZY_SYNC.getInt()) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazySyncPacket: unexpected type " + packetType);
            return null;
        }

        long packetId = 0;
        String senderId = null;
        String serviceName = null;
        byte[] encodedVc = null;
        byte[] stateDigest = null;
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
                    case 34 -> encodedVc = input.readByteArray();
                    case 42 -> stateDigest = input.readByteArray();
                    case 48 -> epoch = input.readInt32();
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazySyncPacket: " + e.getMessage());
            return null;
        }

        if (senderId == null || serviceName == null || encodedVc == null) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazySyncPacket: missing fields");
            return null;
        }

        VectorTimestamp vectorClock;
        try {
            vectorClock = VectorClockCodec.decode(encodedVc);
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazySyncPacket: bad vector clock");
            return null;
        }

        return new LazySyncPacket(packetId, senderId, serviceName, vectorClock, stateDigest,
                epoch);
    }
}
