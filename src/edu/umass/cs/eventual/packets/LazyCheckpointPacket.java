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
 * LazyCheckpointPacket ships a frontier replica's complete service state to a lagging peer,
 * stamped with the vector clock at capture time. Small checkpoints travel inline
 * ({@link #MODE_INLINE}); large ones travel as an out-of-band handle ({@link #MODE_HANDLE},
 * e.g., a LargeCheckpointer handle redeemed by the receiver).
 */
public class LazyCheckpointPacket extends LazyPacket implements Byteable {

    public static final int MODE_INLINE = 1;
    public static final int MODE_HANDLE = 2;

    private final long packetId;
    private final String senderId;
    private final String serviceName;
    private final VectorTimestamp vectorClock;
    private final byte[] stateDigest; // nullable
    private final int epoch;
    private final int mode;
    private final byte[] checkpointData;   // non-null iff mode == MODE_INLINE
    private final String checkpointHandle; // non-null iff mode == MODE_HANDLE

    public static LazyCheckpointPacket createInline(String senderId, String serviceName,
                                                    VectorTimestamp vectorClock,
                                                    byte[] stateDigest, int epoch,
                                                    byte[] checkpointData) {
        return new LazyCheckpointPacket(UUID.randomUUID().getLeastSignificantBits(), senderId,
                serviceName, vectorClock, stateDigest, epoch, MODE_INLINE, checkpointData, null);
    }

    public static LazyCheckpointPacket createWithHandle(String senderId, String serviceName,
                                                        VectorTimestamp vectorClock,
                                                        byte[] stateDigest, int epoch,
                                                        String checkpointHandle) {
        return new LazyCheckpointPacket(UUID.randomUUID().getLeastSignificantBits(), senderId,
                serviceName, vectorClock, stateDigest, epoch, MODE_HANDLE, null, checkpointHandle);
    }

    private LazyCheckpointPacket(long packetId, String senderId, String serviceName,
                                 VectorTimestamp vectorClock, byte[] stateDigest, int epoch,
                                 int mode, byte[] checkpointData, String checkpointHandle) {
        assert senderId != null : "The sender cannot be null";
        assert serviceName != null : "The service name cannot be null";
        assert vectorClock != null : "The vector clock cannot be null";
        assert (mode == MODE_INLINE && checkpointData != null)
                || (mode == MODE_HANDLE && checkpointHandle != null)
                : "Checkpoint payload must match the declared mode";
        this.packetId = packetId;
        this.senderId = senderId;
        this.serviceName = serviceName;
        this.vectorClock = vectorClock;
        this.stateDigest = stateDigest;
        this.epoch = epoch;
        this.mode = mode;
        this.checkpointData = checkpointData;
        this.checkpointHandle = checkpointHandle;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return LazyPacketType.LAZY_CHECKPOINT;
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

    public int getMode() {
        return mode;
    }

    public byte[] getCheckpointData() {
        return checkpointData;
    }

    public String getCheckpointHandle() {
        return checkpointHandle;
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
                + CodedOutputStream.computeInt32Size(6, this.epoch)
                + CodedOutputStream.computeInt32Size(7, this.mode)
                + (this.checkpointData != null
                ? CodedOutputStream.computeByteArraySize(8, this.checkpointData) : 0)
                + (this.checkpointHandle != null
                ? CodedOutputStream.computeStringSize(9, this.checkpointHandle) : 0);

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
            output.writeInt32(7, this.mode);
            if (this.checkpointData != null) {
                output.writeByteArray(8, this.checkpointData);
            }
            if (this.checkpointHandle != null) {
                output.writeString(9, this.checkpointHandle);
            }
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize LazyCheckpointPacket", e);
        }

        return serialized;
    }

    public static LazyCheckpointPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0 : "Encoded packet cannot be empty";

        if (encodedPacket.length < Integer.BYTES) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazyCheckpointPacket: header too small");
            return null;
        }

        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        if (packetType != LazyPacketType.LAZY_CHECKPOINT.getInt()) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazyCheckpointPacket: unexpected type "
                            + packetType);
            return null;
        }

        long packetId = 0;
        String senderId = null;
        String serviceName = null;
        byte[] encodedVc = null;
        byte[] stateDigest = null;
        int epoch = 0;
        int mode = 0;
        byte[] checkpointData = null;
        String checkpointHandle = null;

        CodedInputStream input = CodedInputStream.newInstance(
                encodedPacket, Integer.BYTES, encodedPacket.length - Integer.BYTES);
        // Raise the default 64MB CodedInputStream limit so large inline checkpoints,
        // bounded separately by the NIO payload cap, still decode.
        input.setSizeLimit(Integer.MAX_VALUE);
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
                    case 56 -> mode = input.readInt32();
                    case 66 -> checkpointData = input.readByteArray();
                    case 74 -> checkpointHandle = input.readStringRequireUtf8();
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazyCheckpointPacket: " + e.getMessage());
            return null;
        }

        if (senderId == null || serviceName == null || encodedVc == null
                || (mode == MODE_INLINE && checkpointData == null)
                || (mode == MODE_HANDLE && checkpointHandle == null)) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazyCheckpointPacket: missing fields");
            return null;
        }

        VectorTimestamp vectorClock;
        try {
            vectorClock = VectorClockCodec.decode(encodedVc);
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded LazyCheckpointPacket: bad vector clock");
            return null;
        }

        return new LazyCheckpointPacket(packetId, senderId, serviceName, vectorClock, stateDigest,
                epoch, mode, checkpointData, checkpointHandle);
    }
}
