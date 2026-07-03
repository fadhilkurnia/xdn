package edu.umass.cs.xdn2.primarybackup.packets;

import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * ApplyStateDiffPacket carries a state diff captured by the primary, to be
 * applied by all replicas (including the primary itself, per the design
 * decision that the primary also applies its own diffs to snapshot/).
 *
 * Corresponds to "ApplyStateDiff{stateDiff, stateDiffCount, myID/primaryID,
 * currPlacementEpoch/primaryEpoch, currPlacement/placement}" in the
 * event/action protocol pseudocode.
 *
 * The stateDiff field may either be raw diff bytes (small diffs) or a
 * LargeCheckpointer handle string encoded as bytes (large diffs, e.g. the
 * 200MB MySQL bootstrap case) -- the distinction is not yet encoded as an
 * explicit field here; see TODO below.
 *
 * TODO: per the earlier design discussion, large diffs (e.g. >500KB bootstrap
 *  diffs) should route through LargeCheckpointer.createCheckpointHandle()
 *  instead of embedding raw bytes inline. Consider adding an explicit
 *  boolean isCheckpointHandle field rather than relying on runtime sniffing
 *  via LargeCheckpointer.isCheckpointHandle() on the stateDiff bytes.
 *
 * TODO: the placementEpoch field below is checked against currPlacementEpoch
 *  in the Notify(ApplyStateDiff) handler, but currPlacementEpoch is also the
 *  field name used by Notify(StartEpoch) for a DIFFERENT purpose (tracking
 *  the epoch of the current placement's primary). Confirm this is the
 *  intended field to compare against -- flagged during design discussion,
 *  not yet resolved.
 *
 * Wire format (plain Java, no protobuf):
 *   [4 bytes packetType][8 bytes packetId][4 bytes serviceName length]
 *   [serviceName bytes][4 bytes placement][4 bytes primaryEpoch]
 *   [4 bytes primaryID length][primaryID bytes][4 bytes stateDiffCount]
 *   [4 bytes stateDiff length][stateDiff bytes]
 */
public class ApplyStateDiffPacket extends PrimaryBackupPacket implements Byteable {

    private final long packetId;
    private final String serviceName;
    private final int placement;
    private final int primaryEpoch;
    private final String primaryID;
    private final int stateDiffCount;
    private final byte[] stateDiff;
    private final boolean isLargeDiff;

    public ApplyStateDiffPacket(String serviceName, int placement, int primaryEpoch,
                                String primaryID, int stateDiffCount, byte[] stateDiff) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName, placement, primaryEpoch, primaryID, stateDiffCount,
                stateDiff, false);
    }

    public ApplyStateDiffPacket(String serviceName, int placement, int primaryEpoch,
                                String primaryID, int stateDiffCount) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName, placement, primaryEpoch, primaryID, stateDiffCount,
                new byte[0], true);
    }

    private ApplyStateDiffPacket(long packetId, String serviceName, int placement,
                                 int primaryEpoch, String primaryID, int stateDiffCount,
                                 byte[] stateDiff, boolean isLargeDiff) {
        assert packetId > 0;
        assert serviceName != null;
        assert primaryID != null;
        assert stateDiff != null;

        this.packetId = packetId;
        this.serviceName = serviceName;
        this.placement = placement;
        this.primaryEpoch = primaryEpoch;
        this.primaryID = primaryID;
        this.stateDiffCount = stateDiffCount;
        this.stateDiff = stateDiff;
        this.isLargeDiff = isLargeDiff;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB2_APPLY_STATE_DIFF_PACKET;
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

    public int getPlacement() {
        return this.placement;
    }

    public int getPrimaryEpoch() {
        return this.primaryEpoch;
    }

    public String getPrimaryID() {
        return this.primaryID;
    }

    public int getStateDiffCount() {
        return this.stateDiffCount;
    }

    public byte[] getStateDiff() {
        return this.stateDiff;
    }

    public boolean isLargeDiff() {
        return this.isLargeDiff;
    }

    public String getDiffFilename() {
        return "p" + primaryEpoch + ":" + primaryID + ":" + stateDiffCount + ".diff";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplyStateDiffPacket that = (ApplyStateDiffPacket) o;
        return packetId == that.packetId &&
                placement == that.placement &&
                primaryEpoch == that.primaryEpoch &&
                stateDiffCount == that.stateDiffCount &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(primaryID, that.primaryID) &&
                Arrays.equals(stateDiff, that.stateDiff);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(packetId, serviceName, placement,
                primaryEpoch, primaryID, stateDiffCount);
        result = 31 * result + Arrays.hashCode(stateDiff);
        return result;
    }

    @Override
    public String toString() {
        return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
    }

    @Override
    public byte[] toBytes() {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteStream);

            out.writeInt(this.getRequestType().getInt());
            out.writeLong(this.packetId);

            byte[] serviceNameBytes = this.serviceName.getBytes(StandardCharsets.UTF_8);
            out.writeInt(serviceNameBytes.length);
            out.write(serviceNameBytes);

            out.writeInt(this.placement);
            out.writeInt(this.primaryEpoch);

            byte[] primaryIDBytes = this.primaryID.getBytes(StandardCharsets.UTF_8);
            out.writeInt(primaryIDBytes.length);
            out.write(primaryIDBytes);

            out.writeInt(this.stateDiffCount);
            out.writeBoolean(this.isLargeDiff);

            out.writeInt(this.stateDiff.length);
            out.write(this.stateDiff);

            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ApplyStateDiffPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length >= 4;

        ByteBuffer buffer = ByteBuffer.wrap(encodedPacket);
        int packetType = buffer.getInt();
        assert packetType == PrimaryBackupPacketType.PB2_APPLY_STATE_DIFF_PACKET.getInt()
                : "invalid packet header: " + packetType;

        long packetId = buffer.getLong();

        int serviceNameLen = buffer.getInt();
        byte[] serviceNameBytes = new byte[serviceNameLen];
        buffer.get(serviceNameBytes);
        String serviceName = new String(serviceNameBytes, StandardCharsets.UTF_8);

        int placement = buffer.getInt();
        int primaryEpoch = buffer.getInt();

        int primaryIDLen = buffer.getInt();
        byte[] primaryIDBytes = new byte[primaryIDLen];
        buffer.get(primaryIDBytes);
        String primaryID = new String(primaryIDBytes, StandardCharsets.UTF_8);

        int stateDiffCount = buffer.getInt();

        boolean isLargeDiff = buffer.get() != 0;

        int stateDiffLen = buffer.getInt();
        byte[] stateDiff = new byte[stateDiffLen];
        buffer.get(stateDiff);

        return new ApplyStateDiffPacket(packetId, serviceName, placement,
                primaryEpoch, primaryID, stateDiffCount, stateDiff, isLargeDiff);
    }
}