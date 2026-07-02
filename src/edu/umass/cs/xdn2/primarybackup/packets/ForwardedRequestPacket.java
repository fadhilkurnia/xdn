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
 * ForwardedRequestPacket is sent directly (not via Paxos) from a backup node
 * to the primary, carrying a client request that the backup cannot execute
 * locally.
 *
 * Wire format:
 *   [4 bytes packetType][8 bytes packetId]
 *   [4 bytes serviceName length][serviceName bytes]
 *   [4 bytes entryNodeId length][entryNodeId bytes]
 *   [4 bytes encodedRequest length][encodedRequest bytes]
 */
public class ForwardedRequestPacket extends PrimaryBackupPacket implements Byteable {

    private final long packetId;
    private final String serviceName;
    private final String entryNodeId;
    private final byte[] encodedForwardedRequest;

    public ForwardedRequestPacket(String serviceName, String entryNodeId,
                                  byte[] encodedForwardedRequest) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName, entryNodeId, encodedForwardedRequest);
    }

    private ForwardedRequestPacket(long packetId, String serviceName,
                                   String entryNodeId, byte[] encodedForwardedRequest) {
        assert packetId > 0;
        assert serviceName != null;
        assert entryNodeId != null;
        assert encodedForwardedRequest != null;

        this.packetId = packetId;
        this.serviceName = serviceName;
        this.entryNodeId = entryNodeId;
        this.encodedForwardedRequest = encodedForwardedRequest;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB2_FORWARDED_REQUEST_PACKET;
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
        return false;
    }

    public String getEntryNodeId() {
        return this.entryNodeId;
    }

    public byte[] getEncodedForwardedRequest() {
        return this.encodedForwardedRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForwardedRequestPacket that = (ForwardedRequestPacket) o;
        return packetId == that.packetId &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(entryNodeId, that.entryNodeId) &&
                Arrays.equals(encodedForwardedRequest, that.encodedForwardedRequest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(packetId, serviceName, entryNodeId);
        result = 31 * result + Arrays.hashCode(encodedForwardedRequest);
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

            byte[] entryNodeIdBytes = this.entryNodeId.getBytes(StandardCharsets.UTF_8);
            out.writeInt(entryNodeIdBytes.length);
            out.write(entryNodeIdBytes);

            out.writeInt(this.encodedForwardedRequest.length);
            out.write(this.encodedForwardedRequest);

            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ForwardedRequestPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length >= 4;

        ByteBuffer buffer = ByteBuffer.wrap(encodedPacket);
        int packetType = buffer.getInt();
        assert packetType == PrimaryBackupPacketType.PB2_FORWARDED_REQUEST_PACKET.getInt()
                : "invalid packet header: " + packetType;

        long packetId = buffer.getLong();

        int serviceNameLen = buffer.getInt();
        byte[] serviceNameBytes = new byte[serviceNameLen];
        buffer.get(serviceNameBytes);
        String serviceName = new String(serviceNameBytes, StandardCharsets.UTF_8);

        int entryNodeIdLen = buffer.getInt();
        byte[] entryNodeIdBytes = new byte[entryNodeIdLen];
        buffer.get(entryNodeIdBytes);
        String entryNodeId = new String(entryNodeIdBytes, StandardCharsets.UTF_8);

        int encodedRequestLen = buffer.getInt();
        byte[] encodedRequest = new byte[encodedRequestLen];
        buffer.get(encodedRequest);

        return new ForwardedRequestPacket(packetId, serviceName, entryNodeId, encodedRequest);
    }
}