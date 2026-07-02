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

/**
 * ResponsePacket is sent directly (not via Paxos) from the primary back to
 * the backup that forwarded a client request, carrying the computed response.
 *
 * Wire format:
 *   [4 bytes packetType][8 bytes requestId]
 *   [4 bytes serviceName length][serviceName bytes]
 *   [4 bytes encodedResponse length][encodedResponse bytes]
 */
public class ResponsePacket extends PrimaryBackupPacket implements Byteable {

    private final long requestId;
    private final String serviceName;
    private final byte[] encodedResponse;

    public ResponsePacket(String serviceName, long requestId, byte[] encodedResponse) {
        assert serviceName != null;
        assert encodedResponse != null;

        this.serviceName = serviceName;
        this.requestId = requestId;
        this.encodedResponse = encodedResponse;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB2_RESPONSE_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.requestId;
    }

    @Override
    public boolean needsCoordination() {
        return false;
    }

    public byte[] getEncodedResponse() {
        return this.encodedResponse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponsePacket that = (ResponsePacket) o;
        return requestId == that.requestId &&
                Objects.equals(serviceName, that.serviceName) &&
                Arrays.equals(encodedResponse, that.encodedResponse);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(requestId, serviceName);
        result = 31 * result + Arrays.hashCode(encodedResponse);
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
            out.writeLong(this.requestId);

            byte[] serviceNameBytes = this.serviceName.getBytes(StandardCharsets.UTF_8);
            out.writeInt(serviceNameBytes.length);
            out.write(serviceNameBytes);

            out.writeInt(this.encodedResponse.length);
            out.write(this.encodedResponse);

            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ResponsePacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length >= 4;

        ByteBuffer buffer = ByteBuffer.wrap(encodedPacket);
        int packetType = buffer.getInt();
        assert packetType == PrimaryBackupPacketType.PB2_RESPONSE_PACKET.getInt()
                : "invalid packet header: " + packetType;

        long requestId = buffer.getLong();

        int serviceNameLen = buffer.getInt();
        byte[] serviceNameBytes = new byte[serviceNameLen];
        buffer.get(serviceNameBytes);
        String serviceName = new String(serviceNameBytes, StandardCharsets.UTF_8);

        int encodedResponseLen = buffer.getInt();
        byte[] encodedResponse = new byte[encodedResponseLen];
        buffer.get(encodedResponse);

        return new ResponsePacket(serviceName, requestId, encodedResponse);
    }
}