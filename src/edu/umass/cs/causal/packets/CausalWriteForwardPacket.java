package edu.umass.cs.causal.packets;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.umass.cs.causal.dag.VectorTimestamp;
import edu.umass.cs.causal.proto.CausalPacketProto;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CausalWriteForwardPacket extends CausalPacket implements Byteable {

    private final long packetId;
    private final String serviceName;
    private final String senderId;
    private final List<VectorTimestamp> dependencies;
    private final VectorTimestamp requestTimestamp;
    private final ClientRequest clientWriteOnlyRequest;

    public CausalWriteForwardPacket(String serviceName,
                                    String senderId,
                                    List<VectorTimestamp> dependencies,
                                    VectorTimestamp requestTimestamp,
                                    ClientRequest clientWriteOnlyRequest) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName,
                senderId,
                dependencies,
                requestTimestamp,
                clientWriteOnlyRequest);
    }

    private CausalWriteForwardPacket(long packetId,
                                     String serviceName,
                                     String senderId,
                                     List<VectorTimestamp> dependencies,
                                     VectorTimestamp requestTimestamp,
                                     ClientRequest clientWriteOnlyRequest) {
        this.packetId = packetId;
        this.serviceName = serviceName;
        this.senderId = senderId;
        this.dependencies = dependencies;
        this.requestTimestamp = requestTimestamp;
        this.clientWriteOnlyRequest = clientWriteOnlyRequest;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return CausalPacketType.CAUSAL_WRITE_FORWARD_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    public String getSenderId() {
        return senderId;
    }

    public List<VectorTimestamp> getDependencies() {
        return dependencies;
    }

    @Override
    public byte[] toBytes() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Serialize the packet type
        int packetType = this.getRequestType().getInt();
        byte[] encodedHeader = ByteBuffer.allocate(4).putInt(packetType).array();

        // Serialize the dependencies
        List<CausalPacketProto.VectorTimestamp> dependenciesProto = new ArrayList<>();
        for (VectorTimestamp ts : this.dependencies) {
            CausalPacketProto.VectorTimestamp.Builder timestampBuilder =
                    CausalPacketProto.VectorTimestamp.newBuilder();
            for (String nodeId : ts.getNodeIds()) {
                long counter = ts.getNodeTimestamp(nodeId);
                CausalPacketProto.VectorTimestamp.NodeCounterPair pair =
                        CausalPacketProto.VectorTimestamp.NodeCounterPair.newBuilder()
                                .setNodeId(nodeId)
                                .setCounter(counter)
                                .build();
                timestampBuilder.addComponents(pair);
            }
            dependenciesProto.add(timestampBuilder.build());
        }

        // Serialize the timestamp
        CausalPacketProto.VectorTimestamp.Builder timestampBuilder =
                CausalPacketProto.VectorTimestamp.newBuilder();
        for (String nodeId : this.requestTimestamp.getNodeIds()) {
            long counter = this.requestTimestamp.getNodeTimestamp(nodeId);
            CausalPacketProto.VectorTimestamp.NodeCounterPair pair =
                    CausalPacketProto.VectorTimestamp.NodeCounterPair.newBuilder()
                            .setNodeId(nodeId)
                            .setCounter(counter)
                            .build();
            timestampBuilder.addComponents(pair);
        }

        // Prepare the packet proto builder
        CausalPacketProto.CausalWriteForwardPacket.Builder protoBuilder =
                CausalPacketProto.CausalWriteForwardPacket.newBuilder()
                        .setPacketId(this.packetId)
                        .setServiceName(this.serviceName)
                        .setSenderNodeId(this.senderId)
                        .addAllDependencies(dependenciesProto)
                        .setTimestamp(timestampBuilder.build())
                        .setEncodedWriteRequest(
                                ByteString.copyFrom(this.clientWriteOnlyRequest.toBytes()));

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    public VectorTimestamp getRequestTimestamp() {
        return requestTimestamp;
    }

    public ClientRequest getClientWriteOnlyRequest() {
        return clientWriteOnlyRequest;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CausalWriteForwardPacket that = (CausalWriteForwardPacket) o;
        return packetId == that.packetId &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(senderId, that.senderId) &&
                Objects.equals(requestTimestamp, that.requestTimestamp) &&
                Objects.equals(clientWriteOnlyRequest, that.clientWriteOnlyRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetId, serviceName, senderId,
                requestTimestamp, clientWriteOnlyRequest);
    }

    @Override
    public String toString() {
        return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
    }

    public static CausalWriteForwardPacket createFromBytes(byte[] encodedPacket,
                                                           AppRequestParser clientRequestParser) {
        assert encodedPacket != null && encodedPacket.length > 0;
        assert clientRequestParser != null;

        // Decode the packet type
        assert encodedPacket.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        assert packetType == CausalPacketType.CAUSAL_WRITE_FORWARD_PACKET.getInt()
                : "invalid packet header: " + packetType;
        byte[] encoded = encodedPacket;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        CausalPacketProto.CausalWriteForwardPacket decodedProto = null;
        try {
            decodedProto = CausalPacketProto.CausalWriteForwardPacket.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        // Decode the vector timestamp
        List<String> nodeIds = new ArrayList<>();
        List<Long> counters = new ArrayList<>();
        for (CausalPacketProto.VectorTimestamp.NodeCounterPair pair :
                decodedProto.getTimestamp().getComponentsList()) {
            nodeIds.add(pair.getNodeId());
            counters.add(pair.getCounter());
        }
        VectorTimestamp timestamp = new VectorTimestamp(nodeIds);
        for (int i = 0; i < nodeIds.size(); i++) {
            timestamp.updateNodeTimestamp(nodeIds.get(i), counters.get(i));
        }

        // Decode the dependencies
        List<VectorTimestamp> dependencies = new ArrayList<>();
        for (CausalPacketProto.VectorTimestamp ts : decodedProto.getDependenciesList()) {
            VectorTimestamp newDagDepsTimestamp = new VectorTimestamp(nodeIds);
            for (CausalPacketProto.VectorTimestamp.NodeCounterPair pair :
                    ts.getComponentsList()) {
                newDagDepsTimestamp.updateNodeTimestamp(pair.getNodeId(), pair.getCounter());
            }
            dependencies.add(newDagDepsTimestamp);
        }

        // Decode the clientRequest
        ClientRequest clientWriteOnlyRequest;
        try {
            Request request = clientRequestParser.getRequest(
                    decodedProto.getEncodedWriteRequest().toByteArray(), null);
            assert request instanceof ClientRequest :
                    "Expecting ClientRequest but found " + request.getRequestType().toString();
            clientWriteOnlyRequest = (ClientRequest) request;
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }

        return new CausalWriteForwardPacket(
                decodedProto.getPacketId(),
                decodedProto.getServiceName(),
                decodedProto.getSenderNodeId(),
                dependencies,
                timestamp,
                clientWriteOnlyRequest);
    }
}
