package edu.umass.cs.causal.packets;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.umass.cs.causal.dag.VectorTimestamp;
import edu.umass.cs.causal.proto.CausalPacketProto;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CausalWriteAckPacket extends CausalPacket implements Byteable {

    private final long packetId;
    private final String serviceName;
    private final String senderId;
    private final VectorTimestamp confirmedGraphNodeId;

    // TODO: Existing leaf nodes' ID in the sender so the receiver
    //   can update its database.
    private final List<VectorTimestamp> dagLeafNodeIds;

    // TODO: investigate whether we can replace the confirmedRequestId with VectorTimestamp
    //  as both are unique identifier for a request (or GraphNode).
    //  If so, we can just sent the index of the confirmed VectorTimestamp according to the
    //  dagLeafNodeIds List.

    public CausalWriteAckPacket(String serviceName, String senderId,
                                VectorTimestamp confirmedGraphNodeId,
                                List<VectorTimestamp> dagLeafNodeIds) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName,
                senderId,
                confirmedGraphNodeId,
                dagLeafNodeIds);
    }

    private CausalWriteAckPacket(long packetId, String serviceName, String senderId,
                                 VectorTimestamp confirmedGraphNodeId,
                                 List<VectorTimestamp> dagLeafNodeIds) {
        this.packetId = packetId;
        this.serviceName = serviceName;
        this.senderId = senderId;
        this.confirmedGraphNodeId = confirmedGraphNodeId;
        this.dagLeafNodeIds = dagLeafNodeIds;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return CausalPacketType.CAUSAL_WRITE_ACK_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public byte[] toBytes() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Serialize the packet type
        int packetType = this.getRequestType().getInt();
        byte[] encodedHeader = ByteBuffer.allocate(4).putInt(packetType).array();

        // Serialize the confirmed graph node id
        CausalPacketProto.VectorTimestamp.Builder confirmedGraphNodeIdsBuilder =
                CausalPacketProto.VectorTimestamp.newBuilder();
        for (String nodeId : this.confirmedGraphNodeId.getNodeIds()) {
            long counter = this.confirmedGraphNodeId.getNodeTimestamp(nodeId);
            CausalPacketProto.VectorTimestamp.NodeCounterPair pair =
                    CausalPacketProto.VectorTimestamp.NodeCounterPair.newBuilder()
                            .setNodeId(nodeId)
                            .setCounter(counter)
                            .build();
            confirmedGraphNodeIdsBuilder.addComponents(pair);
        }

        // Serialize the leaf nodes ids
        List<CausalPacketProto.VectorTimestamp> dagLeafNodeIds = new ArrayList<>();
        for (VectorTimestamp leafNodeId : this.dagLeafNodeIds) {
            CausalPacketProto.VectorTimestamp.Builder graphNodeIdBuilder =
                    CausalPacketProto.VectorTimestamp.newBuilder();
            for (String nodeId : leafNodeId.getNodeIds()) {
                CausalPacketProto.VectorTimestamp.NodeCounterPair pair =
                        CausalPacketProto.VectorTimestamp.NodeCounterPair.newBuilder()
                                .setNodeId(nodeId)
                                .setCounter(leafNodeId.getNodeTimestamp(nodeId))
                                .build();
                graphNodeIdBuilder.addComponents(pair);
            }
            dagLeafNodeIds.add(graphNodeIdBuilder.build());
        }

        // Prepare the packet proto builder
        CausalPacketProto.CausalWriteAckPacket.Builder protoBuilder =
                CausalPacketProto.CausalWriteAckPacket.newBuilder()
                        .setPacketId(this.packetId)
                        .setServiceName(this.serviceName)
                        .setSenderNodeId(this.senderId)
                        .setConfirmedGraphNodeId(confirmedGraphNodeIdsBuilder.build())
                        .addAllGraphLeafNodeIds(dagLeafNodeIds);

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public VectorTimestamp getConfirmedGraphNodeId() {
        return confirmedGraphNodeId;
    }

    public String getSenderId() {
        return senderId;
    }

    public static CausalWriteAckPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0;

        // Decode the packet type
        assert encodedPacket.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        assert packetType == CausalPacketType.CAUSAL_WRITE_ACK_PACKET.getInt()
                : "invalid packet header: " + packetType;
        byte[] encoded = encodedPacket;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        CausalPacketProto.CausalWriteAckPacket decodedProto = null;
        try {
            decodedProto = CausalPacketProto.CausalWriteAckPacket.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        // Decode the confirmed graph node id
        List<String> nodeIds = new ArrayList<>();
        List<Long> counters = new ArrayList<>();
        for (CausalPacketProto.VectorTimestamp.NodeCounterPair pair :
                decodedProto.getConfirmedGraphNodeId().getComponentsList()) {
            nodeIds.add(pair.getNodeId());
            counters.add(pair.getCounter());
        }
        VectorTimestamp confirmedGraphNodeId = new VectorTimestamp(nodeIds);
        for (int i = 0; i < nodeIds.size(); i++) {
            confirmedGraphNodeId.updateNodeTimestamp(nodeIds.get(i), counters.get(i));
        }

        // Decode the leaf ids
        List<VectorTimestamp> dagLeafNodeIds = new ArrayList<>();
        for (CausalPacketProto.VectorTimestamp ts : decodedProto.getGraphLeafNodeIdsList()) {
            VectorTimestamp newDagDepsTimestamp = new VectorTimestamp(nodeIds);
            for (CausalPacketProto.VectorTimestamp.NodeCounterPair pair :
                    ts.getComponentsList()) {
                newDagDepsTimestamp.updateNodeTimestamp(pair.getNodeId(), pair.getCounter());
            }
            dagLeafNodeIds.add(newDagDepsTimestamp);
        }

        return new CausalWriteAckPacket(
                decodedProto.getPacketId(),
                decodedProto.getServiceName(),
                decodedProto.getSenderNodeId(),
                confirmedGraphNodeId,
                dagLeafNodeIds);
    }

}
