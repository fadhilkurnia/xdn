package edu.umass.cs.clientcentric.packets;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import edu.umass.cs.clientcentric.VectorTimestamp;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientCentricWriteAfterPacket extends ClientCentricPacket implements Byteable {

    private final VectorTimestamp timestamp;
    private final ClientRequest clientWriteOnlyRequest;
    private final long packetID;
    private final String senderID;

    public ClientCentricWriteAfterPacket(String senderID, VectorTimestamp timestamp,
                                         ClientRequest clientWriteOnlyRequest) {
        this(System.currentTimeMillis(), senderID, timestamp, clientWriteOnlyRequest);
    }

    private ClientCentricWriteAfterPacket(long packetID, String senderID, VectorTimestamp timestamp,
                                          ClientRequest clientWriteOnlyRequest) {
        assert packetID != 0 : "A likely invalid packetID given";
        assert senderID != null : "The sender cannot be null";
        assert timestamp != null : "The timestamp cannot be null";
        assert clientWriteOnlyRequest != null : "The provided ClientRequest cannot be null";
        assert (clientWriteOnlyRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest()) :
                "The provided ClientRequest must be a WriteOnlyRequest";
        this.packetID = packetID;
        this.senderID = senderID;
        this.timestamp = timestamp;
        this.clientWriteOnlyRequest = clientWriteOnlyRequest;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return ClientCentricPacketType.CLIENT_CENTRIC_WRITE_AFTER_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.clientWriteOnlyRequest.getServiceName();
    }

    @Override
    public long getRequestID() {
        return this.packetID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public String getSenderID() {
        return senderID;
    }

    public ClientRequest getClientWriteOnlyRequest() {
        return clientWriteOnlyRequest;
    }

    public VectorTimestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public byte[] toBytes() {
        byte[] encodedRequest = this.clientWriteOnlyRequest.toBytes();
        int vectorTimestampSize = computeVectorTimestampSize(this.timestamp);

        int payloadSize = CodedOutputStream.computeInt64Size(1, this.packetID)
                + CodedOutputStream.computeStringSize(2, this.senderID)
                + CodedOutputStream.computeTagSize(3)
                + CodedOutputStream.computeUInt32SizeNoTag(vectorTimestampSize)
                + vectorTimestampSize
                + CodedOutputStream.computeByteArraySize(4, encodedRequest);

        byte[] serialized = new byte[Integer.BYTES + payloadSize];
        ByteBuffer.wrap(serialized, 0, Integer.BYTES)
                .putInt(this.getRequestType().getInt());

        CodedOutputStream output = CodedOutputStream.newInstance(serialized,
                Integer.BYTES, payloadSize);
        try {
            output.writeInt64(1, this.packetID);
            output.writeString(2, this.senderID);
            output.writeTag(3, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            output.writeUInt32NoTag(vectorTimestampSize);
            writeVectorTimestamp(this.timestamp, output);
            output.writeByteArray(4, encodedRequest);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ClientCentricWriteAfterPacket", e);
        }

        return serialized;
    }

    public static ClientCentricWriteAfterPacket createFromBytes(byte[] encodedPacket,
                                                                AppRequestParser appRequestParser) {
        assert encodedPacket != null && encodedPacket.length >= Integer.BYTES
                : "Encoded packet cannot be empty";
        assert appRequestParser != null : "AppRequestParser cannot be null";

        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        if (packetType != ClientCentricPacketType.CLIENT_CENTRIC_WRITE_AFTER_PACKET.getInt()) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded ClientCentricWriteAfterPacket: unexpected type "
                            + packetType);
            return null;
        }

        long packetId = 0;
        String senderId = null;
        VectorTimestamp timestamp = null;
        byte[] encodedRequest = null;

        CodedInputStream input = CodedInputStream.newInstance(
                encodedPacket, Integer.BYTES, encodedPacket.length - Integer.BYTES);
        try {
            int tag;
            while ((tag = input.readTag()) != 0) {
                switch (tag) {
                    case 8 -> packetId = input.readInt64();
                    case 18 -> senderId = input.readStringRequireUtf8();
                    case 26 -> {
                        byte[] vectorBytes = input.readByteArray();
                        timestamp = decodeVectorTimestamp(vectorBytes);
                    }
                    case 34 -> encodedRequest = input.readByteArray();
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded ClientCentricWriteAfterPacket: " + e.getMessage());
            return null;
        }

        if (senderId == null || timestamp == null || encodedRequest == null) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded ClientCentricWriteAfterPacket: missing fields");
            return null;
        }

        Request clientRequest;
        try {
            clientRequest = appRequestParser.getRequest(encodedRequest, null);
        } catch (RequestParseException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "Receiving an invalid encoded ClientCentricWriteAfterPacket: " + e.getMessage());
            return null;
        }

        assert (clientRequest instanceof ClientRequest) :
                "The request inside ClientCentricWriteAfterPacket must implement ClientRequest interface";
        assert (clientRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest()) :
                "The client request inside ClientCentricWriteAfterPacket must be WriteOnlyRequest";

        return new ClientCentricWriteAfterPacket(packetId, senderId, timestamp,
                (ClientRequest) clientRequest);
    }

    private static int computeVectorTimestampSize(VectorTimestamp timestamp) {
        List<String> nodeIds = new ArrayList<>(timestamp.getNodeIds());
        Collections.sort(nodeIds);
        int size = 0;
        for (String nodeId : nodeIds) {
            size += CodedOutputStream.computeStringSize(1, nodeId);
            size += CodedOutputStream.computeInt64Size(2, timestamp.getNodeTimestamp(nodeId));
        }
        return size;
    }

    private static void writeVectorTimestamp(VectorTimestamp timestamp,
                                             CodedOutputStream output) throws IOException {
        List<String> nodeIds = new ArrayList<>(timestamp.getNodeIds());
        Collections.sort(nodeIds);
        for (String nodeId : nodeIds) {
            output.writeString(1, nodeId);
            output.writeInt64(2, timestamp.getNodeTimestamp(nodeId));
        }
    }

    private static VectorTimestamp decodeVectorTimestamp(byte[] encodedTimestamp) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(encodedTimestamp);
        List<String> nodeIds = new ArrayList<>();
        List<Long> counters = new ArrayList<>();
        int tag;
        while ((tag = input.readTag()) != 0) {
            switch (tag) {
                case 10 -> nodeIds.add(input.readStringRequireUtf8());
                case 16 -> counters.add(input.readInt64());
                default -> input.skipField(tag);
            }
        }

        if (nodeIds.size() != counters.size()) {
            throw new IOException("Mismatched vector timestamp components");
        }

        VectorTimestamp timestamp = new VectorTimestamp(nodeIds);
        for (int i = 0; i < nodeIds.size(); i++) {
            timestamp.updateNodeTimestamp(nodeIds.get(i), counters.get(i));
        }
        return timestamp;
    }
}
