package edu.umass.cs.eventual;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import edu.umass.cs.clientcentric.VectorTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wire codec and small helpers for {@link VectorTimestamp}, used by the anti-entropy packets in
 * {@link edu.umass.cs.eventual.packets}. The wire format matches the length-delimited
 * (nodeId, counter) pair encoding used elsewhere in this codebase (see
 * ClientCentricWriteAfterPacket), kept local so the clientcentric package stays untouched.
 */
public final class VectorClockCodec {

    private VectorClockCodec() {
    }

    public static int computeSize(VectorTimestamp timestamp) {
        List<String> nodeIds = new ArrayList<>(timestamp.getNodeIds());
        Collections.sort(nodeIds);
        int size = 0;
        for (String nodeId : nodeIds) {
            size += CodedOutputStream.computeStringSize(1, nodeId);
            size += CodedOutputStream.computeInt64Size(2, timestamp.getNodeTimestamp(nodeId));
        }
        return size;
    }

    public static byte[] encode(VectorTimestamp timestamp) {
        byte[] encoded = new byte[computeSize(timestamp)];
        CodedOutputStream output = CodedOutputStream.newInstance(encoded);
        try {
            List<String> nodeIds = new ArrayList<>(timestamp.getNodeIds());
            Collections.sort(nodeIds);
            for (String nodeId : nodeIds) {
                output.writeString(1, nodeId);
                output.writeInt64(2, timestamp.getNodeTimestamp(nodeId));
            }
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode VectorTimestamp", e);
        }
        return encoded;
    }

    public static VectorTimestamp decode(byte[] encodedTimestamp) throws IOException {
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

    /** Returns a deep copy of the given vector timestamp. */
    public static VectorTimestamp copy(VectorTimestamp timestamp) {
        List<String> nodeIds = new ArrayList<>(timestamp.getNodeIds());
        VectorTimestamp copied = new VectorTimestamp(nodeIds);
        for (String nodeId : nodeIds) {
            copied.updateNodeTimestamp(nodeId, timestamp.getNodeTimestamp(nodeId));
        }
        return copied;
    }

    /** Sum of all components, i.e., the size of the applied-write set the clock encodes. */
    public static long sum(VectorTimestamp timestamp) {
        long total = 0;
        for (String nodeId : timestamp.getNodeIds()) {
            total += timestamp.getNodeTimestamp(nodeId);
        }
        return total;
    }

    /**
     * The frontier total order: a replica with a larger applied-write set wins; ties are broken
     * by node ID. Returns true iff (sumA, idA) beats (sumB, idB).
     */
    public static boolean frontierWins(long sumA, String idA, long sumB, String idB) {
        if (sumA != sumB) return sumA > sumB;
        return idA.compareTo(idB) > 0;
    }

    /** Component-wise max-merge of {@code from} into {@code into} (mutates {@code into}). */
    public static void maxMergeInto(VectorTimestamp into, VectorTimestamp from) {
        for (String nodeId : into.getNodeIds()) {
            if (!from.getNodeIds().contains(nodeId)) continue;
            long theirs = from.getNodeTimestamp(nodeId);
            if (theirs > into.getNodeTimestamp(nodeId)) {
                into.updateNodeTimestamp(nodeId, theirs);
            }
        }
    }
}
