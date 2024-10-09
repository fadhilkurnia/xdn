package edu.umass.cs.causal.dag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VectorTimestamp extends edu.umass.cs.clientcentric.VectorTimestamp {

    public VectorTimestamp(List<String> nodeIds) {
        super(nodeIds);
    }

    public static VectorTimestamp createMaxTimestamp(List<VectorTimestamp> timestamps) {
        assert !timestamps.isEmpty() : "timestamps cannot be empty";

        // validate all timestamps are comparable,
        // while also getting the component-wise max.
        Set<String> nodeIds = timestamps.getFirst().getNodeIds();
        Map<String, Long> maxTimestamp = new HashMap<>();
        for (String nodeId : nodeIds) {
            maxTimestamp.put(nodeId, 0L);
        }
        for (VectorTimestamp ts : timestamps) {
            assert ts.getNodeIds().size() == nodeIds.size();
            assert ts.getNodeIds().equals(nodeIds);
            for (String nodeId : nodeIds) {
                long currMaxTs = maxTimestamp.get(nodeId);
                long myTs = ts.getNodeTimestamp(nodeId);
                if (myTs > currMaxTs) {
                    maxTimestamp.put(nodeId, myTs);
                }
            }
        }

        VectorTimestamp result = new VectorTimestamp(List.copyOf(nodeIds));
        for (String nodeId : nodeIds) {
            long maxTs = maxTimestamp.get(nodeId);
            result.updateNodeTimestamp(nodeId, maxTs);
        }

        return result;
    }

    public synchronized VectorTimestamp increaseNodeTimestamp(String nodeID) {
        return (VectorTimestamp) super.increaseNodeTimestamp(nodeID);
    }
}
