package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.EventualConsistency.Domain.CheckpointLog;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
import edu.umass.cs.consistency.EventualConsistency.Domain.RequestInformation;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class DAGLoggerTest {
    public static void main(String[] args) throws JSONException, IOException {
        DAGLogger dagLogger = new DAGLogger("testLogFile");

        String state = "testState";
        String quorumId = "quorum123";
        HashMap<Integer, Integer> vectorClock = new HashMap<>();
        vectorClock.put(1, 100);
        vectorClock.put(2, 200);

        ArrayList<GraphNode> leafNodes = new ArrayList<>();
        HashMap<Integer, Integer> leafVectorClock = new HashMap<>();
        leafVectorClock.put(3, 300);
        ArrayList<RequestInformation> requests = new ArrayList<>();
        requests.add(new RequestInformation(101, "data1"));
        leafNodes.add(new GraphNode(leafVectorClock, requests));

        dagLogger.checkpoint(state, 0, vectorClock, quorumId, leafNodes, 0);
        dagLogger.restore();
        CheckpointLog restoredCheckpointLog = dagLogger.getCheckpointLog();
        if (restoredCheckpointLog != null) {
            System.out.println("Restored Checkpoint Log:");
            System.out.println("State: " + restoredCheckpointLog.getState());
            System.out.println("Quorum ID: " + restoredCheckpointLog.getQuorumId());
            System.out.println("Vector Clock: " + restoredCheckpointLog.getVectorClock());
            System.out.println("Latest Vector Clocks: " + restoredCheckpointLog.getLatestNodes());
        } else {
            System.out.println("No checkpoint log found.");
        }
    }
}