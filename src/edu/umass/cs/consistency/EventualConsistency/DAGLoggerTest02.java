package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.EventualConsistency.Domain.CheckpointLog;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
import edu.umass.cs.consistency.EventualConsistency.Domain.RequestInformation;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DAGLoggerTest02 {
    public static void main(String[] args) throws JSONException, IOException {
        DAGLogger dagLogger = new DAGLogger("testLogFile");

        HashMap<Integer, Integer> vectorClock1 = new HashMap<>();
        vectorClock1.put(1, 100);
        vectorClock1.put(2, 200);
        ArrayList<RequestInformation> requests1 = new ArrayList<>();
        requests1.add(new RequestInformation(1001, "data1"));

        GraphNode graphNode1 = new GraphNode(vectorClock1, requests1);

        dagLogger.rollForward(graphNode1);

        HashMap<Integer, Integer> vectorClock2 = new HashMap<>();
        vectorClock2.put(3, 300);
        vectorClock2.put(4, 400);
        ArrayList<RequestInformation> requests2 = new ArrayList<>();
        requests2.add(new RequestInformation(1002, "data2"));

        GraphNode graphNode2 = new GraphNode(vectorClock2, requests2);

        dagLogger.rollForward(graphNode2);

        ArrayList<GraphNode> graphNodes = dagLogger.readFromRollForwardFile();
        for (GraphNode node : graphNodes) {
            System.out.println("GraphNode:");
            System.out.println("Vector Clock: " + node.getVectorClock());
            System.out.println("Requests: " + node.getRequests());
        }
    }
}


