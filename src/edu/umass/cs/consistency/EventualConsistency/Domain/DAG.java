package edu.umass.cs.consistency.EventualConsistency.Domain;

import edu.umass.cs.consistency.EventualConsistency.DynamoManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.util.logging.Level;

public class DAG {
    private GraphNode rootNode;

    public DAG(HashMap<Integer, Integer> vectorClock) {
        this.rootNode = new GraphNode(vectorClock);
        DynamoManager.log.log(Level.INFO, "Set {0} as the rootNode", new Object[]{vectorClock});
    }

    public ArrayList<GraphNode> getLatestNodes() {
        ArrayList<GraphNode> latestNodes = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()) {
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
//            DynamoManager.log.log(Level.INFO, "Current vc: "+current.getVectorClock().toString()+" children "+current.getChildren().size()+" isStop "+current.isStop());
            if (current.getChildren().isEmpty()) {
                if (current.isStop()) {
                    DynamoManager.log.log(Level.INFO, "Current node is stop node");
                    return null;
                }
                latestNodes.add(current);
                continue;
            }
            for (GraphNode childNode : current.getChildren()) {
                if (childNode != null && !visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return latestNodes;
    }

    public ArrayList<GraphNode> latestNodesWithVectorClockAsDominant(GraphNode receivedGraphNode,
                                                                     boolean isPut) {
        DynamoManager.log.log(Level.INFO, "Received node: {0}",
                new Object[]{receivedGraphNode.getVectorClock()});
        ArrayList<GraphNode> latestNodes = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();

        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()) {
            GraphNode current = graphNodeStack.pop();
            if ((current.isDominant(receivedGraphNode) && isPut) || current.isStop()) {
                return null;
            }
            visited.add(current.getVectorClock());
            if (receivedGraphNode.isDominant(current)) {
                addNode(latestNodes, current);
                DynamoManager.log.log(Level.INFO, "Adding Current node: {0}, children: {1}, isEmpty: {2}", new Object[]{current.getVectorClock(), current.getChildren(), current.getChildren().isEmpty()});
            }
            for (GraphNode childNode : current.getChildren()) {
                if (!visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return latestNodes;
    }

    public static GraphNode createDominantChildGraphNode(ArrayList<GraphNode> graphNodesList, HashMap<Integer, Integer> vectorClock) {
        if (graphNodesList == null) {
            return null;
        }
        DynamoManager.log.log(Level.INFO, "GraphNode list size: " + graphNodesList.size());
        GraphNode dominantChildNode = new GraphNode();
        for (GraphNode graphNode : graphNodesList) {
            for (Integer key : vectorClock.keySet()) {
                if (vectorClock.get(key) < graphNode.getVectorClock().get(key)) {
                    vectorClock.put(key, graphNode.getVectorClock().get(key));
                }
            }
            graphNode.addChildNode(dominantChildNode);
        }
        dominantChildNode.setVectorClock(vectorClock);
        DynamoManager.log.log(Level.INFO, "Current vc: " + dominantChildNode.getVectorClock().toString() + " children " + dominantChildNode.getChildren().size() + " isStop " + dominantChildNode.isStop());
        return dominantChildNode;
    }

    public static GraphNode getDominantVC(ArrayList<GraphNode> graphNodesList) {
        HashMap<Integer, Integer> dominantVC = graphNodesList.get(0).getVectorClock();
        for (int i = 1; i < graphNodesList.size(); i++) {
            for (Integer key : dominantVC.keySet()) {
                if (dominantVC.get(key) < graphNodesList.get(i).getVectorClock().get(key)) {
                    dominantVC.put(key, graphNodesList.get(i).getVectorClock().get(key));
                }
            }
        }
        return new GraphNode(dominantVC);
    }

    public static void addChildNode(ArrayList<GraphNode> graphNodesList, GraphNode childNode) {
        for (GraphNode graphNode : graphNodesList) {
            graphNode.addChildNode(childNode);
        }
    }

    public void addChildrenNodes(ArrayList<HashMap<Integer, Integer>> graphNodesList, GraphNode graphNode) {
        for (HashMap<Integer, Integer> graphNodeVC : graphNodesList) {
            graphNode.addChildNode(new GraphNode(graphNodeVC));
            DynamoManager.log.log(Level.INFO, "Added {0} as the child of {1}}", new Object[]{graphNodeVC, graphNode.getVectorClock()});
        }
    }

    private void addNode(ArrayList<GraphNode> latestNodes, GraphNode toBeCompared) {
        for (int i = 0; i < latestNodes.size(); i++) {
            if (toBeCompared.isDominant(latestNodes.get(i))) {
                latestNodes.remove(latestNodes.get(i));
            }
        }
        latestNodes.add(toBeCompared);
    }

    /**
     * Returns a map of memberId into list of requests, indexed by the request ID.
     * All the requests' timestamp are between the
     *
     * @param vectorClock upper-bound timestamp (exclusive).
     * @param memberVectorClocks lower-bound timestamp (exclusive).
     * @return
     */
    public HashMap<Integer, HashMap<Long, String>> getRequestsPerMember(
            HashMap<Integer, Integer> vectorClock,
            HashMap<Integer, HashMap<Integer, Integer>> memberVectorClocks) {
        HashMap<Integer, HashMap<Long, String>> mapOfRequests = new HashMap<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()) {
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if (!current.getRequests().isEmpty() && current.isMinor(vectorClock)) {
                for (Integer memberId : memberVectorClocks.keySet()) {
                    if (!current.isMinor(memberVectorClocks.get(memberId))) {
                        for (RequestInformation requestInformation : current.getRequests()) {
                            if (!mapOfRequests.containsKey(memberId)) {
                                mapOfRequests.put(memberId, new HashMap<>());
                            }
                            mapOfRequests.get(memberId).put(
                                    requestInformation.getRequestID(),
                                    requestInformation.getRequestQuery());
                        }
                    }
                }
            }
            for (GraphNode childNode : current.getChildren()) {
                if (childNode != null && !visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return mapOfRequests;
    }

    public HashMap<Long, String> getAllExecutedRequests(HashMap<Integer, Integer> tillVectorClock,
                                                        HashMap<Integer, Integer> fromVectorClock) {
        HashMap<Long, String> mapOfRequests = new HashMap<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()) {
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if (!current.getRequests().isEmpty() && current.isMinor(tillVectorClock)) {
                if (fromVectorClock == null || !current.isMinor(fromVectorClock)) {
                    for (RequestInformation requestInformation : current.getRequests()) {
                        mapOfRequests.put(requestInformation.getRequestID(), requestInformation.getRequestQuery());
                    }
                }
            }
            for (GraphNode childNode : current.getChildren()) {
                if (childNode != null && !visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return mapOfRequests;
    }

    public HashMap<Integer, Integer> getMinimumLatestNode(HashMap<Integer, Integer> minVectorClock, ArrayList<GraphNode> latestNodes) {
        for (GraphNode graphNode : latestNodes) {
            minVectorClock.replaceAll((k, v) -> Math.min(minVectorClock.get(k), graphNode.getVectorClock().get(k)));
        }
        return minVectorClock;
    }

    public void pruneRequests(HashMap<Integer, Integer> vectorClock) {
        ArrayList<GraphNode> nodes = new ArrayList<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()) {
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if (current.isDominant(vectorClock)) {
                nodes.add(current);
            } else if (current.isMinor(vectorClock)) {
                for (GraphNode childNode : current.getChildren()) {
                    if (childNode != null && !visited.contains(childNode.getVectorClock()))
                        graphNodeStack.push(childNode);
                }
                current.setRequests(null);
            }
        }
        if (!nodes.isEmpty()) {
            rootNode.setChildren(new ArrayList<>());
        }
        for (GraphNode node : nodes) {
            DynamoManager.log.info("Adding as child of root node " + node.getVectorClock());
            rootNode.addChildNode(node);
        }
    }

    public HashMap<Long, String> getAllRequestsWithVectorClockAsDominant(GraphNode graphNode) {
        HashMap<Long, String> mapOfRequests = new HashMap<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        ArrayList<HashMap<Integer, Integer>> visited = new ArrayList<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()) {
            GraphNode current = graphNodeStack.pop();
            visited.add(current.getVectorClock());
            if (!current.getRequests().isEmpty() && graphNode.isDominant(current)) {
                for (RequestInformation requestInformation : current.getRequests()) {
                    mapOfRequests.put(requestInformation.getRequestID(), requestInformation.getRequestQuery());
                }
            }
            for (GraphNode childNode : current.getChildren()) {
                if (childNode != null && !visited.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return mapOfRequests;
    }

    public ArrayList<HashMap<Integer, Integer>> getAllVC() {
        ArrayList<HashMap<Integer, Integer>> allVC = new ArrayList<>();
        Stack<GraphNode> graphNodeStack = new Stack<>();
        graphNodeStack.push(rootNode);
        while (!graphNodeStack.isEmpty()) {
            GraphNode current = graphNodeStack.pop();
            allVC.add(current.getVectorClock());
            for (GraphNode childNode : current.getChildren()) {
                if (childNode != null && !allVC.contains(childNode.getVectorClock()))
                    graphNodeStack.push(childNode);
            }
        }
        return allVC;
    }

    public GraphNode getRootNode() {
        return rootNode;
    }

    public void setRootNode(GraphNode rootNode) {
        this.rootNode = rootNode;
    }
}
