package edu.umass.cs.causal.dag;

import java.util.*;

public class DirectedAcyclicGraph {
    private final List<GraphVertex> startingVertices;

    private final Map<VectorTimestamp, GraphVertex> idToVertexMapper;

    private final Set<GraphVertex> leafVertices;

    public DirectedAcyclicGraph() {
        this(new ArrayList<>());
    }

    public DirectedAcyclicGraph(GraphVertex startingVertex) {
        this(new ArrayList<>(List.of(startingVertex)));
    }

    private DirectedAcyclicGraph(List<GraphVertex> startingVertices) {
        this.startingVertices = new ArrayList<>();
        this.idToVertexMapper = new HashMap<>();
        this.leafVertices = new LinkedHashSet<>();
        this.startingVertices.addAll(startingVertices);
        for (GraphVertex n : startingVertices) {
            this.idToVertexMapper.put(n.getTimestamp(), n);
            if (n.getChildren().isEmpty()) {
                this.leafVertices.add(n);
            }
        }
    }

    public synchronized List<GraphVertex> getLeafVertices() {
        assert !this.leafVertices.isEmpty() : "Expecting leaf vertices but found none";
        assert this.leafVertices.size() < 100 : "Too big of a result: " + this.leafVertices.size();
        return new ArrayList<>(this.leafVertices);
    }

    public synchronized void addChildOf(List<GraphVertex> parents, GraphVertex child) {
        assert parents != null;
        assert child != null;

        if (parents.isEmpty()) {
            this.startingVertices.add(child);
            this.idToVertexMapper.put(child.getTimestamp(), child);
            if (child.getChildren().isEmpty()) {
                this.leafVertices.add(child);
            }
            return;
        }

        // Ensure that parents are not dominant to the child.
        boolean haveDominantParent = false;
        for (GraphVertex n : parents) {
            if (n.isDominantAgainst(child)) {
                haveDominantParent = true;
                break;
            }
        }
        assert !haveDominantParent : "Unexpected dominant parent";

        // Validate that the given parents do exist in this graph
        for (GraphVertex parent : parents) {
            assert this.isContain(parent);
        }

        // Add the new child
        this.idToVertexMapper.put(child.getTimestamp(), child);
        for (GraphVertex parent : parents) {
            parent.addChildVertex(child);
            this.leafVertices.remove(parent);
        }
        if (child.getChildren().isEmpty()) {
            this.leafVertices.add(child);
        }

        // If the child has children, ensure we don't have cycle.
        assert child.getChildren().isEmpty() || !this.isCycleExist() :
                "The newly inserted child vertex create a cycle in the graph";
    }

    public synchronized boolean isContain(GraphVertex vertex) {
        return this.idToVertexMapper.get(vertex.getTimestamp()) != null;
    }

    public boolean isCycleExist() {
        Set<VectorTimestamp> visited = new HashSet<>();

        // Prepare the DFS traversal stack
        Stack<GraphVertex> traversalStack = new Stack<>();
        for (GraphVertex n : this.startingVertices) {
            traversalStack.push(n);
        }

        // Traverse through all the vertices in the graph
        while (!traversalStack.isEmpty()) {
            GraphVertex current = traversalStack.pop();

            if (visited.contains(current.getTimestamp())) {
                return true;
            }

            visited.add(current.getTimestamp());

            List<GraphVertex> children = current.getChildren();
            for (GraphVertex child : children) {
                traversalStack.push(child);
            }
        }

        return false;
    }

    public synchronized boolean isContainAll(List<VectorTimestamp> timestamps) {
        for (VectorTimestamp ts : timestamps) {
            if (!this.idToVertexMapper.containsKey(ts)) return false;
        }
        return true;
    }

    public synchronized List<GraphVertex> getVerticesByTimestamps(List<VectorTimestamp> timestamps) {
        List<GraphVertex> graphVertices = new ArrayList<>();
        for (VectorTimestamp ts : timestamps) {
            GraphVertex n = this.idToVertexMapper.get(ts);
            if (n == null) continue;
            graphVertices.add(n);
        }
        return graphVertices;
    }

}
