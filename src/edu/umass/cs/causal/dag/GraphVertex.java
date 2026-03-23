package edu.umass.cs.causal.dag;

import edu.umass.cs.gigapaxos.interfaces.Request;

import java.util.ArrayList;
import java.util.List;

public class GraphVertex {
    private final VectorTimestamp timestamp;
    private final List<Request> requests;
    private final List<GraphVertex> children;

    public GraphVertex(VectorTimestamp timestamp, List<Request> requests) {
        this.timestamp = timestamp;
        this.requests = requests;
        this.children = new ArrayList<>();
    }

    public void addChildVertex(GraphVertex child) {
        this.children.add(child);
    }

    public List<Request> getRequests() {
        return requests;
    }

    public List<GraphVertex> getChildren() {
        return children;
    }

    public final VectorTimestamp getTimestamp() {
        return this.timestamp;
    }

    public boolean isDominantAgainst(GraphVertex anotherVertex) {
        return anotherVertex.timestamp.isLessThanOrEqualTo(this.timestamp);
    }

    @Override
    public String toString() {
        return "GraphVertex{" +
                "timestamp=" + timestamp +
                '}';
    }
}
