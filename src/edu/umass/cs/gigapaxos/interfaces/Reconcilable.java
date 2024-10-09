package edu.umass.cs.gigapaxos.interfaces;

import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;

import java.util.ArrayList;

public interface Reconcilable extends Replicable {

    /**
     *
     * @param requests
     * @return
     */

    public GraphNode reconcile(ArrayList<GraphNode> requests);
    public String stateForReconcile();
}
