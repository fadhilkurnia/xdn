package edu.umass.cs.primarybackup;

import java.util.Objects;

public class PrimaryEpoch<NodeIDType> {

    public final String nodeID;
    public final int counter;

    public PrimaryEpoch(NodeIDType primaryNodeID, int counter) {
        this.nodeID = primaryNodeID.toString();
        this.counter = counter;
    }

    public PrimaryEpoch(String primaryNodeIDStr, int counter) {
        this.nodeID = primaryNodeIDStr;
        this.counter = counter;
    }

    public PrimaryEpoch(String epochString) {
        String[] raw = epochString.split(":");
        assert raw.length == 2;

        this.nodeID = raw[0];
        this.counter = Integer.parseInt(raw[1]);
    }

    @Override
    public String toString() {
        return String.format("%s:%d", this.nodeID, this.counter);
    }

    public int compareTo(PrimaryEpoch<NodeIDType> that) {
        if (this.counter == that.counter) {
            return this.nodeID.compareTo(that.nodeID);
        }
        if (this.counter < that.counter) {
            return -1;
        }
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrimaryEpoch<?> that = (PrimaryEpoch<?>) o;
        return counter == that.counter && Objects.equals(nodeID, that.nodeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeID, counter);
    }

}
