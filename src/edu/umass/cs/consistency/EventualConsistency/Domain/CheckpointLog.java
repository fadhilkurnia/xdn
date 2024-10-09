package edu.umass.cs.consistency.EventualConsistency.Domain;

import java.util.ArrayList;
import java.util.HashMap;

public class CheckpointLog {
    private String state;
    private int noOfCheckpoints;
    private String quorumId;
    private HashMap<Integer, Integer> vectorClock;
    private ArrayList<HashMap<Integer, Integer>> latestNodes;
    private int myId;
    public CheckpointLog() {}
    public CheckpointLog(String state,int noOOfCheckpoints, String quorumId, HashMap<Integer, Integer> vectorClock, ArrayList<HashMap<Integer, Integer>> latestNodes, int myId) {
        this.state = state;
        this.quorumId = quorumId;
        this.vectorClock = vectorClock;
        this.latestNodes = latestNodes;
        this.noOfCheckpoints = noOOfCheckpoints;
        this.myId = myId;
    }

    public int getNoOfCheckpoints() {
        return noOfCheckpoints;
    }

    public void setNoOfCheckpoints(int noOfCheckpoints) {
        this.noOfCheckpoints = noOfCheckpoints;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getQuorumId() {
        return quorumId;
    }

    public void setQuorumId(String quorumId) {
        this.quorumId = quorumId;
    }

    public HashMap<Integer, Integer> getVectorClock() {
        return vectorClock;
    }

    public void setVectorClock(HashMap<Integer, Integer> vectorClock) {
        this.vectorClock = vectorClock;
    }

    public ArrayList<HashMap<Integer, Integer>> getLatestNodes() {
        return latestNodes;
    }

    public void setLatestNodes(ArrayList<HashMap<Integer, Integer>> latestNodes) {
        this.latestNodes = latestNodes;
    }

    public int getMyId() {
        return myId;
    }
}
