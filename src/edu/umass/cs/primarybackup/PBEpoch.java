package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

public class PBEpoch extends Ballot {
    public PBEpoch(String nodeID, int counter) {
        super(counter, nodeID.hashCode());
    }

    public PBEpoch(int ballotNumber, int coordinatorID) {
        super(ballotNumber, coordinatorID);
    }
}
