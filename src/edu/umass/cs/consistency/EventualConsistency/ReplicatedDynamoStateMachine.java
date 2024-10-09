package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.Quorum.QuorumManager;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;

public class ReplicatedDynamoStateMachine {
    private ArrayList<Integer> quorumMembers; // from head to tail
    private HashMap<Integer, HashMap<Integer, Integer>> memberVectorClocks;
    private final int readQuorum;
    private final int writeQuorum;
    private final String quorumID;
    private final int version;
    private DynamoManager<?> dynamoManager = null;

    private DAGLogger dagLogger;

    public ReplicatedDynamoStateMachine(String quorumID, int version, int id,
                                        Set<Integer> members, Replicable app, String initialState,
                                        DynamoManager<?> qm) {
        this.quorumMembers = new ArrayList<Integer>(members);
        this.quorumID = quorumID;
        this.version = version;
        this.dynamoManager = qm;
        try {
            this.dagLogger = new DAGLogger(id + "." + quorumID);
            this.dagLogger.restore();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RuntimeException | JSONException e) {
            DynamoManager.log.log(Level.INFO, "Restore not performed");
        }
        if (!this.quorumMembers.contains(id)) {
            this.quorumMembers.add(id);
        }
        if (this.quorumMembers.size() % 2 == 0) {
            this.readQuorum = (this.quorumMembers.size()) / 2 + 1;
            this.writeQuorum = (this.quorumMembers.size()) / 2;
        } else {
            this.readQuorum = (this.quorumMembers.size() + 1) / 2;
            this.writeQuorum = (this.quorumMembers.size() + 1) / 2;
        }

        memberVectorClocks = new HashMap<>();
        HashMap<Integer, Integer> zeroVC = getInitialVectorClock();
        for (int member : quorumMembers) {
            memberVectorClocks.put(member, zeroVC);
        }
        DynamoManager.log.log(Level.INFO, "Initialized member vector Clock for {0} to {1}", new Object[]{id, memberVectorClocks});
    }

    public HashMap<Integer, Integer> getInitialVectorClock() {
        if (this.dagLogger.getCheckpointLog() == null ||
                this.dagLogger.getCheckpointLog().getVectorClock() == null) {
            HashMap<Integer, Integer> zeroVC = new HashMap<>();
            for (int member : quorumMembers) {
                zeroVC.put(member, 0);
            }
            return zeroVC;
        }
        return this.dagLogger.getCheckpointLog().getVectorClock();
    }

    public HashMap<Integer, Integer> getMaxVectorClock() {
        HashMap<Integer, Integer> maxVC = new HashMap<>();
        for (int member : quorumMembers) {
            maxVC.put(member, Integer.MAX_VALUE);
        }
        return maxVC;
    }

    public ArrayList<Integer> getQuorumMembers() {
        return this.quorumMembers;
    }

    public int[] getQuorumMembersArray() {
        int[] quorumMembersArray = new int[this.quorumMembers.size()];
        for (int i = 0; i < this.quorumMembers.size(); i++) {
            quorumMembersArray[i] = this.quorumMembers.get(i);
        }
        return quorumMembersArray;
    }

    public int getReadQuorum() {
        return this.readQuorum;
    }

    public int getWriteQuorum() {
        return this.writeQuorum;
    }

    public String getQuorumID() {
        return this.quorumID;
    }

    public int getVersion() {
        return this.version;
    }

    public DAGLogger getDagLogger() {
        return dagLogger;
    }

    public HashMap<Integer, HashMap<Integer, Integer>> getMemberVectorClocks() {
        return memberVectorClocks;
    }

    public void updateMemberVectorClock(int member, HashMap<Integer, Integer> vectorClock) {
        boolean isMinor = true;
        boolean isDominant = true;
        HashMap<Integer, Integer> minVectorClock = new HashMap<>();
        for (int i : vectorClock.keySet()) {
            isMinor = isMinor & (memberVectorClocks.get(member).get(i) > vectorClock.get(i));
            isDominant = isDominant & (memberVectorClocks.get(member).get(i) < vectorClock.get(i));
            minVectorClock.put(i, Math.min(memberVectorClocks.get(member).get(i), vectorClock.get(i)));
        }
        if (isDominant) {
            memberVectorClocks.put(member, vectorClock);
        } else if (!isMinor) {  // the provided vectorClock is not dominant nor minor.
            // FIXME: why min?
            memberVectorClocks.put(member, minVectorClock);
        }
        DynamoManager.log.log(Level.INFO, "Updating member Vector Clock for {0} to updated {1}",
                new Object[]{member, memberVectorClocks.get(member)});
    }

    public HashMap<Integer, Integer> getMinorVectorClock() {
        HashMap<Integer, Integer> minorVC = new HashMap<>();
        for (int member : memberVectorClocks.keySet()) {
            if (minorVC.isEmpty()) {
                minorVC = memberVectorClocks.get(member);
                continue;
            }
            for (int node : minorVC.keySet()) {
                minorVC.put(node, Math.min(minorVC.get(node), memberVectorClocks.get(member).get(node)));
            }
        }
        return minorVC;
    }

    public void setMemberVectorClocks(HashMap<Integer, HashMap<Integer, Integer>> memberVectorClocks) {
        this.memberVectorClocks = memberVectorClocks;
    }


    @Override
    public String toString() {
        StringBuilder members = new StringBuilder("[");
        for (int quorumMember : this.quorumMembers) {
            members.append(quorumMember).append(",");
        }
        members.append("]");

        return "(" + this.quorumID + "," + this.version + "," + members.toString() + ",read quorum="
                + this.readQuorum + ",write quorum=" + this.writeQuorum + ")";
    }
}
