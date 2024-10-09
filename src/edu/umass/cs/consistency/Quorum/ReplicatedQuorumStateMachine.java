package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.gigapaxos.interfaces.Replicable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class ReplicatedQuorumStateMachine {
    private ArrayList<Integer> quorumMembers; // from head to tail
    private final int readQuorum;
    private final int writeQuorum;
    private final String quorumID;
    private final int version;
    private QuorumManager<?> quorumManager = null;

    public ReplicatedQuorumStateMachine(String quorumID, int version, int id,
                                        Set<Integer> members, Replicable app, String initialState,
                                        QuorumManager<?> qm){
        this.quorumMembers = new ArrayList<Integer>(members);
        this.quorumID = quorumID;
        this.version = version;
        this.quorumManager = qm;
        if(!this.quorumMembers.contains(id)){
            this.quorumMembers.add(id);
        }
        if (this.quorumMembers.size()%2 == 0){
            this.readQuorum = (this.quorumMembers.size())/2+1;
            this.writeQuorum = (this.quorumMembers.size())/2;
        }
        else {
            this.readQuorum = (this.quorumMembers.size()+1)/2;
            this.writeQuorum = (this.quorumMembers.size()+1)/2;
        }

    }

    public HashMap<Integer, Integer> getInitialVectorClock(){
        HashMap<Integer,Integer> zeroVC = new HashMap<>();
        for(int member: quorumMembers){
            zeroVC.put(member, 0);
        }
        return zeroVC;
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

    @Override
    public String toString(){
        StringBuilder members = new StringBuilder("[");
        for (int quorumMember : this.quorumMembers) {
            members.append(quorumMember).append(",");
        }
        members.append("]");

        return "("+this.quorumID+","+this.version+","+members.toString()+",read quorum="
                +this.readQuorum+",write quorum="+this.writeQuorum+")";
    }
}
