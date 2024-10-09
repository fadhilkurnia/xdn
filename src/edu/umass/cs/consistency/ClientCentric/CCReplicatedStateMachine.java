package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.gigapaxos.interfaces.Replicable;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class CCReplicatedStateMachine {
    private ArrayList<Integer> members;
    private final String serviceName;
    private final int version;
    private final int readQuorum;
    private final int writeQuorum;
    private CCManager<?> CCManager = null;
    public CCReplicatedStateMachine(String serviceName, int version, int id,
                                    Set<Integer> members, Replicable app, String initialState,
                                    CCManager<?> CCManager){
        this.members = new ArrayList<Integer>(members);
        this.serviceName = serviceName;
        this.version = version;
        this.CCManager = CCManager;
        if(!this.members.contains(id)){
            this.members.add(id);
        }
        if (this.members.size()%2 == 0){
            this.readQuorum = (this.members.size())/2+1;
            this.writeQuorum = (this.members.size())/2;
        }
        else {
            this.readQuorum = (this.members.size()+1)/2;
            this.writeQuorum = (this.members.size()+1)/2;
        }
    }
    public HashMap<Integer, Timestamp> getInitialVectorClock(){
        HashMap<Integer,Timestamp> zeroVC = new HashMap<>();
        for(int member: members){
            zeroVC.put(member, new Timestamp(0));
        }
        return zeroVC;
    }
    @Override
    public String toString(){
        StringBuilder members = new StringBuilder("[");
        for (int member : this.members) {
            members.append(member).append(",");
        }
        members.append("]");
        return "("+this.serviceName+","+this.version+","+members.toString()+")";
    }
    public ArrayList<Integer> getMembers() {
        return this.members;
    }
    public int[] getMembersArray() {
        int[] membersArray = new int[this.members.size()];
        for (int i = 0; i < this.members.size(); i++) {
            membersArray[i] = this.members.get(i);
        }
        return membersArray;
    }
    public int getVersion() {
        return this.version;
    }

    public void setMembers(ArrayList<Integer> members) {
        this.members = members;
    }

    public int getReadQuorum() {
        return readQuorum;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public String getServiceName() {
        return serviceName;
    }
}
