package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class GetReplicaPlacementRequest extends ClientReconfigurationPacket {

    // response data
    private List<String> replicaNodeIds;
    private List<String> replicaAddresses;
    private List<String> replicaRoles;
    private List<String> replicaMetadata;
    private String serviceMetadata;

    public GetReplicaPlacementRequest(InetSocketAddress initiator, String name) {
        super(initiator, PacketType.GET_REPLICA_PLACEMENT_REQUEST, name, 0);

        replicaNodeIds = new ArrayList<>();
        replicaAddresses = new ArrayList<>();
        replicaRoles = new ArrayList<>();
        replicaMetadata = new ArrayList<>();
        serviceMetadata = "";
    }

    public void setReplicaNodeIds(List<String> replicaNodeIds) {
        this.replicaNodeIds = replicaNodeIds;
    }

    public void setReplicaAddresses(List<String> replicaAddresses) {
        this.replicaAddresses = replicaAddresses;
    }

    public void setReplicaRoles(List<String> replicaRoles) {
        this.replicaRoles = replicaRoles;
    }

    public void setReplicaMetadata(List<String> replicaMetadata) {
        this.replicaMetadata = replicaMetadata;
    }

    public void setServiceMetadata(String serviceMetadata) {
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject resultJsonObject = super.toJSONObjectImpl();

        JSONObject dataJsonObject = new JSONObject();
        JSONArray nodeArray = new JSONArray();
        for (int i = 0; i < replicaNodeIds.size(); i++) {
            JSONObject nodeInfo = new JSONObject();
            nodeInfo.put("ID", replicaNodeIds.get(i));
            nodeInfo.put("ADDRESS", replicaAddresses.size() >= i+1 ? replicaAddresses.get(i) : "");
            nodeInfo.put("ROLE", replicaRoles.size() >= i+1 ? replicaRoles.get(i) : "");
            nodeInfo.put("METADATA", replicaMetadata.size() >= i+1 ? replicaMetadata.get(i) : "");
            nodeArray.put(i, nodeInfo);
        }
        dataJsonObject.put("NODES", nodeArray);
        dataJsonObject.put("SERVICE_METADATA", serviceMetadata);

        resultJsonObject.put("DATA", dataJsonObject);
        return resultJsonObject;
    }
}
