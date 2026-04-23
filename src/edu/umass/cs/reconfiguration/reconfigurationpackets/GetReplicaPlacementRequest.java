package edu.umass.cs.reconfiguration.reconfigurationpackets;

import edu.umass.cs.nio.interfaces.Geolocation;
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
    private List<String> replicaHttpAddresses;
    private List<String> replicaRoles;
    private List<String> replicaMetadata;
    private List<Geolocation> replicaGeolocations;
    private String serviceMetadata;

    public GetReplicaPlacementRequest(InetSocketAddress initiator, String name) {
        super(initiator, PacketType.GET_REPLICA_PLACEMENT_REQUEST, name, 0);

        replicaNodeIds = new ArrayList<>();
        replicaAddresses = new ArrayList<>();
        replicaHttpAddresses = new ArrayList<>();
        replicaRoles = new ArrayList<>();
        replicaMetadata = new ArrayList<>();
        replicaGeolocations = new ArrayList<>();
        serviceMetadata = "";
    }

    public void setReplicaNodeIds(List<String> replicaNodeIds) {
        this.replicaNodeIds = replicaNodeIds;
    }

    public void setReplicaAddresses(List<String> replicaAddresses) {
        this.replicaAddresses = replicaAddresses;
    }

    public void setReplicaHttpAddresses(List<String> replicaHttpAddresses) {
        this.replicaHttpAddresses = replicaHttpAddresses;
    }

    public void setPlacementEpochNumber(int epochNumber) {
        this.epochNumber = epochNumber;
    }

    public void setReplicaRoles(List<String> replicaRoles) {
        this.replicaRoles = replicaRoles;
    }

    public void setReplicaMetadata(List<String> replicaMetadata) {
        this.replicaMetadata = replicaMetadata;
    }

    public void setReplicaGeolocations(List<Geolocation> replicaGeolocations) {
        this.replicaGeolocations = replicaGeolocations;
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
            nodeInfo.put("HTTP_ADDRESS",
                    replicaHttpAddresses.size() >= i+1 ? replicaHttpAddresses.get(i) : "");
            nodeInfo.put("ROLE", replicaRoles.size() >= i+1 ? replicaRoles.get(i) : "");
            nodeInfo.put("METADATA", replicaMetadata.size() >= i+1 ? replicaMetadata.get(i) : "");
            Geolocation geo = replicaGeolocations.size() >= i+1 ? replicaGeolocations.get(i) : null;
            if (geo != null) {
                JSONObject geoJson = new JSONObject();
                geoJson.put("LATITUDE", geo.latitude());
                geoJson.put("LONGITUDE", geo.longitude());
                nodeInfo.put("GEOLOCATION", geoJson);
            } else {
                nodeInfo.put("GEOLOCATION", JSONObject.NULL);
            }
            nodeArray.put(i, nodeInfo);
        }
        dataJsonObject.put("NODES", nodeArray);
        dataJsonObject.put("SERVICE_METADATA", serviceMetadata);

        resultJsonObject.put("DATA", dataJsonObject);
        return resultJsonObject;
    }
}
