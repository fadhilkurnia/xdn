package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.PBEpoch;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;

public class StartEpochPacket implements ReplicableRequest {

    public static final String SERIALIZED_PREFIX = "pb:se:";
    private final String serviceName;
    private final PBEpoch startingEpoch;

    public StartEpochPacket(String serviceName, PBEpoch epoch) {
        this.serviceName = serviceName;
        this.startingEpoch = epoch;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_START_EPOCH_PACKET;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    public PBEpoch getStartingEpoch() {
        return startingEpoch;
    }

    @Override
    public long getRequestID() {
        return 0;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StartEpochPacket that = (StartEpochPacket) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(startingEpoch.ballotNumber, that.startingEpoch.ballotNumber) &&
                Objects.equals(startingEpoch.coordinatorID, that.startingEpoch.coordinatorID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, startingEpoch.ballotNumber, startingEpoch.coordinatorID);
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("serviceName", this.serviceName);
            json.put("epochCounter", this.startingEpoch.ballotNumber);
            json.put("epochNode", this.startingEpoch.coordinatorID);
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static StartEpochPacket createFromString(String encodedPacket) {
        if (encodedPacket == null || !encodedPacket.startsWith(SERIALIZED_PREFIX)) {
            return null;
        }
        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            int epochNode = json.getInt("epochNode");
            int epochCounter = json.getInt("epochCounter");

            return new StartEpochPacket(serviceName, new PBEpoch(epochNode, epochCounter));
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

}
