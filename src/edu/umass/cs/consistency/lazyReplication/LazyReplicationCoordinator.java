package edu.umass.cs.consistency.lazyReplication;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.examples.noop.NoopAppCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LazyReplicationCoordinator<NodeIDType> extends
        AbstractReplicaCoordinator<NodeIDType> {
    protected static final Logger log = (ReconfigurationConfig.getLogger());

    private class Data {
        final String name;
        final int epoch;
        final Set<NodeIDType> replicas;

        Data(String name, int epoch, Set<NodeIDType> replicas) {
            this.name = name;
            this.epoch = epoch;
            this.replicas = replicas;
        }
    }
    private final HashMap<String, Data> groups = new HashMap<String, Data>();

    public LazyReplicationCoordinator(Replicable app,  NodeIDType myID,
                                      Stringifiable<NodeIDType> unstringer,
                                      SSLMessenger<NodeIDType, JSONObject> msgr) {
        super(app, msgr);
        System.out.println("MY ID is: "+myID);
        System.out.println("Initializing COORDINATOR-------------------");
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return this.app.getRequestTypes();
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        Level level = Level.FINE;
        System.out.println("In coordinate request, no coordination. Lazy Replication");
        this.app.execute(request);
        callback.executed(request, true);
        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        System.out.println("In create replica group");
        this.groups.put(serviceName, new Data(serviceName, epoch,
                nodes));
        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        this.groups.remove(serviceName);
        return true;
    }

    @Override
    public Integer getEpoch(String name) {
        System.out.println("In get epoch");
        return -1;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        Data data = this.groups.get(serviceName);
        if (data != null)
            return data.replicas;
        else
            return null;
    }
}
