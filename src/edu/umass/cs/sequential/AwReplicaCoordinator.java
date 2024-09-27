package edu.umass.cs.sequential;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;

/**
 * AwReplicaCoordinator implements a protocol providing sequential consistency inspired
 * from the paper "Sequential Consistency versus Linearizability" by Hagit Attiya and
 * Jennifer L Welch. AW is the initial of the authors' last name. Specifically, the protocol
 * refers to the read-write object that does local execution for read-only operations, and
 * uses MultiPaxos as the Atomic Broadcast (ABC) for write-only (and read-modify-write) operations.
 * <p>
 * TODO: Implement Me!
 */
public class AwReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final NodeIDType myNodeID;
    private final PaxosManager<NodeIDType> paxosManager;
    private final Replicable app;

    public AwReplicaCoordinator(Replicable app,
                                NodeIDType myID,
                                Stringifiable<NodeIDType> unstringer,
                                Messenger<NodeIDType, JSONObject> messenger,
                                PaxosManager<NodeIDType> paxosManager) {
        super(app, messenger);
        this.myNodeID = myID;
        this.paxosManager = paxosManager;
        this.app = app;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return null;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        System.out.printf(">> %s:AwReplicaCoordinator -- coordinateRequest %s\n",
                this.myNodeID, request);
        if (!(request instanceof ReplicableClientRequest rcr)) {
            throw new RuntimeException("Unknown request/packet handled by AwReplicaCoordinator");
        }
        Request clientRequest = rcr.getRequest();
        String serviceName = clientRequest.getServiceName();

        // We handle read-only request locally, with no coordination.
        if (clientRequest instanceof BehavioralRequest br && br.isReadOnlyRequest()) {
            boolean isExecSuccess = this.app.execute(clientRequest);
            if (isExecSuccess) {
                callback.executed(clientRequest, true);
            }
            return isExecSuccess;
        }

        // Nil-external request is coordinated, but without waiting for the coordination
        // confirmation. We directly acknowledge back to the client.
        // This implementation refers to the "Sequential Consistency versus Linearizability" paper,
        // specifically for Enqueue and Push operation in FIFO Queue and Stack, respectively.
        if (clientRequest instanceof BehavioralRequest br && br.isNilExternal()) {
            this.paxosManager.propose(serviceName, clientRequest, null);
            callback.executed(clientRequest, true);
            return true;
        }

        // Most requests, especially write-only and read-modify-write requests need to
        // be coordinated. We wait for the coordination confirmation before returning to client.
        this.paxosManager.propose(serviceName, clientRequest, (executedRequest, isHandled) -> {
            // System.out.println(">> execution result: " + executedRequest);
            callback.executed(executedRequest, isHandled);
        });
        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes) {
        boolean isCreated = this.paxosManager.createPaxosInstanceForcibly(
                serviceName, epoch, nodes, this.app, state, 5000);
        boolean isAlreadyExist = this.paxosManager.equalOrHigherVersionExists(serviceName, epoch);

        if (!isCreated && !isAlreadyExist) {
            throw new PaxosInstanceCreationException((this
                    + " failed to create " + serviceName + ":" + epoch
                    + " with state [" + state + "]") + "; existing_version=" +
                    this.paxosManager.getVersion(serviceName));
        }

        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        this.paxosManager.proposeStop(serviceName, epoch, "stop",
                (executedStopRequest, isHandled) ->
                        paxosManager.deleteStoppedPaxosInstance(serviceName, epoch));
        return true;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.paxosManager.getReplicaGroup(serviceName);
    }
}
