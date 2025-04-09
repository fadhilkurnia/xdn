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
import edu.umass.cs.xdn.request.XdnHttpRequest;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * AwReplicaCoordinator implements a protocol providing sequential consistency inspired
 * from the paper "Sequential Consistency versus Linearizability" by Hagit Attiya and
 * Jennifer L Welch. AW is the initial of the authors' last name. Specifically, the protocol
 * refers to the read-write object that does local execution for read-only operations, and
 * uses MultiPaxos as the Atomic Broadcast (ABC) for write-only (and read-modify-write) operations.
 */
public class AwReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIdDeserializer;
    private final PaxosManager<NodeIDType> paxosManager;
    private final Replicable app;

    private final Logger logger = Logger.getGlobal();

    public AwReplicaCoordinator(Replicable app,
                                NodeIDType myID,
                                Stringifiable<NodeIDType> nodeIdDeserializer,
                                Messenger<NodeIDType, JSONObject> messenger,
                                PaxosManager<NodeIDType> paxosManager) {
        super(app, messenger);
        this.myNodeID = myID;
        this.nodeIdDeserializer = nodeIdDeserializer;
        this.paxosManager = paxosManager;
        this.app = app;

        assert messenger.getMyID().equals(myID) : "Invalid node ID given in the messenger";
        assert this.nodeIdDeserializer.valueOf(this.myNodeID.toString()).equals(this.myNodeID)
                : "Invalid node ID deserializer given";

    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return null;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        if (!(request instanceof ReplicableClientRequest rcr)) {
            throw new RuntimeException("Unknown request/packet handled by AwReplicaCoordinator " +
                    request.getRequestType());
        }
        Request clientRequest = rcr.getRequest();
        String serviceName = clientRequest.getServiceName();

        String requestLogText = "";
        if (clientRequest instanceof XdnHttpRequest xdnHttpRequest) {
            requestLogText = xdnHttpRequest.getLogText();
        } else {
            requestLogText = clientRequest.getRequestType().toString();
        }
        logger.log(Level.INFO, String.format("%s:AwReplicaCoordinator -- coordinateRequest %s\n",
                this.myNodeID, requestLogText));

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
        if (clientRequest instanceof BehavioralRequest br && br.isMonotonicRequest()) {
            this.paxosManager.propose(serviceName, clientRequest, null);
            boolean isExecSuccess = this.app.execute(clientRequest);
            if (isExecSuccess) {
                callback.executed(clientRequest, true);
            }
            return isExecSuccess;
        }

        // Most requests, especially write-only and read-modify-write requests need to
        // be coordinated. Note that the code below is asynchronous, we wait for the
        // coordination confirmation before returning to client.
        ExecutedCallback chainedCallback = (executedRequest, handled) -> {
            System.out.println(">> executed");
            callback.executed(executedRequest, handled);
        };
        System.out.println(">>> " + rcr.getClientAddress());
        this.paxosManager.propose(serviceName, rcr, chainedCallback);

        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes, String placementMetadata) {
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
        Request stopRequest = this.getStopRequest(serviceName, epoch);
        this.paxosManager.proposeStop(serviceName, epoch, stopRequest,
                (executedStopRequest, isHandled) ->
                        paxosManager.deleteStoppedPaxosInstance(serviceName, epoch));
        return true;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.paxosManager.getReplicaGroup(serviceName);
    }

    public boolean isPaxosCoordinator(String serviceName) {
        assert serviceName != null : "Service name cannot be null";
        return this.paxosManager.isPaxosCoordinator(serviceName);
    }

}
