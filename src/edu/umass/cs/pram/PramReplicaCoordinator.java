package edu.umass.cs.pram;

import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.pram.packets.*;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * PramReplicaCoordinator is a generic class to handle replica node of type NodeIDType to replicate
 * application using the PRAM protocol, offering PRAM/FIFO consistency model. The protocol is
 * implemented based on
 * <a href="https://www.cs.princeton.edu/techreports/1988/180.pdf">this paper</a>.
 *
 * @param <NodeIDType>
 */
public class PramReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIdStringer;
    private final Set<IntegerPacketType> requestTypes;

    private final Messenger<NodeIDType, JSONObject> messenger;

    private record PramInstance<NodeIDType>
            (String serviceName,
             int currentEpoch,
             String initStateSnapshot,
             Set<NodeIDType> nodes,

             // incoming-queue for each node in the replica group
             ConcurrentMap<NodeIDType, ConcurrentLinkedQueue<PramWriteAfterPacket>> replicaQueue) {
    }

    private final ConcurrentMap<String, PramInstance<NodeIDType>> currentInstances;

    // Dedicated executor for sending WRITE_AFTER packets to peers.
    // Offloaded from the calling thread so messenger.send() never blocks the hot path.
    private final ExecutorService replicationExecutor = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "xdn-pram-replication");
                t.setDaemon(true);
                return t;
            });

    // Per-sender single-thread executor map — preserves FIFO order per sender
    // without spawning a new OS thread per packet.
    private final ConcurrentMap<NodeIDType, ExecutorService> senderExecutors =
            new ConcurrentHashMap<>();

    private Logger logger = Logger.getLogger(PramReplicaCoordinator.class.getName());

    public PramReplicaCoordinator(Replicable app,
                                  NodeIDType myID,
                                  Stringifiable<NodeIDType> nodeIdStringer,
                                  Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        assert nodeIdStringer != null : "nodeIdStringer cannot be null";
        this.myNodeID = myID;
        this.nodeIdStringer = nodeIdStringer;
        this.messenger = messenger;
        this.currentInstances = new ConcurrentHashMap<>();

        // initialize all the supported packet types
        Set<IntegerPacketType> types = new HashSet<>(app.getRequestTypes());
        types.addAll(List.of(PramPacketType.values()));
        this.requestTypes = types;

        // prepare packet deserializer and handler for PramPacket
        this.messenger.precedePacketDemultiplexer(
                new PramPacketDemultiplexer(this, app));
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return requestTypes;
    }

    public static Set<IntegerPacketType> getAllPramRequestTypes() {
        return new HashSet<>(List.of(PramPacketType.values()));
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        logger.log(Level.FINE, String.format("%s:%s - receiving request %s",
                myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                request.getClass().getSimpleName()));
        if (!(request instanceof ReplicableClientRequest) && !(request instanceof PramPacket)) {
            throw new RuntimeException("Unknown request/packet handled by PramReplicaCoordinator");
        }

        // Convert the incoming Request into PramPacket
        PramPacket packet;
        if (request instanceof ReplicableClientRequest rcr &&
                rcr.getRequest() instanceof ClientRequest clientRequest) {
            boolean isWriteOnly =
                    (clientRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest());
            boolean isReadOnly =
                    (clientRequest instanceof BehavioralRequest br && br.isReadOnlyRequest());
            if (isReadOnly) {
                packet = new PramReadPacket(clientRequest);
            } else if (isWriteOnly) {
                packet = new PramWritePacket(clientRequest);
            } else {
                throw new RuntimeException("PramReplicaCoordinator can only handle " +
                        "WriteOnlyRequest or ReadOnlyRequest");
            }
        } else if (request instanceof ReplicableClientRequest rcr &&
                rcr.getRequest() instanceof PramPacket pp) {
            packet = pp;
        } else if (request instanceof ReplicableClientRequest rcr &&
                rcr.getRequest() instanceof ReconfigurableRequest rcRequest &&
                rcRequest.isStop()) {
            // handle stop epoch packet
            boolean isSuccess = this.app.restore(rcr.getServiceName(), null);
            callback.executed(rcRequest, isSuccess);
            return true;
        } else {
            assert request instanceof PramPacket :
                    "The received request must be ReplicableClientRequest or PramPacket, found " +
                            request.getClass().getSimpleName();
            packet = (PramPacket) request;
        }

        return handlePramPacket(packet, callback);
    }

    private boolean handlePramPacket(PramPacket packet, ExecutedCallback callback) {
        if (packet instanceof PramReadPacket p) {
            ClientRequest readRequest = p.getClientReadRequest();
            logger.log(Level.FINER, String.format("%s:%s - handling read request %s",
                    myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                    readRequest.getClass().getSimpleName()));
            boolean isExecSuccess = app.execute(readRequest);
            if (isExecSuccess) {
                stampAll(readRequest, XdnHttpRequest.TS_CALLBACK);
                callback.executed(readRequest, true);
            }
            return isExecSuccess;
        }

        if (packet instanceof PramWritePacket p) {
            ClientRequest writeRequest = p.getClientWriteRequest();
            logger.log(Level.FINER, String.format("%s:%s - handling write request %s",
                    myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                    writeRequest.getClass().getSimpleName()));
            boolean isExecSuccess = app.execute(writeRequest);
            if (isExecSuccess) {
                stampAll(writeRequest, XdnHttpRequest.TS_CALLBACK);
                callback.executed(writeRequest, true);

                // Offload messenger.send() to the replication executor so it does not
                // block the calling thread. The client response has already been dispatched
                // above; peers do not need to be notified before we return.
                String serviceName = writeRequest.getServiceName();
                assert this.currentInstances.containsKey(serviceName) :
                        "Unknown service name " + serviceName;
                final Set<NodeIDType> peers = new HashSet<>(
                        this.currentInstances.get(serviceName).nodes());
                peers.remove(myNodeID);
                if (!peers.isEmpty()) {
                    replicationExecutor.submit(() -> {
                        PramPacket writeAfterPacket =
                                new PramWriteAfterPacket(myNodeID.toString(), writeRequest);
                        GenericMessagingTask<NodeIDType, PramPacket> m =
                                new GenericMessagingTask<>(peers.toArray(), writeAfterPacket);
                        try {
                            logger.log(Level.FINER,
                                    String.format("%s:%s - sending WRITE_AFTER packet of %s",
                                            myNodeID,
                                            PramReplicaCoordinator.class.getSimpleName(),
                                            writeRequest.getClass().getSimpleName()));
                            messenger.send(m);
                        } catch (JSONException | IOException e) {
                            logger.log(Level.WARNING,
                                    "Failed to send WRITE_AFTER packet: " + e.getMessage(), e);
                        }
                    });
                }
            }
            return isExecSuccess;
        }

        if (packet instanceof PramWriteAfterPacket p) {
            logger.log(Level.FINER,
                    String.format("%s:%s - handling write after packet %s name=%s sender=%s",
                            myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                            p.getClientWriteRequest().getClass().getSimpleName(),
                            p.getServiceName(),
                            p.getSenderID()));

            // get the sender ID
            final String senderIdString = p.getSenderID();
            final NodeIDType senderID = nodeIdStringer.valueOf(senderIdString);
            assert senderID != null : "Failed to convert string into NodeIDType";

            // find the service using its name
            String serviceName = p.getServiceName();
            assert this.currentInstances.containsKey(serviceName) :
                    "Unknown service name " + serviceName;
            PramInstance<NodeIDType> instance = this.currentInstances.get(serviceName);

            // Enqueue packet into the per-sender queue so FIFO order is preserved.
            instance.replicaQueue.putIfAbsent(senderID, new ConcurrentLinkedQueue<>());
            Queue<PramWriteAfterPacket> replicaQueue = instance.replicaQueue.get(senderID);
            boolean isAdded = replicaQueue.add(p);
            assert isAdded : "Failed to enqueue the write request from " + senderIdString;

            // Submit execution to a per-sender single-thread executor.
            // A single-thread executor per sender gives us two properties:
            //   1. FIFO ordering — tasks run in submission order.
            //   2. No thread-per-packet overhead — threads are reused across packets.
            // synchronized(replicaQueue) is no longer needed because the single-thread
            // executor serializes access by construction.
            ExecutorService senderExecutor = senderExecutors.computeIfAbsent(senderID,
                    id -> Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r, "xdn-pram-write-after-" + id);
                        t.setDaemon(true);
                        return t;
                    }));

            senderExecutor.submit(() -> {
                while (!replicaQueue.isEmpty()) {
                    PramWriteAfterPacket writePacket = replicaQueue.poll();
                    if (writePacket == null) break;
                    ClientRequest appRequest = writePacket.getClientWriteRequest();
                    if (appRequest instanceof XdnHttpRequest xhr) {
                        xhr.clearHttpResponse();
                    } else if (appRequest instanceof XdnHttpRequestBatch batch) {
                        for (XdnHttpRequest xhr : batch.getRequestList()) {
                            xhr.clearHttpResponse();
                        }
                    }
                    boolean isExecuted = app.execute(appRequest);
                    assert isExecuted :
                            "failed to execute write request from " + senderIdString;
                }
            });

            return true;
        }

        throw new IllegalStateException("Unexpected PramPacket: " + packet.getRequestType());
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes, String placementMetadata) {
        logger.log(Level.INFO,
                String.format("%s:%s - creating replica group name=%s nodes=%s epoch=%d state=%s",
                        myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                        serviceName, nodes, epoch, state));
        PramInstance<NodeIDType> pramInstance =
                new PramInstance<>(serviceName, epoch, state, nodes, new ConcurrentHashMap<>());
        this.currentInstances.put(serviceName, pramInstance);

        // Creating a replica group is a special case for reconfiguration where we reconfigure
        // from nothing to something. In that case, we call app.restore(.) with initialState.
        return this.app.restore(serviceName, state);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        if (!this.currentInstances.containsKey(serviceName)) return true;
        PramInstance<NodeIDType> pramInstance = this.currentInstances.get(serviceName);
        if (pramInstance.currentEpoch != epoch) return true;
        this.currentInstances.remove(serviceName);

        // Deleting a replica group is a special case for reconfiguration where we reconfigure
        // from something into nothing. In that case, we call app.restore(.) with null state.
        return this.app.restore(serviceName, null);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        PramInstance<NodeIDType> pramInstance = this.currentInstances.get(serviceName);
        return pramInstance != null ? pramInstance.nodes() : null;
    }

    private void stampAll(Request request, int stage) {
        if (!XdnHttpRequest.ENABLE_LATENCY_TRACING) return;
        if (request instanceof XdnHttpRequestBatch batch) {
            for (XdnHttpRequest xhr : batch.getRequestList()) xhr.stamp(stage);
        } else if (request instanceof XdnHttpRequest xhr) {
            xhr.stamp(stage);
        }
    }
}