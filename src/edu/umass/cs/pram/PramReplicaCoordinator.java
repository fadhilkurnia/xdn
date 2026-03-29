package edu.umass.cs.pram;

import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.pram.packets.*;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final int DEFAULT_EXECUTE_WORKERS =
            Math.max(32, Runtime.getRuntime().availableProcessors() * 4);

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIdStringer;
    private final Set<IntegerPacketType> requestTypes;

    private final Messenger<NodeIDType, JSONObject> messenger;
    private final ExecutorService executePool;

    private record PramInstance<NodeIDType>
            (String serviceName,
             int currentEpoch,
             String initStateSnapshot,
             Set<NodeIDType> nodes,

             // incoming-queue for each node in the replica group
             ConcurrentMap<NodeIDType, ConcurrentLinkedQueue<PramWriteAfterPacket>> replicaQueue,

             // per-sender flag: true when an executePool task is draining that sender's queue
             ConcurrentMap<NodeIDType, AtomicBoolean> replicaProcessing) {
    }

    private final ConcurrentMap<String, PramInstance<NodeIDType>> currentInstances;

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

        int nWorkers = Integer.getInteger("PRAM_N_EXECUTE_WORKERS", DEFAULT_EXECUTE_WORKERS);
        this.executePool = Executors.newFixedThreadPool(nWorkers, r -> {
            Thread t = new Thread(r, "pram-exec-" + myID);
            t.setDaemon(true);
            return t;
        });

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
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, String.format("%s:%s - receiving request %s",
                    myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                    request.getClass().getSimpleName()));
        }
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
            if (logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER, String.format("%s:%s - handling read request %s",
                        myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                        readRequest.getClass().getSimpleName()));
            }
            // Read-only requests need no coordination — execute inline on the
            // caller thread to avoid thread-pool dispatch overhead.
            boolean isExecSuccess = app.execute(readRequest);
            callback.executed(readRequest, isExecSuccess);
            return true;
        }

        if (packet instanceof PramWritePacket p) {
            ClientRequest writeRequest = p.getClientWriteRequest();
            if (logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER, String.format("%s:%s - handling write request %s",
                        myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                        writeRequest.getClass().getSimpleName()));
            }
            // Write requests are fire-and-forget — execute inline, send
            // WRITE_AFTER BEFORE responding (to ensure ByteBufs are still
            // valid during serialization), then respond to client.
            boolean isExecSuccess = app.execute(writeRequest);
            if (!isExecSuccess) {
                callback.executed(writeRequest, false);
                return true;
            }

            String serviceName = writeRequest.getServiceName();
            assert this.currentInstances.containsKey(serviceName) :
                    "Unknown service name " + serviceName;
            Set<NodeIDType> nodes = new HashSet<>(this.currentInstances.get(serviceName).nodes());
            nodes.remove(myNodeID);
            PramPacket writeAfterPacket = new PramWriteAfterPacket(myNodeID.toString(), writeRequest);
            GenericMessagingTask<NodeIDType, PramPacket> m =
                    new GenericMessagingTask<>(nodes.toArray(), writeAfterPacket);
            try {
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, String.format("%s:%s - sending WRITE_AFTER packet of %s",
                            myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                            writeRequest.getClass().getSimpleName()));
                }
                messenger.send(m);
            } catch (JSONException | IOException e) {
                throw new RuntimeException(e);
            }

            callback.executed(writeRequest, true);

            return true;
        }

        if (packet instanceof PramWriteAfterPacket p) {
            if (logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER,
                        String.format("%s:%s - handling write after packet %s name=%s sender=%s",
                                myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                                p.getClientWriteRequest().getClass().getSimpleName(),
                                p.getServiceName(),
                                p.getSenderID()));
            }

            // get the sender ID
            final String senderIdString = p.getSenderID();
            final NodeIDType senderID = nodeIdStringer.valueOf(senderIdString);
            assert senderID != null : "Failed to convert string into NodeIDType";

            // find the service using its name
            String serviceName = p.getServiceName();
            assert this.currentInstances.containsKey(serviceName) :
                    "Unknown service name " + serviceName;
            PramInstance<NodeIDType> instance = this.currentInstances.get(serviceName);

            // PRAM requires per-sender FIFO execution. We use a lock-free
            // ConcurrentLinkedQueue for buffering and an AtomicBoolean "processing"
            // flag per sender to ensure only one executePool task drains the queue
            // at a time — without holding a synchronized lock during app.execute().
            instance.replicaQueue.putIfAbsent(senderID, new ConcurrentLinkedQueue<>());
            instance.replicaProcessing.putIfAbsent(senderID, new AtomicBoolean(false));
            ConcurrentLinkedQueue<PramWriteAfterPacket> replicaQueue =
                    instance.replicaQueue.get(senderID);
            AtomicBoolean processing = instance.replicaProcessing.get(senderID);
            replicaQueue.add(p);

            // Try to become the sole processor for this sender's queue.
            // If another task is already processing, it will pick up our
            // packet when it loops back to check the queue.
            if (processing.compareAndSet(false, true)) {
                drainSenderQueue(replicaQueue, processing);
            }

            return true;
        }

        throw new IllegalStateException("Unexpected PramPacket: " + packet.getRequestType());
    }

    /**
     * Drain a sender's write-after queue on the executePool, preserving FIFO order.
     *
     * Only one executePool task runs per sender at a time (guarded by the
     * AtomicBoolean flag). The task polls and executes packets one by one,
     * then releases the flag. A re-check after releasing prevents lost packets
     * from the add-then-CAS race window.
     */
    private void drainSenderQueue(ConcurrentLinkedQueue<PramWriteAfterPacket> queue,
                                  AtomicBoolean processing) {
        executePool.execute(() -> {
            try {
                PramWriteAfterPacket writePacket;
                while ((writePacket = queue.poll()) != null) {
                    ClientRequest appRequest = writePacket.getClientWriteRequest();
                    // Clear stale response from sender's execution so
                    // forwardHttpRequestToContainerizedService can set a fresh one.
                    if (appRequest instanceof XdnHttpRequest xhr && xhr.getHttpResponse() != null) {
                        xhr.clearHttpResponse();
                    }
                    boolean isExecuted = app.execute(appRequest);
                    assert isExecuted : "failed to execute write-after request";
                }
            } finally {
                processing.set(false);
                // A packet may have been added between poll() returning null
                // and clearing the flag. Re-check to avoid dangling packets.
                if (!queue.isEmpty() && processing.compareAndSet(false, true)) {
                    drainSenderQueue(queue, processing);
                }
            }
        });
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes, String placementMetadata) {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO,
                    String.format("%s:%s - creating replica group name=%s nodes=%s epoch=%d state=%s",
                            myNodeID, PramReplicaCoordinator.class.getSimpleName(),
                            serviceName, nodes, epoch, state));
        }
        PramInstance<NodeIDType> pramInstance =
                new PramInstance<>(serviceName, epoch, state, nodes,
                        new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
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

    private void retainRequestContent(Request request) {
        if (request instanceof XdnHttpRequest xhr) {
            if (xhr.getHttpRequestContent() != null) {
                ByteBuf content = xhr.getHttpRequestContent().content();
                if (content != null && content.refCnt() > 0) {
                    ReferenceCountUtil.retain(content);
                }
            }
        }
    }

    private void releaseRequestContent(Request request) {
        if (request instanceof XdnHttpRequest xhr) {
            if (xhr.getHttpRequestContent() != null) {
                ByteBuf content = xhr.getHttpRequestContent().content();
                if (content != null && content.refCnt() > 0) {
                    ReferenceCountUtil.release(content);
                }
            }
        }
    }
}
