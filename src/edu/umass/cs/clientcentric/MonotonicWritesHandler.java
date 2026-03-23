package edu.umass.cs.clientcentric;

import edu.umass.cs.clientcentric.interfaces.TimestampedRequest;
import edu.umass.cs.clientcentric.interfaces.TimestampedResponse;
import edu.umass.cs.clientcentric.packets.*;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import org.json.JSONException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MonotonicWritesHandler {

    private static final Logger logger = Logger.getLogger(MonotonicWritesHandler.class.getSimpleName());

    // Shared executor for all messenger.send() calls and WriteAfterPacket handling,
    // so they never block the calling thread (Netty EventLoop or GigaPaxos NIO thread).
    private static final ExecutorService replicationExecutor = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "xdn-mw-replication");
                t.setDaemon(true);
                return t;
            });

    protected static <NodeIDType> boolean coordinateRequest(
            Request request,
            ExecutedCallback callback,
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app,
            Stringifiable<NodeIDType> nodeIdDeserializer,
            Messenger<NodeIDType, ?> messenger) {
        assert request != null : "Unspecified Request/Packet that need to be coordinated";
        assert serviceInstance != null : "Unspecified service instance";
        assert messenger != null : "Unspecified messenger for communication";

        if (request instanceof ReplicableClientRequest rcr) {
            return handleClientRequest(
                    rcr, callback, serviceInstance, app, nodeIdDeserializer, messenger);
        }

        if (request instanceof ClientCentricPacket packet) {
            handleCoordinationPacket(
                    packet, serviceInstance, app, nodeIdDeserializer, messenger);
            return true;
        }

        throw new RuntimeException("Unknown request/packet handled by " +
                "MonotonicWritesHandler that can only handle ReplicableClientRequest or " +
                "ClientCentricPacket");
    }

    private static <NodeIDType> boolean handleClientRequest(
            ReplicableClientRequest clientReplicableRequest,
            ExecutedCallback callback,
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app,
            Stringifiable<NodeIDType> nodeIdDeserializer,
            Messenger<NodeIDType, ?> messenger) {
        try {
            logger.log(Level.FINE, String.format("%s:%s - handling client request %s id=%d",
                    messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                    clientReplicableRequest.getClass().getSimpleName(),
                    clientReplicableRequest.getRequestID()));

            // Validates the client request
            Request clientRequest = clientReplicableRequest.getRequest();
            if (!(clientRequest instanceof ClientRequest) ||
                    !(clientRequest instanceof TimestampedRequest) ||
                    !(clientRequest instanceof TimestampedResponse)) {
                throw new RuntimeException("ClientCentricReplicaCoordinator can only handle " +
                        "ClientRequest with TimestampedRequest and TimestampedResponse");
            }
            if (!(clientRequest instanceof BehavioralRequest behavioralRequest)) {
                throw new RuntimeException("ClientCentricReplicaCoordinator can only handle " +
                        "BehavioralRequest");
            }
            if (!behavioralRequest.isReadOnlyRequest() &&
                    !behavioralRequest.isWriteOnlyRequest() &&
                    !behavioralRequest.isReadModifyWriteRequest()) {
                throw new RuntimeException("ClientCentricReplicaCoordinator can only handle " +
                        "ReadOnlyRequest,  WriteOnlyRequest, or ReadModifyWriteRequest.");
            }
            logger.log(Level.FINE, String.format("(1) %s:%s - client request id=%d is valid",
                    messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                    clientReplicableRequest.getRequestID()));

            // Gather the service's instance metadata
            NodeIDType myNodeID = messenger.getMyID();
            String serviceName = serviceInstance.name();
            List<String> nodeIDs = new ArrayList<>();
            for (NodeIDType nodeID : serviceInstance.nodeIDs()) {
                nodeIDs.add(nodeID.toString());
            }

            // Obtain the service latest timestamp
            VectorTimestamp serviceLastTimestamp = serviceInstance.currTimestamp();
            assert serviceLastTimestamp != null :
                    "An active service=" + serviceName + " having null timestamp";

            logger.log(Level.FINE, String.format("(2) %s:%s - client request id=%d serviceLastTimestamp not null",
                    messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                    clientReplicableRequest.getRequestID()));

            // Check the request's last write timestamp
            VectorTimestamp requestLastWriteTimestamp =
                    ((TimestampedRequest) clientRequest).getLastTimestamp("W");
            if (requestLastWriteTimestamp == null) {
                // initialize a blank timestamp
                requestLastWriteTimestamp = new VectorTimestamp(nodeIDs);
            }

            logger.log(Level.FINE, String.format("(3) %s:%s - client request id=%d has a valid requestLastWriteTimestamp",
                    messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                    clientReplicableRequest.getRequestID()));

            // Reset the client's write timestamp, if it is not comparable
            boolean isTimestampComparable = requestLastWriteTimestamp.isComparableWith(serviceLastTimestamp);
            logger.log(Level.FINE, String.format("(4) %s:%s - client request id=%d passed isComparableWith()",
                    messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                    clientReplicableRequest.getRequestID()));

            if (!isTimestampComparable) {
                // reset the client timestamp
                logger.log(Level.FINE,
                        "An active service=" + serviceName + " having incomparable timestamp." +
                                " req_timestamp=" + requestLastWriteTimestamp +
                                " svc_timestamp=" + serviceLastTimestamp +
                                ". This is possible if client is tampering with the timestamp, " +
                                "or reconfiguration happened. We are resetting client timestamp.");
                requestLastWriteTimestamp = new VectorTimestamp(nodeIDs);
            }
            assert requestLastWriteTimestamp.isComparableWith(serviceLastTimestamp);
            logger.log(Level.FINE, String.format("(5) %s:%s - client request id=%d passed isComparableWith() assertion",
                    messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                    clientReplicableRequest.getRequestID()));

            // Handle read request, directly.
            if (behavioralRequest.isReadOnlyRequest()) {
                logger.log(Level.FINE, String.format("(6) %s:%s - client request id=%d is ReadOnly",
                        messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                        clientReplicableRequest.getRequestID()));
                boolean isExecSuccess = app.execute(clientRequest, false);
                if (!isExecSuccess) {
                    logger.log(Level.FINE, "Failed to execute request: " + clientRequest);
                    return false;
                }

                // Update the client's read timestamp, then send response back to client.
                ((TimestampedResponse) clientRequest)
                        .setLastTimestamp("R", serviceLastTimestamp);
                callback.executed(clientRequest, true);
            }

            // Handle write request by first checking the timestamp.
            if (behavioralRequest.isWriteOnlyRequest() ||
                    behavioralRequest.isReadModifyWriteRequest()) {
                logger.log(Level.FINE, String.format("(7) %s:%s - client request id=%d is WriteOnly. reqTs=%s, serTs=%s",
                        messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                        clientReplicableRequest.getRequestID(), requestLastWriteTimestamp, serviceLastTimestamp));

                // Case-1: Handle write directly if this replica is "recent" enough.
                if (requestLastWriteTimestamp.isLessThanOrEqualTo(serviceLastTimestamp)) {
                    logger.log(Level.FINE, String.format("(8.1) %s:%s - client request id=%d enters Case-1",
                            messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                            clientReplicableRequest.getRequestID()));

                    boolean isExecSuccess = app.execute(clientRequest, false);
                    if (!isExecSuccess) {
                        logger.log(Level.FINE, "Failed to execute request: " + clientRequest);
                        return false;
                    }

                    // Bump up our timestamp
                    serviceLastTimestamp = serviceInstance.currTimestamp()
                            .increaseNodeTimestamp(myNodeID.toString());

                    // Update the client's write timestamp, then send response back to client.
                    ((TimestampedResponse) clientRequest)
                            .setLastTimestamp("W", serviceLastTimestamp);
                    callback.executed(clientRequest, true);

                    // Offload toBytes() + send off the critical path — client is already unblocked.
                    final VectorTimestamp finalTimestamp = serviceLastTimestamp;
                    final ClientRequest finalRequest = (ClientRequest) clientRequest;
                    final Set<NodeIDType> otherReplicas = new HashSet<>(serviceInstance.nodeIDs());
                    otherReplicas.remove(myNodeID);
                    replicationExecutor.submit(() -> {
                        // Enqueue the serialized request for sync
                        synchronized (serviceInstance.executedRequests()) {
                            serviceInstance.executedRequests().add(finalRequest.toBytes());
                        }

                        // Send write-after to peers
                        if (!otherReplicas.isEmpty()) {
                            ClientCentricWriteAfterPacket writeAfterPacket =
                                    new ClientCentricWriteAfterPacket(
                                            /*senderID=*/myNodeID.toString(),
                                            /*timestamp=*/finalTimestamp,
                                            /*clientWriteOnlyRequest=*/finalRequest);
                            GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                                    new GenericMessagingTask<>(otherReplicas.toArray(), writeAfterPacket);
                            try {
                                logger.log(Level.FINE, "Sending ClientCentricWriteAfterPacket: "
                                        + writeAfterPacket.getServiceName());
                                messenger.send(m);
                            } catch (JSONException | IOException e) {
                                logger.log(Level.WARNING,
                                        "Failed to send ClientCentricWriteAfterPacket: " + e.getMessage(), e);
                            }
                        }
                    });
                } else {
                    // Case-2: We are not "recent" enough, thus sync is needed.
                    logger.log(Level.FINE, String.format("(8.2) %s:%s - client request id=%d enters Case-2",
                            messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                            clientReplicableRequest.getRequestID()));
                    Long requestID = ((ClientRequest) clientRequest).getRequestID();

                    logger.log(Level.FINE, String.format("(8.2.1) %s:%s - Case-2 passed getRequestID() ",
                            messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName()));

                    // First, we buffer the request.
                    serviceInstance.pendingRequests().put(
                            requestID,
                            new RequestAndCallback(
                                    clientRequest, requestLastWriteTimestamp, callback));

                    logger.log(Level.FINE, String.format("(8.2.2) %s:%s - Case-2 pendingRequests().put()",
                            messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName()));

                    // Prepare sync packets to other replicas that are more recent.
                    List<GenericMessagingTask<NodeIDType, ClientCentricPacket>> syncPackets =
                            new ArrayList<>();

                    logger.log(Level.FINE, String.format("(8.2.3) %s:%s - Case-2 initialized syncPackets",
                            messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName()));

                    for (String nodeIdRaw : serviceLastTimestamp.getNodeIds()) {
                        logger.log(Level.FINE, String.format("(8.2.4) %s:%s - Case-2 for serviceLastTimestamp.getNodeIds()",
                                messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName()));
                        if (myNodeID.toString().equals(nodeIdRaw)) {
                            logger.log(Level.FINE, String.format("(8.2.4.1) %s:%s - Case-2 myNodeID.toString().equals(nodeIdRaw)",
                                    messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName()));
                            continue;
                        }

                        long clientTs = requestLastWriteTimestamp.getNodeTimestamp(nodeIdRaw);
                        long replicaTs = serviceLastTimestamp.getNodeTimestamp(nodeIdRaw);

                        logger.log(Level.FINE, String.format("(8.2.4.2) %s:%s - Case-2 clientTs=%d, replicaTs=%d",
                                messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(), clientTs, replicaTs));

                        if (clientTs > replicaTs) {
                            long startingSeqNum = replicaTs + 1;
                            ClientCentricSyncRequestPacket syncPacket =
                                    new ClientCentricSyncRequestPacket(
                                            /*senderId=*/messenger.getMyID().toString(),
                                            /*serviceName=*/serviceInstance.name(),
                                            /*fromSequenceNumber*/startingSeqNum);
                            NodeIDType targetReplicaNodeId = nodeIdDeserializer.valueOf(nodeIdRaw);
                            GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                                    new GenericMessagingTask<>(targetReplicaNodeId, syncPacket);
                            syncPackets.add(m);

                            logger.log(Level.FINE,
                                    String.format("%s:%s - preparing SyncRequestPacket to %s, clientTs=%d ourTs=%d",
                                            messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                                            nodeIdRaw, clientTs, replicaTs));
                        }
                    }

                    logger.log(Level.FINE, String.format("(8.2.5) %s:%s - Case-2 buffered request",
                            messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName()));

                    // Send the sync packets
                    for (GenericMessagingTask<NodeIDType, ClientCentricPacket> m : syncPackets) {
                        logger.log(Level.FINE, String.format("(8.2.6) %s:%s - Case-2 sending messenger...",
                                messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName()));
                        replicationExecutor.submit(() -> {
                            try {
                                messenger.send(m);
                            } catch (IOException | JSONException e) {
                                logger.log(Level.WARNING,
                                        "Failed to send ClientCentricSyncRequestPacket: " + e.getMessage(), e);
                            }
                        });
                    }
                }
            }

            return true;
        } catch (Throwable t) {
            logger.log(Level.SEVERE, "Unexpected error in handleClientRequest", t);
            throw t;  // or return false
        }
    }

    private static <NodeIDType> void handleCoordinationPacket(
            ClientCentricPacket packet,
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app,
            Stringifiable<NodeIDType> nodeIdDeserializer,
            Messenger<NodeIDType, ?> messenger) {
        assert packet != null : "packet cannot be null";
        String serviceName = packet.getServiceName();
        assert serviceName != null : "unspecified service name";
        NodeIDType myNodeID = messenger.getMyID();

        logger.log(Level.FINE, String.format("%s:%s - handling coordination packet %s id=%d",
                messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                packet.getRequestType(),
                packet.getRequestID()));

        // Handle WriteAfterPacket
        if (packet.getRequestType() == ClientCentricPacketType.CLIENT_CENTRIC_WRITE_AFTER_PACKET) {
            assert packet instanceof ClientCentricWriteAfterPacket;
            ClientCentricWriteAfterPacket writeAfterPacket = (ClientCentricWriteAfterPacket) packet;

            // Validate the sender
            String rawSenderID = writeAfterPacket.getSenderID();
            NodeIDType senderID = nodeIdDeserializer.valueOf(rawSenderID);
            assert serviceInstance.nodeIDs().contains(senderID) : "Invalid sender " + senderID;

            // Validate the timestamp
            VectorTimestamp timestamp = writeAfterPacket.getTimestamp();
            VectorTimestamp serviceLastTimestamp = serviceInstance.currTimestamp();
            assert timestamp != null && serviceLastTimestamp != null : "Invalid timestamp";
            assert timestamp.isComparableWith(serviceLastTimestamp) :
                    "Non comparable timestamp " + timestamp + " vs. " + serviceLastTimestamp;

            // Check the recency
            long theirTs = timestamp.getNodeTimestamp(rawSenderID);
            long ourTs = serviceLastTimestamp.getNodeTimestamp(rawSenderID);

            // Case-1: the propagated write is stale, or we are already "recent" enough.
            if (ourTs >= theirTs) {
                // do-nothing, ignore the stale update
                logger.log(Level.FINE,
                        String.format("%s:%s - ignoring stale write request ourTs=%s theirTs=%s",
                                messenger.getMyID(),
                                MonotonicWritesHandler.class.getSimpleName(),
                                ourTs,
                                theirTs));
                return;
            }

            // Case-2: the propagated write is most recent than our state — offload execution.
            if (ourTs == theirTs - 1) {
                replicationExecutor.submit(() -> {
                    Request clientRequest = writeAfterPacket.getClientWriteOnlyRequest();
                    boolean isExecSuccess = app.execute(clientRequest, true);
                    assert isExecSuccess : "Failed to execute request";
                    serviceInstance.currTimestamp().updateNodeTimestamp(rawSenderID, theirTs);
                    logger.log(Level.FINE,
                            String.format("%s:%s - executed the write after request, updating ts to %s",
                                    messenger.getMyID(),
                                    MonotonicWritesHandler.class.getSimpleName(),
                                    serviceInstance.currTimestamp()));
                });
                return;
            }

            // Case-3: missing some updates — offload gap-fill sync request.
            {
                long peerFromSeqNum = ourTs + 1;
                NodeIDType senderNodeId = nodeIdDeserializer.valueOf(rawSenderID);

                // prevent storming our peer with repetitive sync request if we have requested before
                // with the same fromSeqNum
                Long prevPeerRequestedFromSeqNum =
                        serviceInstance.peerLastSyncRequestSeqNum().get(senderNodeId);
                if (prevPeerRequestedFromSeqNum != null && prevPeerRequestedFromSeqNum == peerFromSeqNum) {
                    return;
                }

                ClientCentricSyncRequestPacket syncRequestPacket =
                        new ClientCentricSyncRequestPacket(
                                /*senderId=*/myNodeID.toString(),
                                /*serviceName=*/serviceInstance.name(),
                                /*fromSequenceNumber*/peerFromSeqNum);
                final GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                        new GenericMessagingTask<>(senderNodeId, syncRequestPacket);

                // cache the last requested seqNum to prevent storming the same peer with same requests
                serviceInstance.peerLastSyncRequestSeqNum().put(senderNodeId, peerFromSeqNum);

                replicationExecutor.submit(() -> {
                    try {
                        logger.log(Level.FINE,
                                String.format("%s:%s - missing updates, thus need to send ClientCentricSyncRequestPacket to %s",
                                        messenger.getMyID(),
                                        MonotonicWritesHandler.class.getSimpleName(),
                                        rawSenderID));
                        messenger.send(m);
                    } catch (IOException | JSONException e) {
                        logger.log(Level.WARNING,
                                "Failed to send gap-fill ClientCentricSyncRequestPacket: " + e.getMessage(), e);
                    }
                });
            }

            return;
        }

        // Handle SyncReqPacket
        if (packet.getRequestType() == ClientCentricPacketType.CLIENT_CENTRIC_SYNC_REQ_PACKET) {
            assert packet instanceof ClientCentricSyncRequestPacket;
            ClientCentricSyncRequestPacket syncPacket = (ClientCentricSyncRequestPacket) packet;

            // Validate the sender
            String rawSenderId = syncPacket.getSenderId();
            NodeIDType senderId = nodeIdDeserializer.valueOf(rawSenderId);
            assert serviceInstance.nodeIDs().contains(senderId) : "Invalid sender " + senderId;

            // Check the recency
            long theirReqSeq = syncPacket.getStartingSequenceNumber();
            long ourTs = serviceInstance.currTimestamp().getNodeTimestamp(myNodeID.toString());

            // The peer requesting non-existent requests
            if (theirReqSeq > ourTs) {
                // no-op
                return;
            }

            // Gather the requested write operations.
            // Note that index=0 stores seqNum=1 and timestamp starts at 1.
            long size = serviceInstance.executedRequests().size();
            List<byte[]> copiedRequests = new ArrayList<>();
            synchronized (serviceInstance.executedRequests()) {
                // synchronized is needed, otherwise the messenger could throw
                // ConcurrentModificationException when sending the response packet.
                List<byte[]> subList = serviceInstance.executedRequests().subList(
                        (int) theirReqSeq - 1, (int) size);
                for (byte[] curr : subList) {
                    byte[] copy = Arrays.copyOf(curr, curr.length);
                    copiedRequests.add(copy);
                }
            }

            // prepare the response packet
            ClientCentricSyncResponsePacket responsePacket =
                    new ClientCentricSyncResponsePacket(
                            /*senderId=*/myNodeID.toString(),
                            /*serviceName=*/serviceName,
                            /*fromSequenceNumber=*/theirReqSeq,
                            /*encodedRequests=*/copiedRequests);

            // send the response packet
            final GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                    new GenericMessagingTask<>(senderId, responsePacket);
            replicationExecutor.submit(() -> {
                try {
                    logger.log(Level.FINE, "Sending ClientCentricSyncResponsePacket: "
                            + responsePacket.getServiceName());
                    messenger.send(m);
                } catch (IOException | JSONException e) {
                    logger.log(Level.WARNING,
                            "Failed to send ClientCentricSyncResponsePacket: " + e.getMessage(), e);
                }
            });

            return;
        }

        // Handle SynResPacket
        if (packet.getRequestType() == ClientCentricPacketType.CLIENT_CENTRIC_SYNC_RES_PACKET) {
            assert packet instanceof ClientCentricSyncResponsePacket;
            ClientCentricSyncResponsePacket syncResponse = (ClientCentricSyncResponsePacket) packet;

            // validate the sender
            String rawSenderId = syncResponse.getSenderId();
            NodeIDType senderId = nodeIdDeserializer.valueOf(rawSenderId);
            assert serviceInstance.nodeIDs().contains(senderId) : "Invalid sender";

            // check the recency
            long respStartingSeqNum = syncResponse.getStartingSequenceNumber();
            long ourLatestSeqNum = serviceInstance.currTimestamp().getNodeTimestamp(rawSenderId);

            // Case-1: there are unknown ops between our latest sequence number and the given ops.
            //  For example, our sequence number is 0, but the given ops starts from seq number 5,
            //  making us missing seq num [1,4]. We do nothing for this case.
            if (respStartingSeqNum > ourLatestSeqNum + 1) {
                logger.log(Level.FINE,
                        String.format("%s:%s - detecting missing operations from %s, theirSeqNum=%d ourSeqNum=%d",
                                messenger.getMyID(), MonotonicWritesHandler.class.getSimpleName(),
                                senderId, respStartingSeqNum, ourLatestSeqNum));
                return;
            }

            // Case-2: we can execute the ops from our peers, we update our timestamp
            if (respStartingSeqNum <= ourLatestSeqNum + 1 ) {
                List<byte[]> writeOps = syncResponse.getEncodedRequests();
                long lastSeqNum = respStartingSeqNum + writeOps.size() - 1;
                for (long currSeqNum = respStartingSeqNum; currSeqNum <= lastSeqNum; ++currSeqNum) {
                    // skip the already executed write operations
                    if (currSeqNum <= ourLatestSeqNum) {
                        continue;
                    }

                    // decode the write operation, then execute it
                    try {
                        int idx = (int) (currSeqNum - respStartingSeqNum);
                        byte[] encodedReq = writeOps.get(idx);
                        Request appReq = app.getRequest(
                                new String(encodedReq, StandardCharsets.ISO_8859_1));
                        boolean isExecSuccess = app.execute(appReq, false);
                        assert isExecSuccess : "Failed to execute request";
                    } catch (RequestParseException e) {
                        throw new RuntimeException(e);
                    }
                }

                // Update our timestamp
                serviceInstance.currTimestamp().updateNodeTimestamp(rawSenderId, lastSeqNum);

                // Process the buffered write requests, if any.
                processBufferedWriteRequests(serviceInstance, app);

                return;
            }

            throw new RuntimeException("Illegal: this should not happen");
        }

        throw new IllegalStateException("Unexpected ClientCentricPacket: " +
                packet.getRequestType());
    }

    private static <NodeIDType> void processBufferedWriteRequests(
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app) {
        if (serviceInstance.pendingRequests().isEmpty()) return;

        VectorTimestamp serviceLastTimestamp = serviceInstance.currTimestamp();
        Map<Long, RequestAndCallback> pendingWriteRequests =
                serviceInstance.pendingRequests();

        // Iterate through all the pending write requests and execute them if our current
        // timestamp is already "recent" enough.
        Iterator<Map.Entry<Long, RequestAndCallback>> it =
                pendingWriteRequests.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, RequestAndCallback> e = it.next();
            RequestAndCallback clientWriteRequestAndMetadata = e.getValue();
            VectorTimestamp clientLastWriteTimestamp = clientWriteRequestAndMetadata.timestamp();

            // validate the request and replica timestamp
            if (!serviceLastTimestamp.isComparableWith(clientLastWriteTimestamp)) {
                logger.log(Level.FINE,
                        "Incomparable client and replica timestamp: " +
                                clientLastWriteTimestamp + " vs. " + serviceLastTimestamp);
                continue;
            }

            // still buffer the write request if our replica is not "recent" enough
            if (serviceLastTimestamp.isLessThan(clientLastWriteTimestamp)) {
                continue;
            }

            // execute the write request since we are already "recent" enough
            Request clientReadRequest = clientWriteRequestAndMetadata.request();
            ExecutedCallback callback = clientWriteRequestAndMetadata.callback();
            assert (clientReadRequest instanceof BehavioralRequest br) &&
                    ((br.isWriteOnlyRequest() || br.isReadModifyWriteRequest()))
                    : "Unexpected non WriteOnlyRequest nor ReadModifyWriteRequest " +
                    "is buffered in MonotonicWritesHandler";
            boolean isExecSuccess = app.execute(clientReadRequest, false);
            if (!isExecSuccess) {
                logger.log(Level.FINE,
                        "Failed to execute write request: " + clientReadRequest);
                continue;
            }

            // Append to executedRequests so peers can sync this write — this was missing
            // in the original, causing writes buffered through Case-2 to be invisible
            // to peers requesting a sync.
            synchronized (serviceInstance.executedRequests()) {
                serviceInstance.executedRequests().add(
                        ((ClientRequest) clientReadRequest).toBytes());
            }

            // send response back to client
            assert clientReadRequest instanceof TimestampedResponse;
            ((TimestampedResponse) clientReadRequest).setLastTimestamp(
                    "W", serviceLastTimestamp);
            callback.executed(clientReadRequest, true);

            // remove this executed write request from the buffered request
            it.remove();
        }
    }

}