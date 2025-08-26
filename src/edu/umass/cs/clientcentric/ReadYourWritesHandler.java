package edu.umass.cs.clientcentric;

import edu.umass.cs.clientcentric.interfaces.TimestampedRequest;
import edu.umass.cs.clientcentric.interfaces.TimestampedResponse;
import edu.umass.cs.clientcentric.packets.*;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import org.json.JSONException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadYourWritesHandler {

    private static final Logger logger = Logger.getGlobal();

    protected static <NodeIDType> boolean coordinateRequest(
            Request request,
            ExecutedCallback callback,
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app,
            Stringifiable<NodeIDType> nodeIdDeserializer,
            Messenger<NodeIDType, ?> messenger) {
        assert request != null : "Unspecified Request/ClientCentricPacket";
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
                "ReadYourWritesHandler that can only handle ReplicableClientRequest or " +
                "ClientCentricPacket");
    }

    private static <NodeIDType> boolean handleClientRequest(
            ReplicableClientRequest clientReplicableRequest,
            ExecutedCallback callback,
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app,
            Stringifiable<NodeIDType> nodeIdDeserializer,
            Messenger<NodeIDType, ?> messenger) {
        // Validate client request
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

        // Check the request's last write timestamp
        VectorTimestamp requestLastWriteTimestamp =
                ((TimestampedRequest) clientRequest).getLastTimestamp("W");
        if (requestLastWriteTimestamp == null) {
            // initialize a blank timestamp
            requestLastWriteTimestamp = new VectorTimestamp(nodeIDs);
        }

        logger.log(Level.INFO, String.format("%s:%s - handling client request %s id=%d clientWriteTs=%s ourTs=%s",
                messenger.getMyID(), ReadYourWritesHandler.class.getSimpleName(),
                clientReplicableRequest.getClass().getSimpleName(),
                clientReplicableRequest.getRequestID(),
                requestLastWriteTimestamp,
                serviceLastTimestamp));

        // Reset the client's write timestamp, if it is not comparable
        boolean isTimestampComparable = requestLastWriteTimestamp.isComparableWith(serviceLastTimestamp);
        if (!isTimestampComparable) {
            // reset the client timestamp
            logger.log(Level.WARNING,
                    "An active service=" + serviceName + " having incomparable timestamp." +
                            " req_timestamp=" + requestLastWriteTimestamp +
                            " svc_timestamp=" + serviceLastTimestamp +
                            ". This is possible if client is tampering with the timestamp, " +
                            "or reconfiguration happened. We are resetting client timestamp.");
            requestLastWriteTimestamp = new VectorTimestamp(nodeIDs);
        }
        assert requestLastWriteTimestamp.isComparableWith(serviceLastTimestamp);

        // Handle read request
        if (behavioralRequest.isReadOnlyRequest()) {
            // Case-1: this replica is "recent" enough based on the client's write timestamp,
            //  so execute the read request locally.
            if (requestLastWriteTimestamp.isLessThanOrEqualTo(serviceLastTimestamp)) {
                boolean isExecSuccess = app.execute(clientRequest, false);
                if (!isExecSuccess) {
                    logger.log(Level.WARNING, "Failed to execute request: " + clientRequest);
                    return false;
                }

                // update the client's read timestamp
                ((TimestampedResponse) clientRequest)
                        .setLastTimestamp("R", serviceLastTimestamp);

                // send response back to client
                callback.executed(clientRequest, true);
                return true;
            }

            // Case-2: this replica is not "recent" enough, so we need to sync the write requests
            //  from other replicas.
            {
                // First, we buffer the request.
                Long requestID = ((ClientRequest) clientRequest).getRequestID();
                serviceInstance.pendingRequests().put(
                        requestID,
                        new RequestAndCallback(
                                clientRequest, requestLastWriteTimestamp, callback));

                // Then, prepare sync packets to other replicas that are more recent.
                List<GenericMessagingTask<NodeIDType, ClientCentricPacket>> syncPackets =
                        new ArrayList<>();
                for (String nodeIdRaw : serviceLastTimestamp.getNodeIds()) {
                    if (messenger.getMyID().toString().equals(nodeIdRaw)) continue;
                    long clientTs = requestLastWriteTimestamp.getNodeTimestamp(nodeIdRaw);
                    long replicaTs = serviceLastTimestamp.getNodeTimestamp(nodeIdRaw);
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

                        logger.log(Level.INFO,
                                String.format("%s:%s - preparing SyncRequestPacket to %s, clientTs=%d ourTs=%d",
                                        messenger.getMyID(), ReadYourWritesHandler.class.getSimpleName(),
                                        nodeIdRaw, clientTs, replicaTs));
                    }
                }

                // Finally, send the sync packets.
                for (GenericMessagingTask<NodeIDType, ClientCentricPacket> m : syncPackets) {
                    try {
                        messenger.send(m);
                    } catch (IOException | JSONException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        // Handle write request, directly.
        if (behavioralRequest.isWriteOnlyRequest() ||
                behavioralRequest.isReadModifyWriteRequest()) {
            // execute the write request
            boolean isExecSuccess = app.execute(clientRequest, false);
            if (!isExecSuccess) {
                logger.log(Level.WARNING, "Failed to execute request: " + clientRequest);
                return false;
            }

            // Enqueue the executed request
            synchronized (serviceInstance.executedRequests()) {
                serviceInstance.executedRequests().add(clientRequest.toBytes());
            }

            // Bump up the service's current timestamp
            serviceLastTimestamp = serviceInstance.currTimestamp()
                    .increaseNodeTimestamp(myNodeID.toString());

            // Send response back to client, along with the service's latest timestamp
            ((TimestampedResponse) clientRequest)
                    .setLastTimestamp("W", serviceLastTimestamp);
            callback.executed(clientRequest, true);

            // asynchronously send the writes to other replicas
            Set<NodeIDType> otherReplicas = new HashSet<>(serviceInstance.nodeIDs());
            otherReplicas.remove(myNodeID);
            ClientCentricWriteAfterPacket writeAfterPacket =
                    new ClientCentricWriteAfterPacket(
                            /*senderID=*/myNodeID.toString(),
                            /*timestamp=*/serviceLastTimestamp,
                            /*clientWriteOnlyRequest=*/(ClientRequest) clientRequest);
            GenericMessagingTask<NodeIDType, JSONPacket> m =
                    new GenericMessagingTask<>(otherReplicas.toArray(), writeAfterPacket);
            try {
                logger.log(Level.FINER, "Sending ClientCentricWriteAfterPacket: "
                        + writeAfterPacket.getServiceName());
                messenger.send(m);
            } catch (JSONException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    private static <NodeIDType> void handleCoordinationPacket(
            ClientCentricPacket packet,
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app,
            Stringifiable<NodeIDType> nodeIdDeserializer,
            Messenger<NodeIDType, ?> messenger) {
        NodeIDType myNodeID = messenger.getMyID();
        String serviceName = packet.getServiceName();

        // Handle WriteAfterPacket
        if (packet.getRequestType() == ClientCentricPacketType.CLIENT_CENTRIC_WRITE_AFTER_PACKET) {
            assert packet instanceof ClientCentricWriteAfterPacket;
            ClientCentricWriteAfterPacket writeAfterPacket = (ClientCentricWriteAfterPacket) packet;

            // validate the sender
            String rawSenderID = writeAfterPacket.getSenderID();
            NodeIDType senderID = nodeIdDeserializer.valueOf(rawSenderID);
            assert serviceInstance.nodeIDs().contains(senderID) : "Invalid sender";

            // validate the timestamp
            VectorTimestamp timestamp = writeAfterPacket.getTimestamp();
            VectorTimestamp serviceLastTimestamp = serviceInstance.currTimestamp();
            assert timestamp != null && serviceLastTimestamp != null : "Invalid timestamp";
            assert timestamp.isComparableWith(serviceLastTimestamp) : "Non comparable timestamp";

            // check the recency
            long theirTs = timestamp.getNodeTimestamp(rawSenderID);
            long ourTs = serviceLastTimestamp.getNodeTimestamp(rawSenderID);

            // Case-1: the propagated write is stale, or we are already "recent" enough.
            if (ourTs >= theirTs) {
                // do-nothing, ignore the stale update
                return;
            }

            // Case-2: the propagated write is most recent than our state.
            if (ourTs == theirTs - 1) {
                // execute the write operation
                Request clientRequest = writeAfterPacket.getClientWriteOnlyRequest();
                boolean isExecSuccess = app.execute(clientRequest, true);
                assert isExecSuccess : "Failed to execute request";

                // adjust the latest timestamp
                serviceInstance.currTimestamp().updateNodeTimestamp(rawSenderID, theirTs);
                return;
            }

            // Case-3: missing some updates between the received update and the executed updates.
            //  Thus, we need to send sync packet.
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
                GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                        new GenericMessagingTask<>(
                                nodeIdDeserializer.valueOf(rawSenderID),
                                syncRequestPacket);

                // cache the last requested seqNum to prevent storming the same peer with same requests
                serviceInstance.peerLastSyncRequestSeqNum().put(senderNodeId, peerFromSeqNum);

                try {
                    logger.log(Level.FINER, "Sending ClientCentricSyncRequestPacket: "
                            + syncRequestPacket.getServiceName());
                    messenger.send(m);
                } catch (IOException | JSONException e) {
                    throw new RuntimeException(e);
                }
            }

            return;
        }

        // Handle SyncReqPacket
        if (packet.getRequestType() == ClientCentricPacketType.CLIENT_CENTRIC_SYNC_REQ_PACKET) {
            assert packet instanceof ClientCentricSyncRequestPacket;
            ClientCentricSyncRequestPacket syncPacket = (ClientCentricSyncRequestPacket) packet;

            // validate the sender
            String rawSenderId = syncPacket.getSenderId();
            NodeIDType senderId = nodeIdDeserializer.valueOf(rawSenderId);
            assert serviceInstance.nodeIDs().contains(senderId) : "Invalid sender";

            // check the recency
            long theirReqSeq = syncPacket.getStartingSequenceNumber();
            long ourTs = serviceInstance.currTimestamp().getNodeTimestamp(myNodeID.toString());

            // the peer requesting non-existent requests
            if (theirReqSeq > ourTs) {
                // no-op
                return;
            }

            // Gather the requested write operations.
            // Note that index=0 at executedRequests stores seqNum=1 and timestamp starts at 1.
            long size = serviceInstance.executedRequests().size();
            List<byte[]> reqs = new ArrayList<>();
            synchronized (serviceInstance.executedRequests()) {
                // synchronized is needed, otherwise the messenger could throw
                // ConcurrentModificationException when sending the response packet.
                List<byte[]> subList = serviceInstance.executedRequests().subList(
                        (int) theirReqSeq - 1, (int) size);
                for (byte[] curr : subList) {
                    byte[] copy = Arrays.copyOf(curr, curr.length);
                    reqs.add(copy);
                }
            }

            // prepare the response packet
            ClientCentricSyncResponsePacket responsePacket =
                    new ClientCentricSyncResponsePacket(
                            /*senderId=*/messenger.getMyID().toString(),
                            /*serviceName=*/serviceName,
                            /*fromSequenceNumber=*/theirReqSeq,
                            /*encodedRequests=*/reqs);

            // send the response packet
            GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                    new GenericMessagingTask<>(senderId, responsePacket);
            try {
                logger.log(Level.FINER, "Sending ClientCentricSyncResponsePacket: "
                        + responsePacket.getServiceName());
                messenger.send(m);
            } catch (IOException | JSONException e) {
                throw new RuntimeException(e);
            }

            return;
        }

        // Handle SyncResPacket
        if (packet.getRequestType() == ClientCentricPacketType.CLIENT_CENTRIC_SYNC_RES_PACKET) {
            assert packet instanceof ClientCentricSyncResponsePacket;
            ClientCentricSyncResponsePacket syncResponse = (ClientCentricSyncResponsePacket) packet;

            // validate the sender
            String rawSenderId = syncResponse.getSenderId();
            NodeIDType senderId = nodeIdDeserializer.valueOf(rawSenderId);
            assert serviceInstance.nodeIDs().contains(senderId) : "Invalid sender";

            // check the recency
            long respStartingSeqNum = syncResponse.getStartingSequenceNumber(); // inclusive
            long ourLatestSeqNum = serviceInstance.currTimestamp().getNodeTimestamp(rawSenderId);

            logger.log(Level.INFO,
                    String.format("%s:%s - handling SyncResponsePacket from %s, theirSeqNum=%d ourSeqNum=%d",
                            messenger.getMyID(), ReadYourWritesHandler.class.getSimpleName(),
                            senderId, respStartingSeqNum, ourLatestSeqNum));

            // Case-1: there are unknown ops between our latest sequence number and the given ops.
            //  For example, our sequence number is 0, but the given ops starts from seq number 5,
            //  making us missing seq num [1,4]. We do nothing for this case.
            if (respStartingSeqNum > ourLatestSeqNum + 1) {
                logger.log(Level.WARNING,
                        String.format("%s:%s - detecting missing operations from %s, theirSeqNum=%d ourSeqNum=%d",
                                messenger.getMyID(), ReadYourWritesHandler.class.getSimpleName(),
                                senderId, respStartingSeqNum, ourLatestSeqNum));
                return;
            }

            // Case-2: we can execute the ops from our peers, we update our timestamp
            if (respStartingSeqNum <= ourLatestSeqNum + 1 ) {
                List<byte[]> writeOps = syncResponse.getEncodedRequests();
                long lastSeqNum = respStartingSeqNum + writeOps.size() - 1; // inclusive
                for (long currSeqNum = respStartingSeqNum; currSeqNum <= lastSeqNum; ++currSeqNum) {
                    // skip the already executed write operations
                    if (currSeqNum <= ourLatestSeqNum) {
                        continue;
                    }

                    // decode the write operation, then execute it
                    // TODO: validate the behaviors of the operations given by our peer
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

                // update our timestamp
                serviceInstance.currTimestamp().updateNodeTimestamp(rawSenderId, lastSeqNum);

                // process the buffered read requests, if any
                processBufferedReadRequests(serviceInstance, app);

                return;
            }

            throw new RuntimeException("This should not happen");
        }

        throw new IllegalStateException("Unexpected ClientCentricPacket: " +
                packet.getRequestType());
    }


    private static <NodeIDType> void processBufferedReadRequests(
            BayouReplicaCoordinator.ReplicaInstance<NodeIDType> serviceInstance,
            Replicable app) {
        if (serviceInstance.pendingRequests().isEmpty()) return;

        VectorTimestamp serviceLastTimestamp = serviceInstance.currTimestamp();
        Map<Long, RequestAndCallback> pendingReadRequests =
                serviceInstance.pendingRequests();

        // Iterate through all the pending requests and execute them if our current
        // timestamp is already "recent" enough.
        Iterator<Map.Entry<Long, RequestAndCallback>> it =
                pendingReadRequests.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, RequestAndCallback> e = it.next();
            RequestAndCallback clientReadRequestAndMetadata = e.getValue();
            VectorTimestamp lastWriteTimestamp = clientReadRequestAndMetadata.timestamp();

            // validate the request and replica timestamp
            if (!serviceLastTimestamp.isComparableWith(lastWriteTimestamp)) {
                logger.log(Level.WARNING,
                        "Incomparable client and replica timestamp: " +
                                lastWriteTimestamp + " vs. " + serviceLastTimestamp);
                continue;
            }

            // still buffer the request if our replica is not "recent" enough
            if (serviceLastTimestamp.isLessThan(lastWriteTimestamp)) {
                continue;
            }

            // execute the read request since we are already "recent" enough
            Request clientReadRequest = clientReadRequestAndMetadata.request();
            ExecutedCallback callback = clientReadRequestAndMetadata.callback();
            assert (clientReadRequest instanceof BehavioralRequest br) && (br.isReadOnlyRequest())
                    : "Unexpected non ReadOnlyRequest is buffered in ReadYourWritesHandler";
            boolean isExecSuccess = app.execute(clientReadRequest, false);
            if (!isExecSuccess) {
                logger.log(Level.WARNING,
                        "Failed to execute read request: " + clientReadRequest);
                continue;
            }

            // send response back to client
            assert clientReadRequest instanceof TimestampedResponse;
            ((TimestampedResponse) clientReadRequest).setLastTimestamp(
                    "R", serviceLastTimestamp);
            callback.executed(clientReadRequest, true);

            // remove this executed read request from the buffered request
            it.remove();
        }
    }

}
