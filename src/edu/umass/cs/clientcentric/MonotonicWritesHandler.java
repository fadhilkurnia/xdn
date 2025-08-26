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

public class MonotonicWritesHandler {

    private static final Logger logger = Logger.getGlobal();

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

        // Handle read request, directly.
        if (behavioralRequest.isReadOnlyRequest()) {
            boolean isExecSuccess = app.execute(clientRequest, false);
            if (!isExecSuccess) {
                logger.log(Level.WARNING, "Failed to execute request: " + clientRequest);
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
            // Case-1: Handle write directly if this replica is "recent" enough.
            if (requestLastWriteTimestamp.isLessThanOrEqualTo(serviceLastTimestamp)) {
                boolean isExecSuccess = app.execute(clientRequest, false);
                if (!isExecSuccess) {
                    logger.log(Level.WARNING, "Failed to execute request: " + clientRequest);
                    return false;
                }

                // Enqueue the executed request
                serviceInstance.executedRequests().add(clientRequest.toBytes());

                // Bump up our timestamp
                serviceLastTimestamp = serviceInstance.currTimestamp()
                        .increaseNodeTimestamp(myNodeID.toString());

                // Update the client's write timestamp, then send response back to client.
                ((TimestampedResponse) clientRequest)
                        .setLastTimestamp("W", serviceLastTimestamp);
                callback.executed(clientRequest, true);

                // Asynchronously send the writes to other replicas
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
                    logger.log(Level.INFO, "Sending ClientCentricWriteAfterPacket: "
                            + writeAfterPacket);
                    messenger.send(m);
                } catch (JSONException | IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // Case-2: We are not "recent" enough, thus sync is needed.
            {
                // First, we buffer the request.
                Long requestID = ((ClientRequest) clientRequest).getRequestID();
                serviceInstance.pendingRequests().put(
                        requestID,
                        new RequestAndCallback(
                                clientRequest, requestLastWriteTimestamp, callback));

                // Prepare sync packets to other replicas that are more recent.
                List<GenericMessagingTask<NodeIDType, ClientCentricPacket>> syncPackets =
                        new ArrayList<>();
                for (String nodeIdRaw : serviceLastTimestamp.getNodeIds()) {
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
                    }
                }

                // Send the sync packets
                for (GenericMessagingTask<NodeIDType, ClientCentricPacket> m : syncPackets) {
                    try {
                        logger.log(Level.INFO, "Sending ClientCentricSyncRequestPacket: "
                                + m.msgs[0]);
                        messenger.send(m);
                    } catch (IOException | JSONException e) {
                        throw new RuntimeException(e);
                    }
                }
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
        assert packet != null : "packet cannot be null";
        String serviceName = packet.getServiceName();
        assert serviceName != null : "unspecified service name";
        NodeIDType myNodeID = messenger.getMyID();

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
                ClientCentricSyncRequestPacket syncRequestPacket =
                        new ClientCentricSyncRequestPacket(
                                /*senderId=*/myNodeID.toString(),
                                /*serviceName=*/serviceInstance.name(),
                                /*fromSequenceNumber*/ourTs + 1);
                GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                        new GenericMessagingTask<>(
                                nodeIdDeserializer.valueOf(rawSenderID),
                                syncRequestPacket);

                try {
                    logger.log(Level.INFO, "Sending ClientCentricSyncRequestPacket: "
                            + syncRequestPacket);
                    messenger.send(m);
                } catch (IOException | JSONException e) {
                    throw new RuntimeException(e);
                }
            }
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
            List<byte[]> reqs = serviceInstance.executedRequests().subList(
                    (int) theirReqSeq - 1, (int) size);

            // prepare the response packet
            ClientCentricSyncResponsePacket responsePacket =
                    new ClientCentricSyncResponsePacket(
                            /*senderId=*/rawSenderId,
                            /*serviceName=*/serviceName,
                            /*fromSequenceNumber=*/theirReqSeq,
                            /*encodedRequests=*/reqs);

            // send the response packet
            GenericMessagingTask<NodeIDType, ClientCentricPacket> m =
                    new GenericMessagingTask<>(senderId, responsePacket);
            try {
                logger.log(Level.INFO, "Sending ClientCentricSyncResponsePacket: "
                        + responsePacket);
                messenger.send(m);
            } catch (IOException | JSONException e) {
                throw new RuntimeException(e);
            }
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
            long ourLatestSeqNum = serviceInstance.currTimestamp()
                    .getNodeTimestamp(rawSenderId) - 1;

            // Case-1: got a more recent write ops
            if (respStartingSeqNum > ourLatestSeqNum) {
                return;
            }

            // Case-2: respStartingSeqNum < ourLatestSeqNum
            {
                // Execute the given requests from our peer
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
            }
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
                logger.log(Level.WARNING,
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
                logger.log(Level.WARNING,
                        "Failed to execute write request: " + clientReadRequest);
                continue;
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
