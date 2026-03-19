package edu.umass.cs.causal;

import edu.umass.cs.causal.packets.CausalPacket;
import edu.umass.cs.causal.packets.CausalPacketType;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CausalPacketDemultiplexer extends AbstractPacketDemultiplexer<CausalPacket> {

    private final CausalReplicaCoordinator<?> coordinator;
    private final AppRequestParser appRequestParser;

    // Forwarded writes call app.execute() -> httpForwarderClient.executeAsync().join(),
    // which blocks for a full container round-trip (~ms). If handleMessage() runs that
    // work inline on the NIO selector thread, a single slow container response stalls
    // ALL subsequent incoming causal packets on this node -- including acks, dependency
    // checks, and forwarded writes from other clients.
    //
    // Virtual threads are the right tool here: they park cheaply on the .join() without
    // consuming a platform thread, so the NIO selector is never blocked regardless of
    // how many forwarded writes are in-flight simultaneously. The natural backpressure
    // is the container connection pool (maxConnections=512 in XdnGigapaxosApp).
    private final Executor workerExecutor = Executors.newVirtualThreadPerTaskExecutor();

    private static final Logger logger =
            Logger.getLogger(CausalPacketDemultiplexer.class.getName());

    public CausalPacketDemultiplexer(CausalReplicaCoordinator<?> coordinator,
                                     AppRequestParser appRequestParser) {
        this.coordinator = coordinator;
        this.appRequestParser = appRequestParser;
        this.register(CausalPacketType.values());
    }

    @Override
    protected Integer getPacketType(CausalPacket message) {
        return message.getRequestType().getInt();
    }

    @Override
    protected CausalPacket processHeader(byte[] message, NIOHeader header) {
        CausalPacketType packetType = CausalPacket.getQuickPacketTypeFromEncodedPacket(message);
        if (packetType == null) {
            return null;
        }

        return CausalPacket.createFromBytes(message, this.appRequestParser);
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof CausalPacket;
    }

    @Override
    public boolean handleMessage(CausalPacket message, NIOHeader header) {
        if (message == null) return false;

        // Dispatch off the NIO selector thread so the selector is never blocked
        // on container I/O. coordinateRequest() may call app.execute() -> .join()
        // for forwarded write packets, which would otherwise stall all subsequent
        // packet processing on this node for the duration of the container call.
        workerExecutor.execute(() -> {
            try {
                coordinator.coordinateRequest(message, null);
            } catch (IOException | RequestParseException e) {
                logger.log(Level.WARNING,
                        "CausalPacketDemultiplexer - error handling message: " + e.getMessage(), e);
            }
        });

        // Return true immediately -- the NIO selector is free to process the next packet.
        return true;
    }
}