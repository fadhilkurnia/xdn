package edu.umass.cs.consistency.Quorum;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.NoopAppClient;
import edu.umass.cs.reconfiguration.examples.linwrites.SimpleAppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import io.netty.handler.codec.spdy.SpdyHttpResponseStreamIdHandler;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class QuorumClient extends ReconfigurableAppClientAsync<QuorumRequestPacket> {
    private final String[] items = new String[]{"table", "chair", "pen"};
    private final int[] ports = new int[]{2000,2001,2002};
    public QuorumClient() throws IOException {
        super();
    }

    @Override
    public QuorumRequestPacket getRequest(String stringified) throws RequestParseException {
        System.out.println("In getRequest of client");
        try {
            return new QuorumRequestPacket(new JSONObject(stringified));
        }
        catch (Exception e){
            System.out.println("Exception: "+e);
        }
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(QuorumRequestPacket.QuorumPacketType.values()));
    }

    public static void main(String[] args) throws IOException, InterruptedException{
        QuorumClient quorumClient = new QuorumClient();
        QuorumRequestPacket request;
//        request = new QuorumRequestPacket((long)(Math.random()*Integer.MAX_VALUE),
//                "", QuorumRequestPacket.QuorumPacketType.READ, QuorumManager.getDefaultServiceName());
        request = new QuorumRequestPacket((long)(Math.random()*Integer.MAX_VALUE),
                "60", QuorumRequestPacket.QuorumPacketType.WRITE, QuorumManager.getDefaultServiceName());
        long reqInitime = System.currentTimeMillis();
        System.out.println(request);
        quorumClient.sendRequest(request ,
                new InetSocketAddress("localhost", 2000),
                new Callback<Request, QuorumRequestPacket>() {

                    long createTime = System.currentTimeMillis();
                    @Override
                    public QuorumRequestPacket processResponse(Request response) {
                        assert(response instanceof QuorumRequestPacket) :
                                response.getSummary();
                        System.out
                                .println("Response for request ["
                                        + request.getSummary()
                                        + "] = "
                                        + ((QuorumRequestPacket)response).getResponseValue()
                                        + " received in "
                                        + (System.currentTimeMillis() - createTime)
                                        + "ms");
                        return (QuorumRequestPacket) response;
                    }
        });
    }
}
