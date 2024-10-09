package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.consistency.Quorum.QuorumRequestPacket;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.*;

public class MRClient extends ReconfigurableAppClientAsync<CCRequestPacket> {
    private String[] types = new String[]{"C"};
    private String[] items = new String[]{"CIRCLE01", "TRIANGLE01", "CIRCLE02", "TRIANGLE02", "CIRCLE03"};
    private String[] coords = new String[]{"1,1", "3,2", "4,4", "9,4", "8,5", "3,7", "6,3", "8,0"};
    private int[] ports = new int[]{2000,2001,2002};
    private final Long clientID;
    private HashMap<Integer, ArrayList<CCManager.Write>> requestWrites = new HashMap<>();
    private HashMap<Integer, Timestamp> requestVectorClock = new HashMap<Integer, Timestamp>();
    public MRClient() throws IOException {
        super();
        this.clientID = (long) (Math.random() * Integer.MAX_VALUE);
    }
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
//        System.out.println("In getRequest of client");
        try {
//            System.out.println(stringified);
            return new CCRequestPacket(new JSONObject(stringified));
        }
        catch (Exception e){
            System.out.println("Exception: "+e);
        }
        return null;
    }
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(CCRequestPacket.CCPacketType.values()));
    }
    public static CCRequestPacket makeWriteRequest(MRClient mrc){
        int type = (int)(Math.random() * (mrc.types.length));
        int item = (int)(Math.random() * (mrc.items.length));
        int coord = (int)(Math.random() * (mrc.coords.length));
        String command = mrc.types[type] + " " + mrc.items[item] + " " + mrc.coords[coord];
        return new CCRequestPacket(mrc.clientID, (long)(Math.random()*Integer.MAX_VALUE), CCRequestPacket.CCPacketType.MR_WRITE, CCManager.getDefaultServiceName(),
                command, mrc.requestVectorClock, mrc.requestWrites);
    }
    public static CCRequestPacket makeReadRequest(MRClient mrc){
        return new CCRequestPacket(mrc.clientID, (long)(Math.random()*Integer.MAX_VALUE), CCRequestPacket.CCPacketType.MR_READ, CCManager.getDefaultServiceName(),
                "read_request", mrc.requestVectorClock, mrc.requestWrites);
    }
    public static void updateWrites(CCRequestPacket response, MRClient mrc){
//        System.out.println("Response: "+response);
        mrc.requestVectorClock = response.getResponseVectorClock();
        if (response.getResponseWrites().containsKey(response.getSource())){
            for (CCManager.Write write : response.getResponseWrites().get(response.getSource())) {
                if (!mrc.requestWrites.containsKey(response.getSource())) {
                    mrc.requestWrites.put(response.getSource(), new ArrayList<>());
                }
                mrc.requestWrites.get(response.getSource()).add(write);
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException{
        MRClient mrClient = new MRClient();
        for (int i = 0; i < 10; i++) {
            CCRequestPacket request;
            request = i%2==0 ? makeWriteRequest(mrClient) : makeReadRequest(mrClient);
            long reqInitime = System.currentTimeMillis();
//            System.out.println("Sending request vc:"+request.getRequestVectorClock());
            mrClient.sendRequest(request ,
                    new InetSocketAddress("localhost", mrClient.ports[(int)(Math.random() * (mrClient.ports.length))]),
                    new Callback<Request, CCRequestPacket>() {

                        long createTime = System.currentTimeMillis();
                        @Override
                        public CCRequestPacket processResponse(Request response) {
                            assert(response instanceof QuorumRequestPacket) :
                                    response.getSummary();
                            System.out
                                    .println("Response for request ["
                                            + request.getSummary()
                                            + " "
                                            + request.getRequestValue()
                                            + "] = "
                                            + ((CCRequestPacket)response).getResponseValue()
                                            + " received in "
                                            + (System.currentTimeMillis() - createTime)
                                            + "ms");
//                            System.out.println("Response:"+(MRRequestPacket)response);
                            updateWrites(((CCRequestPacket)response), mrClient);
                            return (CCRequestPacket) response;
                        }
                    });
            Thread.sleep(1000);
        }

    }
}
