package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.consistency.EventualConsistency.Domain.DAG;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Reconcilable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamoAppLC implements Reconcilable {

    private final String checkpointFilename;

    private final String restoreFilename;
    private ArrayList<String> cart = new ArrayList<>();
    private Logger log = Logger.getLogger(DynamoApp.class.getName());


    public DynamoAppLC(String[] args) {
        super();
        Properties properties = PaxosConfig.getAsProperties();
        this.checkpointFilename = properties.getProperty("CURRENT_STATE_DIR") + "/checkpoint_" + args[0] + ".txt";
        this.restoreFilename = properties.getProperty("CURRENT_STATE_DIR") + "/restore_" + args[0] + ".txt";
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return this.execute(request);
    }

    private Set<IntegerPacketType> getGETRequestTypes() {
        ArrayList<DynamoRequestPacket.DynamoPacketType> GETTypes = new ArrayList<>();
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET_FWD);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.GET_ACK);
        return new HashSet<IntegerPacketType>(GETTypes);
    }
    private Set<IntegerPacketType> getPUTRequestTypes() {
        ArrayList<DynamoRequestPacket.DynamoPacketType> GETTypes = new ArrayList<>();
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT_FWD);
        GETTypes.add(DynamoRequestPacket.DynamoPacketType.PUT_ACK);
        return new HashSet<IntegerPacketType>(GETTypes);
    }
    @Override
    public boolean execute(Request request) {
        if (request instanceof DynamoRequestPacket) {
            if (getGETRequestTypes().contains(((DynamoRequestPacket) request).getType())) {
                log.log(Level.INFO, "GET request for index: "+((DynamoRequestPacket) request).getRequestValue());
                if(!((DynamoRequestPacket) request).getRequestValue().isEmpty()) {
                    try {
                        JSONObject jsonObject = new JSONObject(((DynamoRequestPacket) request).getRequestValue());
                        ((DynamoRequestPacket) request).getResponsePacket().setValue(String.valueOf(Collections.frequency(this.cart, jsonObject.getString("key"))));
                    } catch (Exception e) {
                        ((DynamoRequestPacket) request).getResponsePacket().setValue("0");
                    }
                }
                else {
                    ((DynamoRequestPacket) request).getResponsePacket().setValue("0");
                }
            }
            else if(getPUTRequestTypes().contains(((DynamoRequestPacket) request).getType())){
                log.log(Level.INFO, "In Dynamo App for PUT request");
                try {
                    System.out.println("-----------------App PUT"+((DynamoRequestPacket) request).getRequestValue()+"--"+((DynamoRequestPacket) request).getRequestID()+"----------------------------");
                    JSONObject jsonObject = new JSONObject(((DynamoRequestPacket) request).getRequestValue());
                    this.cart.add(jsonObject.getString("key"));
                } catch (JSONException e) {
                    log.log(Level.WARNING, "Check the request value");
                    throw new RuntimeException(e);
                }
            }
            log.log(Level.INFO, "After execution: "+this.cart.toString());
            return true;
        }
        log.log(Level.WARNING, "Unknown request type: " + request.getRequestType());
        return false;
    }

    @Override
    public String checkpoint(String name) {
        Path checkpointPath = Paths.get(this.checkpointFilename);
        try {
            if (!Files.exists(checkpointPath)) {
                Files.createFile(checkpointPath);
            }
            Files.writeString(checkpointPath, "", StandardOpenOption.WRITE);
            for(String item: cart){
                Files.writeString(checkpointPath, item + System.lineSeparator(), StandardOpenOption.APPEND);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return LargeCheckpointer.createCheckpointHandle(this.checkpointFilename);
    }

    @Override
    public boolean restore(String name, String state) {
        if (LargeCheckpointer.isCheckpointHandle(state)) {
            try {
                if (Files.notExists(Paths.get(this.restoreFilename))) {
                    Files.createFile(Paths.get(this.restoreFilename));
                }
                LargeCheckpointer.restoreCheckpointHandle(state, this.restoreFilename);
                cart = new ArrayList<>();
                File file = new File(this.restoreFilename);
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String line;
                while ((line = br.readLine()) != null) {
                    cart.add(line);
                }
                fr.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        try {
            return new DynamoRequestPacket(new JSONObject(stringified));
        } catch (JSONException je) {
            throw new RequestParseException(je);
        }
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(DynamoRequestPacket.DynamoPacketType.values()));
    }

    @Override
    public GraphNode reconcile(ArrayList<GraphNode> requests) {
        if (requests.isEmpty()){
            log.log(Level.WARNING, "Reconcile method called on an empty array of requests.");
            return null;
        }
        try {
            return DAG.getDominantVC(requests);
        } catch (Exception e) {
            log.log(Level.WARNING, "Error in reconciling the requests: " + e.toString());
        }
        return null;
    }

    @Override
    public String stateForReconcile(){
        return cart.toString();
    }
}
