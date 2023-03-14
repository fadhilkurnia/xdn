package edu.umass.cs.reconfiguration.examples.dynamo;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DynamoApp implements Replicable, Reconfigurable {

    /**
     * data is the actual key value store this DynamoApp provides
     */
    private Map<String, String> data;

    /**
     * serviceData is KV for each serviceName
     */
    private Map<String, Map<String, String>> serviceData;

    private boolean isAcceptingRequest;

    /**
     * getRequest implements AppRequestParser interface that is being used in the
     * Application interface which is used in both Replicable and Reconfigurable interfaces.
     * This method accepts a string from the wire (e.g. TCP stream) and deserialize it into
     * a Request object.
     *
     * @param stringified
     * @return
     * @throws RequestParseException
     */
    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        return (Request) JSONObject.stringToValue(stringified);
    }

    /**
     * getRequestTypes() return a set of types for requests which will be handled by this
     * DynamoApp.
     * @return
     */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> types = new HashSet<IntegerPacketType>();

        // client to application requests
        types.add(DynamoRequest.RequestType.WRITE);
        types.add(DynamoRequest.RequestType.READ);

        // packets between replicated application
        // TODO: confirm about this, these types should be declared in the ReplicaCoordinator
        // TODO: not in the Application level.
        types.add(DynamoPacket.PacketType.FORWARD_WRITE_REQUEST);
        types.add(DynamoPacket.PacketType.FORWARD_WRITE_RESPONSE);
        types.add(DynamoPacket.PacketType.FORWARD_READ_REQUEST);
        types.add(DynamoPacket.PacketType.FORWARD_READ_RESPONSE);

        return types;
    }

    @Override
    public boolean execute(Request request) {
        return execute(request, false);
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        if (!this.isAcceptingRequest) {
            return false;
        }

        assert (request instanceof DynamoRequest);
        DynamoRequest req = (DynamoRequest) request;
        IntegerPacketType reqType = req.getRequestType();

        if (reqType == DynamoRequest.RequestType.WRITE) {
            String oldValue = this.data.get(req.getKey());
            this.data.put(req.getKey(), req.getValue());

            // set the response
            req.setResponse(req.getKey(), oldValue);
            return true;
        }
        if (reqType == DynamoRequest.RequestType.READ) {
            String curValue = this.data.get(req.getKey());

            // set the response
            req.setResponse(req.getKey(), curValue);
            return true;
        }

        return true;
    }

    @Override
    public String checkpoint(String name) {
        // TODO: confirm about this, application should be oblivious with the service name.
        // TODO: Currently, we are ignoring the service name.

        System.out.println(">>>>>>> snapshot " + name);

        if (this.data.size() == 0) {
            return null;
        }

        String snapshot;
        try {
            snapshot = JSONObject.valueToString(this.data);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        return snapshot;
    }

    @Override
    public boolean restore(String name, String state) {
        // TODO: confirm about this, application should be oblivious with the service name.
        // TODO: Currently, we are ignoring the service name.

        System.out.println(">>>>>>> restore " + name + " " + state);

        if (state == null || state.equals("{}")) {
            this.data = new HashMap<>();
            return true;
        }

        Object dataObject = JSONObject.stringToValue(state);
        this.data = (Map<String, String>) dataObject;

        return true;
    }

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return DynamoPacket.getStopPacket(name, epoch);
    }

    @Override
    public String getFinalState(String name, int epoch) {
        throw new RuntimeException("This method should not have been called");
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        throw new RuntimeException("This method should not have been called");
    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        throw new RuntimeException("This method should not have been called");
    }

    @Override
    public Integer getEpoch(String name) {
        throw new RuntimeException("This method should not have been called");
    }

    /**
     * the default constructor DynamoApp() will be called via reflection.
     */
    public DynamoApp() {
        this.data = new HashMap<String, String>();
        this.isAcceptingRequest = true;
    }

}
