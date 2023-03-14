package edu.umass.cs.reconfiguration.examples.dynamo;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

/**
 * DynamoRequest class represents requests sent by the client to the DynamoApp
 */
public class DynamoRequest implements ClientRequest {

    public enum RequestType implements IntegerPacketType {
        READ(800),
        WRITE(801);

        private final int number;

        RequestType(int i) {
            this.number = i;
        }

        @Override
        public int getInt() {
            return this.number;
        }
    }

    public class Payload {
        public String key;
        public String value;
    }

    private IntegerPacketType type;
    private Payload payload;

    private Payload result;

    public DynamoRequest(RequestType type, String key, String value) {
        this.type = type;
        this.payload = new Payload();
        this.payload.key = key;
        this.payload.value = value;
    }

    public void setResponse(String key, String value) {
        this.result = new Payload();
        this.result.key = key;
        this.result.value = value;
    }

    public String getKey() {
        return this.payload.key;
    }

    public String getValue() {
        return this.payload.value;
    }

    @Override
    public ClientRequest getResponse() {
        return this;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return this.type;
    }

    @Override
    public String getServiceName() {
        return "dynamo-default-service-name";
    }

    @Override
    public long getRequestID() {
        return (long) (Math.random() * Integer.MAX_VALUE);
    }
}
