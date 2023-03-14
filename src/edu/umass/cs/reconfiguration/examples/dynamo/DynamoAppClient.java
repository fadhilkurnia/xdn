package edu.umass.cs.reconfiguration.examples.dynamo;

import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DynamoAppClient extends ReconfigurableAppClientAsync<DynamoRequest> {

    public DynamoAppClient() throws IOException {
        super();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>(Arrays.asList(DynamoRequest.RequestType.values()));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final DynamoAppClient asyncClient = new DynamoAppClient();

        asyncClient.sendRequest(
                new DynamoRequest(
                        DynamoRequest.RequestType.WRITE, "hello", "world"),
                new Callback<Request, DynamoRequest>() {
                    @Override
                    public DynamoRequest processResponse(Request response) {
                        System.out.println("processing response " + response);
                        return null;
                    }
                }
        );

        Thread.sleep(10000);

        asyncClient.close();
    }

}
