package edu.umass.cs.clientcentric;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;

public record RequestAndCallback(Request request,
                                 VectorTimestamp timestamp,
                                 ExecutedCallback callback) {
}
