package edu.umass.cs.clientcentric.interfaces;

import edu.umass.cs.clientcentric.VectorTimestamp;

public interface TimestampedRequest {
    /**
     * Gets the client's latest timestamp.
     *
     * @param timestampName must be either 'R' or 'W'.
     * @return the client's read or write timestamp.
     */
    VectorTimestamp getLastTimestamp(String timestampName);
}
