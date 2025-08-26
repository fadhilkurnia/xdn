package edu.umass.cs.clientcentric.interfaces;

import edu.umass.cs.clientcentric.VectorTimestamp;

public interface TimestampedResponse {
    /**
     * Sets the client's latest timestamp.
     *
     * @param timestampName must be either 'R' or 'W' for read and write, respectively.
     * @param timestamp latest read or write timestamp.
     */
    void setLastTimestamp(String timestampName, VectorTimestamp timestamp);
}
