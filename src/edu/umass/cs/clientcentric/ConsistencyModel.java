package edu.umass.cs.clientcentric;

public enum ConsistencyModel {
    MONOTONIC_READS,
    MONOTONIC_WRITES,
    READ_YOUR_WRITES,
    WRITES_FOLLOW_READS,
}
