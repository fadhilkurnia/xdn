package edu.umass.cs.xdn.service;

public enum ConsistencyModel {
    LINEARIZABILITY,
    LINEARIZABLE,
    SEQUENTIAL,
    CAUSAL,
    EVENTUAL,
    READ_YOUR_WRITES,
    WRITES_FOLLOW_READS,
    MONOTONIC_READS,
    MONOTONIC_WRITES
}
