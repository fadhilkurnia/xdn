package edu.umass.cs.xdn;

/**
 * Consistency models supported by XDN, each mapped to the read/write access types
 * consumed by the placement algorithms ("Closest", "Source", or "Majority").
 *
 * <p>Register a model for a service via:
 * <pre>
 *   XdnGeoDemandProfiler.registerConsistencyModel("my-service", ConsistencyModel.LINEARIZABILITY);
 * </pre>
 * Unregistered services default to {@link #EVENTUAL}.
 */
public enum ConsistencyModel {
    LINEARIZABILITY("Majority", "Majority"),
    SEQUENTIAL("Closest", "Majority"),
    EVENTUAL("Closest", "Closest"),
    PRIMARY_BACKUP("Source", "Source");

    /** Access type for reads: "Closest", "Source", or "Majority". */
    public final String readType;

    /** Access type for writes: "Closest", "Source", or "Majority". */
    public final String writeType;

    ConsistencyModel(String readType, String writeType) {
        this.readType  = readType;
        this.writeType = writeType;
    }
}