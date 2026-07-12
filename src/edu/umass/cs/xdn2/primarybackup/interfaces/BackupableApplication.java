package edu.umass.cs.xdn2.primarybackup.interfaces;

/**
 * BackupableApplication is intended for PrimaryBackupReplicaCoordinator so that a primary can
 * capture statediff of running application and a backup application can apply statediffs, and
 * a primary can capture stat. As with the execute() method, captureStatediff(), saveStatediff(), and applySnpDiff()
 * should be done atomically. The captureStatediff() method must be called after execute(.),
 * otherwise the captureStatediff(.) must return a null String.
 */
public interface BackupableApplication {
    byte[] captureStatediff(String serviceName);
    boolean saveStatediff(String serviceName, byte[] statediff, String filename);
    boolean applySnpDiff(String serviceName, String filename);
}