package edu.umass.cs.primarybackup;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class PrimaryBackupTest {

  @Test
  @DisplayName("Getting the number of instances")
  public void NumInstancesTest() {
    PrimaryBackupTestReplicaSet replicaSet1 = PrimaryBackupTestReplicaSet.initialize(3);
    assertEquals(replicaSet1.getNumInstances(), 3);
    assertTrue(replicaSet1.getServiceName().startsWith("test-service-"));
    replicaSet1.destroy();

    PrimaryBackupTestReplicaSet replicaSet2 = PrimaryBackupTestReplicaSet.initialize(5);
    assertEquals(replicaSet2.getNumInstances(), 5);
    assertTrue(replicaSet2.getServiceName().startsWith("test-service-"));
    replicaSet2.destroy();
  }

  @Test
  @DisplayName("Ensure there is valid primary node")
  public void ValidPrimaryTest() throws InterruptedException {
    PrimaryBackupTestReplicaSet replicaSet = PrimaryBackupTestReplicaSet.initialize(3);
    Thread.sleep(5000);

    try {
      // Ensures we have valid primary node.
      assertNotNull(replicaSet.getPrimaryNode());

      // Ensures the primary is one of the node in the cluster.
      assertTrue(replicaSet.getNodeIds().contains(replicaSet.getPrimaryNode()));
    } finally {
      replicaSet.destroy();
    }
  }

  @Test
  @DisplayName("Ensure we can change the primary")
  public void PrimaryChangeTest() throws InterruptedException {
    PrimaryBackupTestReplicaSet replicaSet = PrimaryBackupTestReplicaSet.initialize(5);
    Thread.sleep(5000);

    try {
      // Changes the primary into node4.
      replicaSet.setPrimaryNode("node4");
      Thread.sleep(5000);

      // Validates the current primary.
      assertEquals("node4", replicaSet.getPrimaryNode());

      // Changes the primary into node1.
      replicaSet.setPrimaryNode("node1");
      Thread.sleep(5000);

      // Validates the current primary.
      assertEquals("node1", replicaSet.getPrimaryNode());
    } finally {
      replicaSet.destroy();
    }
  }
}
