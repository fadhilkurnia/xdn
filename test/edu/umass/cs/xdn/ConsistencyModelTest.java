package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class ConsistencyModelTest {

  @Test
  public void testEventual_closestReadClosestWrite() {
    assertEquals("Closest", ConsistencyModel.EVENTUAL.readType);
    assertEquals("Closest", ConsistencyModel.EVENTUAL.writeType);
  }

  @Test
  public void testSequential_closestReadMajorityWrite() {
    assertEquals("Closest", ConsistencyModel.SEQUENTIAL.readType);
    assertEquals("Majority", ConsistencyModel.SEQUENTIAL.writeType);
  }

  @Test
  public void testPrimaryBackup_sourceReadSourceWrite() {
    assertEquals("Source", ConsistencyModel.PRIMARY_BACKUP.readType);
    assertEquals("Source", ConsistencyModel.PRIMARY_BACKUP.writeType);
  }

  @Test
  public void testLinearizable_majorityReadMajorityWrite() {
    assertEquals("Majority", ConsistencyModel.LINEARIZABLE.readType);
    assertEquals("Majority", ConsistencyModel.LINEARIZABLE.writeType);
    assertEquals("Majority", ConsistencyModel.LINEARIZABILITY.readType);
    assertEquals("Majority", ConsistencyModel.LINEARIZABILITY.writeType);
  }

  @Test
  public void testAllModelsHaveNonNullTypes() {
    for (ConsistencyModel model : ConsistencyModel.values()) {
      assertNotNull(model.readType, "readType should not be null for " + model);
      assertNotNull(model.writeType, "writeType should not be null for " + model);
    }
  }

  @Test
  public void testAllModelsHaveValidAccessTypes() {
    // GreedyHeuristic only accepts these three strings — verify no typos in the enum.
    java.util.Set<String> valid = java.util.Set.of("Closest", "Source", "Majority");
    for (ConsistencyModel model : ConsistencyModel.values()) {
      assertTrue(valid.contains(model.readType), "Invalid readType for " + model);
      assertTrue(valid.contains(model.writeType), "Invalid writeType for " + model);
    }
  }
}
