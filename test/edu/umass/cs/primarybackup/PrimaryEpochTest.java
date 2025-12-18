package edu.umass.cs.primarybackup;

import org.junit.Test;

public class PrimaryEpochTest {

  @Test
  public void TestEquals() {
    PrimaryEpoch<String> e1 = new PrimaryEpoch<>("node1:0");
    PrimaryEpoch<String> e2 = new PrimaryEpoch<>("node1:0");
    assert e1.equals(e2);
    assert e1.compareTo(e2) == 0;
  }

  @Test
  public void TestCompareLessThan() {
    PrimaryEpoch<String> e1 = new PrimaryEpoch<>("node1:0");
    PrimaryEpoch<String> e2 = new PrimaryEpoch<>("node1:1");
    assert e1.compareTo(e2) < 0;
  }

  @Test
  public void TestCompareGreaterThan() {
    PrimaryEpoch<String> e1 = new PrimaryEpoch<>("node2:0");
    PrimaryEpoch<String> e2 = new PrimaryEpoch<>("node1:1");
    assert e1.compareTo(e2) < 0;
  }
}
