package edu.umass.cs.clientcentric;

import java.util.List;
import org.junit.jupiter.api.Test;

public class VectorTimestampTest {

  @Test
  public void VectorTimestampTest_ToString() {
    VectorTimestamp timestamp = new VectorTimestamp(List.of("AR0", "AR1", "AR2"));
    assert timestamp.toString().equals("VectorTimestamp/AR1:0.AR2:0.AR0:0/")
        : "Incorrect timestamp format in String: " + timestamp;
  }

  @Test
  public void VectorTimestampTest_FromString() {
    String encoded = "VectorTimestamp/AR1:313.AR2:354.AR0:413/";
    VectorTimestamp timestamp = VectorTimestamp.createFromString(encoded);
    assert timestamp.getNodeTimestamp("AR1") == 313;
    assert timestamp.getNodeTimestamp("AR2") == 354;
    assert timestamp.getNodeTimestamp("AR0") == 413;
  }

  @Test
  public void VectorTimestampTest_Equals() {
    String encoded = "VectorTimestamp/AR1:313.AR2:354.AR0:413/";
    VectorTimestamp timestamp1 = VectorTimestamp.createFromString(encoded);
    VectorTimestamp timestamp2 = VectorTimestamp.createFromString(encoded);

    assert timestamp1.equals(timestamp2);
  }
}
