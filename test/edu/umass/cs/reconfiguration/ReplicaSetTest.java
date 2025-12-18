package edu.umass.cs.reconfiguration;

import java.util.ArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ReplicaSetTest {

  private static class ReplicaSet {

    // TODO: make a helper function that initialize 3-member replicaset,
    //  and configurable extra instances.
    //  What important features need to be tested:
    //   - reconfiguration in XDN using Paxos
    //      - reconfiguration in GigaPaxos with a simple app.
    //   - reconfiguration in XDN using PrimaryBackup.
    //      - reconfiguration in PrimaryBackup using simple monotonic app.
    //   - primary-backup when paxos coordinator change under our feet.

    private ArrayList<ReconfigurableNode<String>> instances;
  }

  @Test
  @DisplayName("")
  void testProgressOnMajority() {}
}
