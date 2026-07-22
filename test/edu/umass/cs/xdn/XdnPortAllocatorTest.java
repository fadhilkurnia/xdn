package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.xdn.utils.PortAllocator;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Docker-free unit test for {@link PortAllocator}. */
public class XdnPortAllocatorTest {

  @Test
  public void testAllocatesAboveEphemeralRange() {
    for (int i = 0; i < 50; i++) {
      int port = PortAllocator.allocate();
      assertTrue(
          port >= 61000 && port < 65000,
          "port " + port + " outside [61000, 65000): must stay above Linux's ephemeral window");
    }
  }

  @Test
  public void testAllocatedPortIsImmediatelyBindable() throws Exception {
    int port = PortAllocator.allocate();
    try (ServerSocket s = new ServerSocket()) {
      s.setReuseAddress(false);
      s.bind(new InetSocketAddress(port));
    }
  }

  @Test
  public void testNoRepeatsWithinRecentWindow() {
    Set<Integer> seen = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      int port = PortAllocator.allocate();
      assertTrue(seen.add(port), "port " + port + " granted twice within the recent window");
    }
  }
}
