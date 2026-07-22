package edu.umass.cs.xdn.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Allocates host ports for containers' published entry ports.
 *
 * <p>A port is probed by binding it on the wildcard address, exactly what dockerd will do when it
 * publishes the port. A connect-probe is not sufficient: a port held as the source of an outbound
 * socket has no listener (so a connect fails, suggesting "free") yet still blocks dockerd's later
 * bind with EADDRINUSE.
 *
 * <p>The range sits above Linux's default ephemeral window (32768-60999) so kernel-assigned client
 * ports never race an allocation on CI or production hosts. macOS's ephemeral range extends to
 * 65535, leaving a small overlap on dev machines that the bind-probe covers in practice.
 *
 * <p>Recently granted ports are remembered and skipped: in tests, multiple ActiveReplicas share one
 * JVM and allocate concurrently, and a probe-then-close on one replica leaves a window where
 * another replica could probe the same port successfully before either docker bind happens. The
 * memory is a bounded ring, so no deallocation plumbing is needed.
 */
public final class PortAllocator {

  private static final int RANGE_START = 61000;
  private static final int RANGE_END = 65000; // exclusive
  private static final int MAX_ATTEMPTS = 20;
  private static final int RECENT_CAPACITY = 128;

  private static final Set<Integer> recentPorts = new HashSet<>();
  private static final Deque<Integer> recentOrder = new ArrayDeque<>();

  private PortAllocator() {}

  /**
   * Returns a host port in [{@value RANGE_START}, {@value RANGE_END}) that was bindable at probe
   * time and was not granted recently. Falls back to an unprobed random port if every attempt
   * fails, letting docker surface the failure loudly.
   */
  public static synchronized int allocate() {
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      int port = ThreadLocalRandom.current().nextInt(RANGE_START, RANGE_END);
      if (recentPorts.contains(port)) {
        continue;
      }
      try (ServerSocket probe = new ServerSocket()) {
        probe.setReuseAddress(false);
        probe.bind(new InetSocketAddress(port));
        remember(port);
        return port;
      } catch (IOException e) {
        // in use; try another
      }
    }
    int port = ThreadLocalRandom.current().nextInt(RANGE_START, RANGE_END);
    remember(port);
    return port;
  }

  private static void remember(int port) {
    if (recentPorts.add(port)) {
      recentOrder.addLast(port);
      if (recentOrder.size() > RECENT_CAPACITY) {
        recentPorts.remove(recentOrder.removeFirst());
      }
    }
  }
}
