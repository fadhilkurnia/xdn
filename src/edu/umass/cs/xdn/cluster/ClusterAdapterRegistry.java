package edu.umass.cs.xdn.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tiny registry from adapter name (the spec's {@code "adapter"} field) to a {@link ClusterAdapter}
 * instance. Kept lazy so adapter classes don't have to construct themselves at class-load time.
 */
public final class ClusterAdapterRegistry {

  private static final Map<String, ClusterAdapter> ADAPTERS = new ConcurrentHashMap<>();

  static {
    register(new EtcdClusterAdapter());
  }

  private ClusterAdapterRegistry() {}

  public static void register(ClusterAdapter adapter) {
    ADAPTERS.put(adapter.name(), adapter);
  }

  /** Returns the adapter for {@code name}, or null if none is registered. */
  public static ClusterAdapter get(String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    return ADAPTERS.get(name);
  }
}
