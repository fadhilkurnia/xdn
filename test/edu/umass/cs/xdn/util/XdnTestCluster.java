package edu.umass.cs.xdn.util;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.http.HttpActiveReplica;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.utils.Shell;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.json.JSONException;
import org.json.JSONObject;

/** Utility helper that provisions a local XDN cluster for integration-style tests. */
public class XdnTestCluster implements AutoCloseable {

  private static final String RECONFIGURATOR_ID = "RC0";
  private static final List<String> ACTIVE_REPLICA_IDS = List.of("AR0", "AR1", "AR2");
  private static final String LOOPBACK = "127.0.0.1";
  private static final int RECONFIGURATOR_BASE_PORT = 3000;
  private static final int ACTIVE_REPLICA_BASE_PORT = 2000;
  private static final int HTTP_PORT_OFFSET = 300;

  private static final Path GP_DATA_DIR = Paths.get("/tmp/gigapaxos");
  private static final Path XDN_WORK_DIR = Paths.get("/tmp/xdn");

  public static final Duration PORT_WAIT_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration SERVICE_READY_TIMEOUT = Duration.ofSeconds(90);
  public static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

  private final HttpClient httpClient =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private final Map<String, InetSocketAddress> activeReplicas = new LinkedHashMap<>();
  private final Map<String, InetSocketAddress> reconfigurators =
      Map.of(RECONFIGURATOR_ID, new InetSocketAddress(LOOPBACK, RECONFIGURATOR_BASE_PORT));
  private final List<ReconfigurableNode<String>> nodes = new ArrayList<>();
  private final List<String> createdServices = new ArrayList<>();
  private static volatile boolean loggingConfigured = false;

  public XdnTestCluster() {
    for (int i = 0; i < ACTIVE_REPLICA_IDS.size(); i++) {
      activeReplicas.put(
          ACTIVE_REPLICA_IDS.get(i), new InetSocketAddress(LOOPBACK, ACTIVE_REPLICA_BASE_PORT + i));
    }
  }

  /** Boots the Reconfigurator and ActiveReplica nodes required for an XDN cluster. */
  public void start() throws Exception {
    // TODO: enable logging for better debugging in Github action runners.
    //   configureLogging();
    configureGigapaxos();
    cleanDirectory(GP_DATA_DIR);
    cleanDirectory(XDN_WORK_DIR);

    nodes.add(startNode(RECONFIGURATOR_ID));
    for (String activeId : ACTIVE_REPLICA_IDS) {
      nodes.add(startNode(activeId));
    }

    waitForPort(LOOPBACK, getReconfiguratorHttpPort(), PORT_WAIT_TIMEOUT);
    waitForPort(LOOPBACK, getActiveHttpPort(ACTIVE_REPLICA_IDS.getFirst()), PORT_WAIT_TIMEOUT);
  }

  /** Launches a service using default SEQUENTIAL consistency and deterministic=true. */
  public void launchService(String serviceName, String imageName, String stateDirectory)
      throws IOException, InterruptedException, JSONException {
    launchService(serviceName, imageName, stateDirectory, "SEQUENTIAL", true);
  }

  /** Launches a service by issuing a request to the HTTP reconfigurator. */
  public void launchService(
      String serviceName,
      String imageName,
      String stateDirectory,
      String consistency,
      boolean deterministic)
      throws IOException, InterruptedException, JSONException {

    JSONObject serviceJson = new JSONObject();
    serviceJson.put("name", serviceName);
    serviceJson.put("image", imageName);
    serviceJson.put("port", 80);
    serviceJson.put("state", stateDirectory.endsWith("/") ? stateDirectory : stateDirectory + "/");
    if (consistency != null && !consistency.isEmpty()) {
      serviceJson.put("consistency", consistency);
    }
    serviceJson.put("deterministic", deterministic);

    String initialState = "xdn:init:" + serviceJson;
    String encodedInitialState = URLEncoder.encode(initialState, StandardCharsets.UTF_8);
    String endpoint =
        "http://%s:%d/?type=CREATE&name=%s&initial_state=%s"
            .formatted(LOOPBACK, getReconfiguratorHttpPort(), serviceName, encodedInitialState);
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(endpoint)).timeout(REQUEST_TIMEOUT).GET().build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException(
          "Service creation failed with status " + response.statusCode());
    }
    JSONObject responseJson = new JSONObject(response.body());
    if (responseJson.optBoolean("FAILED", false)) {
      throw new IllegalStateException(
          "Service creation failed: "
              + responseJson.optString("RESPONSE_MESSAGE", "unknown error"));
    }
    createdServices.add(serviceName);
  }

  /**
   * Waits for a service to respond with a successful HTTP status by periodically issuing requests.
   */
  public HttpResponse<String> awaitServiceReady(String serviceName, Duration timeout)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Exception lastError = null;

    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<String> response = invokeService(serviceName);
        if (response.statusCode() >= 200 && response.statusCode() < 500) {
          return response;
        }
        lastError = new IllegalStateException("Unexpected HTTP status " + response.statusCode());
      } catch (IOException e) {
        lastError = e;
      }
      TimeUnit.SECONDS.sleep(1);
    }

    throw new RuntimeException(
        "Timed out waiting for service '%s'".formatted(serviceName), lastError);
  }

  /** Invokes the service through the ActiveReplica HTTP frontend. */
  public HttpResponse<String> invokeService(String serviceName)
      throws IOException, InterruptedException {
    int httpPort = getActiveHttpPort(ACTIVE_REPLICA_IDS.getFirst());
    URI uri = URI.create("http://%s:%d/".formatted(LOOPBACK, httpPort));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(uri)
            .timeout(REQUEST_TIMEOUT)
            .header("XDN", serviceName)
            .GET()
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  /** Deletes a service if it exists. */
  public void deleteService(String serviceName) throws IOException, InterruptedException {
    String endpoint =
        "http://%s:%d/?type=DELETE&name=%s"
            .formatted(LOOPBACK, getReconfiguratorHttpPort(), serviceName);
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(endpoint)).timeout(REQUEST_TIMEOUT).GET().build();
    httpClient.send(request, HttpResponse.BodyHandlers.discarding());
  }

  /**
   * Issues a HTTP GET request to the provided endpoint and waits synchronously for the response.
   */
  public HttpResponse<String> sendGetRequest(
      String serviceName, int replicaIdx, String endpoint, Duration timeout)
      throws IOException, InterruptedException {
    assert replicaIdx >= 0 && replicaIdx < ACTIVE_REPLICA_IDS.size()
        : String.format(
            "Invalid replicaIdx=%d, which is not between 0 and %d",
            replicaIdx, ACTIVE_REPLICA_IDS.size() - 1);
    Duration effectiveTimeout = timeout != null ? timeout : REQUEST_TIMEOUT;
    int httpPort = getActiveHttpPort(ACTIVE_REPLICA_IDS.get(replicaIdx));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://%s:%d%s".formatted(LOOPBACK, httpPort, endpoint)))
            .timeout(effectiveTimeout)
            .header("XDN", serviceName)
            .GET()
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  /** Issues a HTTP GET request with the default timeout. */
  public HttpResponse<String> sendGetRequest(String serviceName, String endpoint)
      throws IOException, InterruptedException {
    return sendGetRequest(serviceName, 0, endpoint, REQUEST_TIMEOUT);
  }

  /** Issues a HTTP GET request with the default timeout to specific replica index */
  public HttpResponse<String> sendGetRequest(String serviceName, int replicaIdx, String endpoint)
      throws IOException, InterruptedException {
    return sendGetRequest(serviceName, replicaIdx, endpoint, REQUEST_TIMEOUT);
  }

  /**
   * Gracefully shuts down the cluster, deleting all created services and cleaning temp directories.
   */
  @Override
  public void close() {
    for (String service : new ArrayList<>(createdServices)) {
      safeDeleteService(service);
    }
    createdServices.clear();

    // wait for services to be deleted
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    for (ReconfigurableNode<String> node : nodes) {
      try {
        node.close();
      } catch (Exception ignored) {
        // best effort
      }
    }
    nodes.clear();

    try {
      TimeUnit.SECONDS.sleep(2);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    cleanDirectory(GP_DATA_DIR);
    cleanDirectory(XDN_WORK_DIR);
  }

  private static void configureLogging() {
    if (loggingConfigured) {
      return;
    }
    synchronized (XdnTestCluster.class) {
      if (loggingConfigured) {
        return;
      }
      Path loggingDir = Paths.get("output").toAbsolutePath();
      try {
        Files.createDirectories(loggingDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Path loggingConfig = Paths.get("conf", "logging.properties").toAbsolutePath();
      if (Files.exists(loggingConfig)) {
        System.setProperty("java.util.logging.config.file", loggingConfig.toString());
        try (InputStream in = Files.newInputStream(loggingConfig)) {
          LogManager.getLogManager().readConfiguration(in);
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to load logging configuration from " + loggingConfig, e);
        }
      }

      Logger rootLogger = Logger.getLogger("");
      if (rootLogger.getLevel() == null
          || rootLogger.getLevel().intValue() > Level.FINE.intValue()) {
        rootLogger.setLevel(Level.FINE);
      }
      for (var handler : rootLogger.getHandlers()) {
        handler.setLevel(Level.FINE);
      }

      Logger.getLogger(HttpActiveReplica.class.getName()).setLevel(Level.FINE);
      loggingConfigured = true;
    }
  }

  public static boolean isDockerAvailable() {
    try {
      return Shell.runCommand("docker version", true) == 0;
    } catch (RuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  public int getReconfiguratorHttpPort() {
    return RECONFIGURATOR_BASE_PORT + HTTP_PORT_OFFSET;
  }

  public int getActiveHttpPort(String activeId) {
    int replicaIndex = ACTIVE_REPLICA_IDS.indexOf(activeId);
    if (replicaIndex < 0) {
      throw new IllegalArgumentException("Unknown active replica id: " + activeId);
    }
    return ACTIVE_REPLICA_BASE_PORT + replicaIndex + HTTP_PORT_OFFSET;
  }

  private static void configureGigapaxos() {
    Path configPath = Paths.get("conf", "gigapaxos.local.properties").toAbsolutePath();
    System.setProperty(PaxosConfig.GIGAPAXOS_CONFIG_FILE_KEY, configPath.toString());
    Config.register(
        new String[] {
          "ENABLE_ACTIVE_REPLICA_HTTP=true",
          "ENABLE_RECONFIGURATOR_HTTP=true",
          "REPLICA_COORDINATOR_CLASS=edu.umass.cs.xdn.XdnReplicaCoordinator",
          "INITIAL_STATE_VALIDATOR_CLASS=edu.umass.cs.xdn.XdnServiceInitialStateValidator",
          "XDN_PB_STATEDIFF_RECORDER_TYPE=RSYNC"
        });
  }

  private ReconfigurableNode<String> startNode(String nodeId) throws IOException {
    ReconfigurableNodeConfig<String> nodeConfig =
        new DefaultNodeConfig<>(activeReplicas, reconfigurators);
    return new ReconfigurableNode.DefaultReconfigurableNode(
        nodeId, nodeConfig, new String[] {nodeId}, false);
  }

  private static void waitForPort(String host, int port, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(host, port), 500);
        return;
      } catch (IOException ignored) {
        TimeUnit.MILLISECONDS.sleep(500);
      }
    }
    throw new RuntimeException("Timed out waiting for port %s:%d".formatted(host, port));
  }

  private static void cleanDirectory(Path directory) {
    if (Files.notExists(directory)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException ignored) {
                  // best effort cleanup
                }
              });
    } catch (IOException ignored) {
      // best effort cleanup
    }
  }

  private void safeDeleteService(String serviceName) {
    try {
      deleteService(serviceName);
    } catch (Exception ignored) {
      // best effort cleanup
    }
  }
}
