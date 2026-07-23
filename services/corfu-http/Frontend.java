import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

/**
 * HTTP key-value frontend for the CorfuDB reference service (services/corfu-cluster). Runs as the
 * entry sidecar in each replica's pod; it IS the corfu client, so the client-driven chain (token
 * from the sequencer, then a write to each log unit in chain order) originates here, inside the
 * member's network namespace. Same uniform surface as every XDN measurement shim:
 *
 * <pre>
 *   GET  /          -&gt; 200 once the runtime is connected
 *   PUT  /kv/{key}  -&gt; append the body to the shared log stream
 *   GET  /kv/{key}  -&gt; query the sequencer for the stream tail, read that address
 * </pre>
 *
 * The KV surface maps onto corfu's shared log: writes append, reads fetch the latest entry. On
 * ordinal 0 the frontend also bootstraps the cluster layout (all members, one CHAIN_REPLICATION
 * segment) before connecting, retrying while members boot; an already-bootstrapped layout makes
 * the attempt a no-op. That folds the previously manual corfu_bootstrap_cluster step into the
 * service itself.
 */
public class Frontend {

  private static final AtomicReference<CorfuRuntime> RT = new AtomicReference<>();
  private static UUID streamId;

  public static void main(String[] args) throws Exception {
    String port = env("XDN_CLUSTER_PEER_PORT", "9000");
    int size = Integer.parseInt(env("XDN_CLUSTER_SIZE", "3"));
    String ordinal = env("XDN_CLUSTER_ORDINAL", "0");
    streamId = CorfuRuntime.getStreamID("bw");

    Thread connector =
        new Thread(
            () -> {
              if (ordinal.equals("0")) {
                bootstrapUntilDone(size, port);
              }
              CorfuRuntime rt =
                  new CorfuRuntime("127.0.0.1:" + port).setCacheDisabled(true).connect();
              RT.set(rt);
              System.out.println("corfu-http: runtime connected");
            });
    connector.setDaemon(true);
    connector.start();

    HttpServer server = HttpServer.create(new InetSocketAddress(8080), 64);
    server.setExecutor(Executors.newFixedThreadPool(16));
    server.createContext("/", Frontend::handle);
    server.start();
    System.out.println("corfu-http: serving :8080");
  }

  private static void bootstrapUntilDone(int size, String port) {
    StringBuilder servers = new StringBuilder();
    for (int i = 0; i < size; i++) {
      if (i > 0) servers.append(',');
      servers.append("\"replica-").append(i).append(":").append(port).append("\"");
    }
    String json =
        "{\"layoutServers\":["
            + servers
            + "],\"sequencers\":["
            + servers
            + "],\"segments\":[{\"replicationMode\":\"CHAIN_REPLICATION\",\"start\":0,\"end\":-1,"
            + "\"stripes\":[{\"logServers\":["
            + servers
            + "]}]}],\"unresponsiveServers\":[],\"epoch\":0,\"clusterId\":\""
            + UUID.randomUUID()
            + "\"}";
    while (true) {
      try {
        BootstrapUtil.bootstrap(Layout.fromJSONString(json), 3, Duration.ofSeconds(3));
        System.out.println("corfu-http: layout bootstrapped");
        return;
      } catch (Exception e) {
        String msg = String.valueOf(e.getMessage()).toLowerCase();
        // An already-bootstrapped member means a layout is installed: done.
        if (msg.contains("already") || msg.contains("bootstrapped")) {
          System.out.println("corfu-http: layout already installed");
          return;
        }
        System.out.println("corfu-http: bootstrap retry: " + e);
        try {
          Thread.sleep(3000);
        } catch (InterruptedException ie) {
          return;
        }
      }
    }
  }

  private static void handle(HttpExchange ex) throws java.io.IOException {
    try {
      String path = ex.getRequestURI().getPath();
      String method = ex.getRequestMethod();
      CorfuRuntime rt = RT.get();
      if (rt == null) {
        respond(ex, 503, "warming up".getBytes(StandardCharsets.UTF_8));
        return;
      }
      if (!path.startsWith("/kv/")) {
        respond(ex, 200, "ok corfu-http".getBytes(StandardCharsets.UTF_8));
        return;
      }
      if (method.equals("PUT") || method.equals("POST")) {
        byte[] body = ex.getRequestBody().readAllBytes();
        rt.getStreamsView().get(streamId).append(body);
        respond(ex, 200, "OK".getBytes(StandardCharsets.UTF_8));
      } else if (method.equals("GET")) {
        long tail = rt.getSequencerView().query(streamId);
        if (tail < 0) {
          respond(ex, 404, "not found".getBytes(StandardCharsets.UTF_8));
          return;
        }
        Object payload = rt.getAddressSpaceView().read(tail).getPayload(rt);
        byte[] out =
            payload instanceof byte[]
                ? (byte[]) payload
                : String.valueOf(payload).getBytes(StandardCharsets.UTF_8);
        respond(ex, 200, out);
      } else {
        respond(ex, 405, "method not allowed".getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      respond(ex, 500, String.valueOf(e).getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void respond(HttpExchange ex, int code, byte[] body) throws java.io.IOException {
    ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
    ex.sendResponseHeaders(code, body.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(body);
    }
  }

  private static String env(String key, String dflt) {
    String v = System.getenv(key);
    return v != null && !v.isEmpty() ? v : dflt;
  }
}
