import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.corfudb.runtime.CorfuRuntime;

/**
 * HTTP key-value frontend for the CorfuDB reference service (services/corfu-cluster). Runs as the
 * entry sidecar in each replica's pod.
 *
 * <p>Corfu is the irreducibly client-driven case: replication logic lives IN the client library
 * (token from the sequencer, then a write to each log unit in chain order), and a corfu server
 * neither forwards nor coordinates on a client's behalf. A shim restricted to the co-located
 * server literally cannot append. So this shim embeds the corfu client seeded with the LOCAL
 * member only, and the client-side fan-out that follows is the service's own protocol shape, not
 * shim routing. Layout bootstrap belongs to the service (the member entrypoint self-bootstraps
 * from ordinal 0); the shim only connects.
 *
 * <pre>
 *   GET  /          -&gt; 200 once the runtime is connected
 *   PUT  /kv/{key}  -&gt; append the body to the shared log stream
 *   GET  /kv/{key}  -&gt; query the sequencer for the stream tail, read that address
 * </pre>
 */
public class Frontend {

  private static final AtomicReference<CorfuRuntime> RT = new AtomicReference<>();
  private static UUID streamId;

  public static void main(String[] args) throws Exception {
    String port = env("XDN_CLUSTER_PEER_PORT", "9000");
    // The co-located member binds (and advertises) its replica-N overlay
    // alias, not loopback (see services/corfu-cluster/entrypoint.sh), so
    // the local member is dialed by its own canonical name from the env
    // contract. Still zero discovery: XDN_CLUSTER_SELF IS this pod's member.
    String self = env("XDN_CLUSTER_SELF", "127.0.0.1");
    streamId = CorfuRuntime.getStreamID("bw");

    Thread connector =
        new Thread(
            () -> {
              CorfuRuntime rt =
                  new CorfuRuntime(self + ":" + port).setCacheDisabled(true).connect();
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
