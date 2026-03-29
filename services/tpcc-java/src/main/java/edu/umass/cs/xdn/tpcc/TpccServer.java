package edu.umass.cs.xdn.tpcc;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Java TPC-C REST API for coordination granularity microbenchmark.
 *
 * <p>Uses Javalin 5 (Jetty 11) with TCP_NODELAY explicitly enabled on the
 * server connector. This prevents the Nagle + delayed ACK interaction that
 * causes a 40ms penalty when responses are split across multiple TCP segments.
 */
public class TpccServer {

    private static HikariDataSource dataSource;
    private static int warehouses;
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        String dbUrl = System.getenv("DATABASE_URL");
        if (dbUrl == null) dbUrl = "postgresql://postgres:benchpass@localhost:5432/tpcc";
        int port = intEnv("PORT", 80);
        warehouses = intEnv("WAREHOUSES", 10);

        initPool(dbUrl);

        Javalin app = Javalin.create(config -> {
            config.jetty.server(() -> {
                var server = new org.eclipse.jetty.server.Server();
                var connector = new org.eclipse.jetty.server.ServerConnector(server);
                connector.setPort(port);
                // Force TCP_NODELAY on all accepted connections
                var connFactory = connector.getConnectionFactory(
                    org.eclipse.jetty.server.HttpConnectionFactory.class);
                if (connFactory != null) {
                    connFactory.getHttpConfiguration().setSendServerVersion(false);
                }
                server.addConnector(connector);
                return server;
            });
        }).start();

        // Also set TCP_NODELAY on the connector after start
        try {
            var server = app.jettyServer().server();
            for (var c : server.getConnectors()) {
                if (c instanceof org.eclipse.jetty.server.ServerConnector sc) {
                    sc.setAcceptedTcpNoDelay(true);
                    System.out.println("TCP_NODELAY: set on ServerConnector");
                }
            }
        } catch (Exception e) {
            System.out.println("TCP_NODELAY: " + e.getMessage());
        }

        app.post("/orders", TpccServer::handleOrders);
        app.post("/init_db", TpccServer::handleInitDb);
        app.get("/health", TpccServer::handleHealth);
        System.out.printf("TPC-C Java server started on port %d (warehouses=%d)%n",
                port, warehouses);
    }

    private static void initPool(String url) {
        URI uri = URI.create(url);
        String userInfo = uri.getUserInfo();
        String jdbcUrl = "jdbc:postgresql://" + uri.getHost()
                + ":" + (uri.getPort() > 0 ? uri.getPort() : 5432)
                + uri.getPath();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        if (userInfo != null) {
            String[] parts = userInfo.split(":", 2);
            config.setUsername(parts[0]);
            if (parts.length > 1) config.setPassword(parts[1]);
        }
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(4);
        dataSource = new HikariDataSource(config);
    }

    private static void handleOrders(Context ctx) {
        try {
            JsonObject req = gson.fromJson(ctx.body(), JsonObject.class);

            int wId = req.has("w_id") ? req.get("w_id").getAsInt() : 1;
            int cId = req.has("c_id") ? req.get("c_id").getAsInt() : 1;
            int txns = req.has("txns") ? req.get("txns").getAsInt() : 1;
            int olCnt = req.has("ol_cnt") ? req.get("ol_cnt").getAsInt() : 0; // 0 = random
            boolean autocommit = !req.has("autocommit") || req.get("autocommit").getAsBoolean();

            int totalSqlCount = 0;
            long t0 = System.nanoTime();

            try (Connection conn = dataSource.getConnection()) {
                if (autocommit) {
                    conn.setAutoCommit(true);
                    for (int i = 0; i < txns; i++) {
                        totalSqlCount += NewOrderTransaction.execute(
                                conn, wId, cId, warehouses, olCnt);
                    }
                } else {
                    conn.setAutoCommit(false);
                    for (int i = 0; i < txns; i++) {
                        totalSqlCount += NewOrderTransaction.execute(
                                conn, wId, cId, warehouses, olCnt);
                        conn.commit();
                    }
                }
            }

            double durationMs = (System.nanoTime() - t0) / 1_000_000.0;

            JsonObject resp = new JsonObject();
            resp.addProperty("result", true);
            resp.addProperty("txns_executed", txns);
            resp.addProperty("sql_count", totalSqlCount);
            resp.addProperty("autocommit", autocommit);
            resp.addProperty("duration_ms", Math.round(durationMs * 100.0) / 100.0);
            ctx.contentType("application/json").result(gson.toJson(resp));

        } catch (SQLException e) {
            JsonObject err = new JsonObject();
            err.addProperty("error", e.getClass().getSimpleName());
            err.addProperty("message", e.getMessage());
            ctx.status(500).contentType("application/json").result(gson.toJson(err));
        }
    }

    private static void handleInitDb(Context ctx) {
        try {
            int wh = warehouses;
            String body = ctx.body();
            if (body != null && !body.isBlank()) {
                JsonObject req = gson.fromJson(body, JsonObject.class);
                if (req.has("warehouses")) {
                    wh = req.get("warehouses").getAsInt();
                }
            }

            try (Connection conn = dataSource.getConnection()) {
                SchemaInitializer.init(conn, wh);
            }

            ctx.contentType("application/json").result("{\"result\":true}");

        } catch (SQLException e) {
            JsonObject err = new JsonObject();
            err.addProperty("error", e.getClass().getSimpleName());
            err.addProperty("message", e.getMessage());
            ctx.status(500).contentType("application/json").result(gson.toJson(err));
        }
    }

    private static void handleHealth(Context ctx) {
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        JsonObject resp = new JsonObject();
        resp.addProperty("health", true);
        resp.addProperty("host", host);
        ctx.contentType("application/json").result(gson.toJson(resp));
    }

    private static int intEnv(String name, int defaultValue) {
        String val = System.getenv(name);
        return val != null ? Integer.parseInt(val) : defaultValue;
    }
}
