package edu.umass.cs.xdn.tpcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Creates and populates the TPC-C schema in PostgreSQL.
 *
 * <p>Schema matches the existing Python TPC-C service (services/tpcc/)
 * with the {@code dtata2} typo fixed to {@code data2}.
 */
public class SchemaInitializer {

    private static final String[] CITIES =
            {"Moscow", "St. Petersbrg", "Pshkin", "Oraneinbaum", "Vladivostok"};
    private static final String[] NAMES =
            {"Ivan", "Evgeniy", "Alexander", "Fedor", "Julia", "Stephany",
             "Sergey", "Natalya", "Keanu", "Jhon", "Harry", "James"};
    private static final String[] LAST_NAMES =
            {"Petrov", "Ivanov", "Andreev", "Mils", "Smith", "Anderson",
             "Dominov", "Tishenko", "Zhitnikov"};

    public static void init(Connection conn, int warehouses) throws SQLException {
        long t0 = System.currentTimeMillis();
        dropTables(conn);
        createTables(conn);
        populate(conn, warehouses);
        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("Schema initialized: %d warehouses in %d ms%n", warehouses, elapsed);
    }

    private static void dropTables(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            // Drop in dependency order
            st.execute("DROP TABLE IF EXISTS order_line CASCADE");
            st.execute("DROP TABLE IF EXISTS history CASCADE");
            st.execute("DROP TABLE IF EXISTS new_order CASCADE");
            st.execute("DROP TABLE IF EXISTS \"order\" CASCADE");
            st.execute("DROP TABLE IF EXISTS stock CASCADE");
            st.execute("DROP TABLE IF EXISTS item CASCADE");
            st.execute("DROP TABLE IF EXISTS customer CASCADE");
            st.execute("DROP TABLE IF EXISTS district CASCADE");
            st.execute("DROP TABLE IF EXISTS warehouse CASCADE");
        }
    }

    private static void createTables(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE warehouse (
                    id SERIAL PRIMARY KEY,
                    number INTEGER NOT NULL,
                    street_1 VARCHAR(255) NOT NULL,
                    street_2 VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NOT NULL,
                    w_zip VARCHAR(255) NOT NULL,
                    tax DOUBLE PRECISION NOT NULL,
                    ytd DOUBLE PRECISION NOT NULL
                )""");

            st.execute("""
                CREATE TABLE district (
                    id SERIAL PRIMARY KEY,
                    warehouse_id INTEGER NOT NULL REFERENCES warehouse(id),
                    name VARCHAR(255) NOT NULL,
                    street_1 VARCHAR(255) NOT NULL,
                    street_2 VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NOT NULL,
                    d_zip VARCHAR(255) NOT NULL,
                    tax DOUBLE PRECISION NOT NULL,
                    ytd DOUBLE PRECISION NOT NULL
                )""");
            st.execute("CREATE INDEX idx_district_warehouse ON district(warehouse_id)");

            st.execute("""
                CREATE TABLE customer (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    middle_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    street_1 VARCHAR(255) NOT NULL,
                    street_2 VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NOT NULL,
                    c_zip VARCHAR(255) NOT NULL,
                    phone VARCHAR(255) NOT NULL,
                    since TIMESTAMP NOT NULL,
                    credit VARCHAR(255) NOT NULL,
                    credit_lim DOUBLE PRECISION NOT NULL,
                    discount DOUBLE PRECISION NOT NULL,
                    delivery_cnt INTEGER NOT NULL,
                    payment_cnt INTEGER NOT NULL,
                    balance DOUBLE PRECISION NOT NULL,
                    ytd_payment DOUBLE PRECISION NOT NULL,
                    data1 TEXT NOT NULL,
                    data2 TEXT NOT NULL,
                    district_id INTEGER REFERENCES district(id)
                )""");
            st.execute("CREATE INDEX idx_customer_district ON customer(district_id)");

            st.execute("""
                CREATE TABLE item (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    data VARCHAR(255) NOT NULL
                )""");

            st.execute("""
                CREATE TABLE stock (
                    id SERIAL PRIMARY KEY,
                    warehouse_id INTEGER NOT NULL REFERENCES warehouse(id),
                    item_id INTEGER NOT NULL REFERENCES item(id),
                    quantity INTEGER NOT NULL,
                    ytd INTEGER NOT NULL,
                    order_cnt INTEGER NOT NULL,
                    remote_cnt INTEGER NOT NULL,
                    data VARCHAR(255) NOT NULL
                )""");
            st.execute("CREATE INDEX idx_stock_warehouse ON stock(warehouse_id)");
            st.execute("CREATE INDEX idx_stock_item ON stock(item_id)");
            st.execute("CREATE INDEX idx_stock_wh_item ON stock(warehouse_id, item_id)");

            st.execute("""
                CREATE TABLE "order" (
                    id SERIAL PRIMARY KEY,
                    whouse_id INTEGER REFERENCES warehouse(id),
                    district_id INTEGER REFERENCES district(id),
                    ol_cnt INTEGER NOT NULL,
                    customer_id INTEGER REFERENCES customer(id),
                    entry_d TIMESTAMP NOT NULL,
                    is_o_delivered BOOLEAN NOT NULL DEFAULT false
                )""");
            st.execute("CREATE INDEX idx_order_warehouse ON \"order\"(whouse_id)");
            st.execute("CREATE INDEX idx_order_district ON \"order\"(district_id)");
            st.execute("CREATE INDEX idx_order_customer ON \"order\"(customer_id)");

            st.execute("""
                CREATE TABLE order_line (
                    id SERIAL PRIMARY KEY,
                    delivery_d TIMESTAMP,
                    item_id INTEGER REFERENCES item(id),
                    amount INTEGER NOT NULL,
                    order_id INTEGER REFERENCES "order"(id)
                )""");
            st.execute("CREATE INDEX idx_orderline_item ON order_line(item_id)");
            st.execute("CREATE INDEX idx_orderline_order ON order_line(order_id)");

            st.execute("""
                CREATE TABLE history (
                    id SERIAL PRIMARY KEY,
                    date TIMESTAMP NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    data VARCHAR(255) NOT NULL,
                    customer_id INTEGER REFERENCES customer(id)
                )""");
            st.execute("CREATE INDEX idx_history_customer ON history(customer_id)");
        }
    }

    private static void populate(Connection conn, int n) throws SQLException {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        conn.setAutoCommit(false);

        // Warehouses
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO warehouse (number, street_1, street_2, city, w_zip, tax, ytd) "
                + "VALUES (?, ?, ?, ?, ?, ?, 0)")) {
            for (int i = 1; i <= n; i++) {
                ps.setInt(1, i);
                ps.setString(2, "w_st " + i);
                ps.setString(3, "w_st2 " + i);
                ps.setString(4, CITIES[rng.nextInt(CITIES.length)]);
                ps.setString(5, "w_zip " + i);
                ps.setDouble(6, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        // Districts (10 per warehouse)
        int dCnt = 0;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO district (warehouse_id, name, street_1, street_2, city, d_zip, tax, ytd) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, 0)")) {
            for (int i = 1; i <= n; i++) {
                for (int j = 0; j < 10; j++) {
                    ps.setInt(1, i);
                    ps.setString(2, "dist " + i + " " + j);
                    ps.setString(3, "d_st " + j);
                    ps.setString(4, "d_st2 " + j);
                    ps.setString(5, CITIES[rng.nextInt(CITIES.length)]);
                    ps.setString(6, "d_zip " + j);
                    ps.setDouble(7, j);
                    ps.addBatch();
                    dCnt++;
                }
            }
            ps.executeBatch();
        }

        // Customers (10 * n)
        Timestamp since = Timestamp.valueOf(LocalDateTime.of(2005, 7, 14, 12, 30));
        double[] discounts = {0, 10, 15, 20, 30};
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO customer (first_name, middle_name, last_name, "
                + "street_1, street_2, city, c_zip, phone, since, credit, "
                + "credit_lim, discount, delivery_cnt, payment_cnt, balance, "
                + "ytd_payment, data1, data2, district_id) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,0,1000000,0,?,?,?)")) {
            for (int i = 0; i < 10 * n; i++) {
                ps.setString(1, NAMES[rng.nextInt(NAMES.length)]);
                ps.setString(2, NAMES[rng.nextInt(NAMES.length)]);
                ps.setString(3, LAST_NAMES[rng.nextInt(LAST_NAMES.length)]);
                ps.setString(4, "c_st " + i);
                ps.setString(5, "c_st2 " + i);
                ps.setString(6, CITIES[rng.nextInt(CITIES.length)]);
                ps.setString(7, "c_zip " + i);
                ps.setString(8, "phone");
                ps.setTimestamp(9, since);
                ps.setString(10, "credit");
                ps.setDouble(11, rng.nextInt(1000, 100001));
                ps.setDouble(12, discounts[rng.nextInt(discounts.length)]);
                ps.setString(13, "customer " + i);
                ps.setString(14, "hello " + i);
                ps.setInt(15, rng.nextInt(1, dCnt + 1));
                ps.addBatch();
                if (i % 500 == 499) ps.executeBatch();
            }
            ps.executeBatch();
        }

        // Items (10 * n)
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO item (name, price, data) VALUES (?, ?, 'data')")) {
            for (int i = 1; i <= n * 10; i++) {
                ps.setString(1, "item " + i);
                ps.setDouble(2, rng.nextInt(1, 100001));
                ps.addBatch();
                if (i % 500 == 499) ps.executeBatch();
            }
            ps.executeBatch();
        }

        // Stock (10*n items x n warehouses)
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO stock (warehouse_id, item_id, quantity, ytd, "
                + "order_cnt, remote_cnt, data) VALUES (?, ?, 100000, ?, 0, 0, 'data')")) {
            int batch = 0;
            for (int i = 1; i <= n * 10; i++) {
                for (int j = 1; j <= n; j++) {
                    ps.setInt(1, j);
                    ps.setInt(2, i);
                    ps.setInt(3, rng.nextInt(1, 100001));
                    ps.addBatch();
                    batch++;
                    if (batch % 1000 == 0) ps.executeBatch();
                }
            }
            ps.executeBatch();
        }

        conn.commit();
        conn.setAutoCommit(true);
    }
}
