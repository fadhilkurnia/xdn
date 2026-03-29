package edu.umass.cs.xdn.tpcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * TPC-C New Order transaction implemented with explicit, countable JDBC calls.
 *
 * <p>Each SQL statement is a separate PreparedStatement execution so the total
 * SQL count per transaction is clearly measurable. For a New Order with
 * {@code ol_cnt} order lines, the count is {@code 4 + 4 * ol_cnt}:
 *
 * <pre>
 *   1  SELECT warehouse
 *   2  SELECT district (random)
 *   3  SELECT customer
 *   4  INSERT order
 *   --- per order line (x ol_cnt) ---
 *   5  SELECT item
 *   6  SELECT stock FOR UPDATE
 *   7  UPDATE stock
 *   8  INSERT order_line
 * </pre>
 *
 * <p>This is the critical file for the coordination granularity argument:
 * XDN does 1 Paxos round per HTTP request regardless of SQL count,
 * while baselines pay per SQL (rqlite), per commit (semi-sync), or per
 * fsync (OpenEBS).
 */
public class NewOrderTransaction {

    /**
     * Execute one New Order transaction.
     *
     * @return the number of individual SQL statements executed
     */
    public static int execute(Connection conn, int wId, int cId, int numWarehouses)
            throws SQLException {
        return execute(conn, wId, cId, numWarehouses, 0);
    }

    /**
     * Execute one New Order transaction.
     *
     * @param fixedOlCnt if > 0, use this fixed order line count instead of random 1-10
     * @return the number of individual SQL statements executed
     */
    public static int execute(Connection conn, int wId, int cId, int numWarehouses,
                               int fixedOlCnt) throws SQLException {
        int sqlCount = 0;
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int olCnt = fixedOlCnt > 0 ? fixedOlCnt : rng.nextInt(1, 11);
        int amount = rng.nextInt(1, 11);  // amount per line

        // --- SQL 1: SELECT warehouse ---
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, tax, ytd FROM warehouse WHERE id = ?")) {
            ps.setInt(1, wId);
            ps.executeQuery().close();
            sqlCount++;
        }

        // --- SQL 2: SELECT a random district for this warehouse ---
        int districtId;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, tax, ytd FROM district WHERE warehouse_id = ? "
                + "ORDER BY random() LIMIT 1")) {
            ps.setInt(1, wId);
            try (ResultSet rs = ps.executeQuery()) {
                districtId = rs.next() ? rs.getInt("id") : 1;
            }
            sqlCount++;
        }

        // --- SQL 3: SELECT customer ---
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, discount, last_name, credit FROM customer WHERE id = ?")) {
            ps.setInt(1, cId);
            ps.executeQuery().close();
            sqlCount++;
        }

        // --- SQL 4: INSERT order ---
        int orderId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO \"order\" (whouse_id, district_id, ol_cnt, customer_id, "
                + "entry_d, is_o_delivered) "
                + "VALUES (?, ?, ?, ?, NOW(), false) RETURNING id")) {
            ps.setInt(1, wId);
            ps.setInt(2, districtId);
            ps.setInt(3, olCnt);
            ps.setInt(4, cId);
            try (ResultSet rs = ps.executeQuery()) {
                orderId = rs.next() ? rs.getInt(1) : -1;
            }
            sqlCount++;
        }

        int maxItems = numWarehouses * 10;

        // --- Per order line (ol_cnt iterations, 4 SQL each) ---
        for (int i = 0; i < olCnt; i++) {
            int itemId = rng.nextInt(1, maxItems + 1);

            // --- SQL 5+4i: SELECT item ---
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, price, name FROM item WHERE id = ?")) {
                ps.setInt(1, itemId);
                ps.executeQuery().close();
                sqlCount++;
            }

            // --- SQL 6+4i: SELECT stock FOR UPDATE ---
            int stockId;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, quantity FROM stock "
                    + "WHERE warehouse_id = ? AND item_id = ? FOR UPDATE")) {
                ps.setInt(1, wId);
                ps.setInt(2, itemId);
                try (ResultSet rs = ps.executeQuery()) {
                    stockId = rs.next() ? rs.getInt("id") : -1;
                }
                sqlCount++;
            }

            // --- SQL 7+4i: UPDATE stock ---
            if (stockId > 0) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE stock SET quantity = quantity - ?, "
                        + "order_cnt = order_cnt + 1 WHERE id = ?")) {
                    ps.setInt(1, amount);
                    ps.setInt(2, stockId);
                    ps.executeUpdate();
                    sqlCount++;
                }
            } else {
                sqlCount++; // count even if skipped, for consistency
            }

            // --- SQL 8+4i: INSERT order_line ---
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO order_line (item_id, amount, order_id) "
                    + "VALUES (?, ?, ?)")) {
                ps.setInt(1, itemId);
                ps.setInt(2, amount);
                ps.setInt(3, orderId);
                ps.executeUpdate();
                sqlCount++;
            }
        }

        return sqlCount;
    }
}
