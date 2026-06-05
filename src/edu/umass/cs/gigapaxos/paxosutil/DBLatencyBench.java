package edu.umass.cs.gigapaxos.paxosutil;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * A controlled, repeatable DB write-path latency benchmark. It drives many
 * concurrent read-modify-write transactions (BEGIN; SELECT; UPDATE; COMMIT) at
 * a small set of rows through the real DataSource machinery (C3P0 or
 * {@link SimpleDataSource}) and reports throughput and latency percentiles plus
 * a breakdown of SQLite BUSY/BUSY_SNAPSHOT errors. This is the transaction
 * shape gigapaxos uses (read state, then write), and the one that exposes
 * SQLite's single-writer contention.
 *
 * Usage: DBLatencyBench &lt;mode&gt; &lt;threads&gt; &lt;opsPerThread&gt; &lt;rows&gt; &lt;payloadBytes&gt;
 * mode in { derby-c3p0, derby-simple, sqlite-pool, sqlite-immediate, sqlite-single }
 */
public class DBLatencyBench {

	static final String DIR_BASE = "/tmp/dbbench";

	public static void main(String[] args) throws Exception {
		String mode = args.length > 0 ? args[0] : "sqlite-pool";
		int threads = args.length > 1 ? Integer.parseInt(args[1]) : 8;
		int opsPerThread = args.length > 2 ? Integer.parseInt(args[2]) : 2000;
		int rows = args.length > 3 ? Integer.parseInt(args[3]) : 200;
		int payloadBytes = args.length > 4 ? Integer.parseInt(args[4]) : 1024;

		String dir = DIR_BASE + "_" + mode;
		recursiveDelete(new File(dir));
		new File(dir).mkdirs();

		boolean sqlite = mode.startsWith("sqlite");
		DataSource ds = buildDataSource(mode, dir);

		String blobType = sqlite ? "text" : "varchar(8192)";
		try (Connection c = ds.getConnection()) {
			c.setAutoCommit(true);
			try (PreparedStatement p = c.prepareStatement("drop table bench")) {
				p.execute();
			} catch (SQLException ignore) {
			}
			try (PreparedStatement p = c.prepareStatement("create table bench(id int primary key, payload "
					+ blobType + ")")) {
				p.execute();
			}
			try (PreparedStatement p = c
					.prepareStatement("insert into bench(id,payload) values(?,?)")) {
				for (int i = 0; i < rows; i++) {
					p.setInt(1, i);
					p.setString(2, "init");
					p.executeUpdate();
				}
			}
		}

		final String payload = makePayload(payloadBytes);
		final DataSource fds = ds;
		// warmup
		runLoad(fds, 2, 50, rows, payload, new long[0], null);

		final long[][] perThread = new long[threads][opsPerThread];
		final ConcurrentHashMap<String, AtomicInteger> errors = new ConcurrentHashMap<String, AtomicInteger>();
		final CountDownLatch start = new CountDownLatch(1);
		final CountDownLatch done = new CountDownLatch(threads);
		List<Thread> ts = new ArrayList<Thread>();
		for (int t = 0; t < threads; t++) {
			final int tid = t;
			Thread th = new Thread(() -> {
				Random rnd = new Random(tid * 9973L + 7);
				try {
					start.await();
				} catch (InterruptedException e) {
				}
				for (int i = 0; i < opsPerThread; i++) {
					long t0 = System.nanoTime();
					perThread[tid][i] = doTxn(fds, rows, payload, rnd, errors);
					if (perThread[tid][i] < 0)
						perThread[tid][i] = System.nanoTime() - t0; // record even on error
				}
				done.countDown();
			});
			ts.add(th);
			th.start();
		}
		long wall0 = System.nanoTime();
		start.countDown();
		done.await();
		long wallNs = System.nanoTime() - wall0;

		// merge
		long[] all = new long[threads * opsPerThread];
		int k = 0;
		for (long[] arr : perThread)
			for (long v : arr)
				all[k++] = v;
		Arrays.sort(all);
		double totalOps = all.length;
		double wallSec = wallNs / 1e9;

		System.out.println("=== mode=" + mode + " threads=" + threads
				+ " opsPerThread=" + opsPerThread + " rows=" + rows
				+ " payloadBytes=" + payloadBytes + " ===");
		System.out.printf("ops=%d  wall=%.2fs  throughput=%.0f ops/s%n",
				(long) totalOps, wallSec, totalOps / wallSec);
		System.out.printf(
				"latency ms: p50=%.2f  p90=%.2f  p99=%.2f  p999=%.2f  max=%.2f  mean=%.2f%n",
				pct(all, 50), pct(all, 90), pct(all, 99), pct(all, 99.9),
				all[all.length - 1] / 1e6, mean(all) / 1e6);
		int totalErr = 0;
		for (String key : errors.keySet()) {
			int v = errors.get(key).get();
			totalErr += v;
			System.out.println("  error[" + key + "]=" + v);
		}
		System.out.println("total_errors=" + totalErr + " ("
				+ String.format("%.2f", 100.0 * totalErr / totalOps) + "% of ops)");
		if (ds instanceof ComboPooledDataSource)
			((ComboPooledDataSource) ds).close();
		System.exit(0);
	}

	/* one read-modify-write transaction; returns latency ns, or -1 on error */
	static long doTxn(DataSource ds, int rows, String payload, Random rnd,
			ConcurrentHashMap<String, AtomicInteger> errors) {
		int id = rnd.nextInt(rows);
		long t0 = System.nanoTime();
		Connection c = null;
		try {
			c = ds.getConnection();
			c.setAutoCommit(false);
			try (PreparedStatement sel = c
					.prepareStatement("select payload from bench where id=?")) {
				sel.setInt(1, id);
				try (ResultSet rs = sel.executeQuery()) {
					rs.next();
				}
			}
			try (PreparedStatement upd = c
					.prepareStatement("update bench set payload=? where id=?")) {
				upd.setString(1, payload);
				upd.setInt(2, id);
				upd.executeUpdate();
			}
			c.commit();
			return System.nanoTime() - t0;
		} catch (SQLException e) {
			record(errors, e);
			if (c != null)
				try {
					c.rollback();
				} catch (SQLException ignore) {
				}
			return -1;
		} finally {
			if (c != null)
				try {
					c.close();
				} catch (SQLException ignore) {
				}
		}
	}

	static void runLoad(DataSource ds, int threads, int ops, int rows,
			String payload, long[] sink, ConcurrentHashMap<String, AtomicInteger> e)
			throws InterruptedException {
		ConcurrentHashMap<String, AtomicInteger> errs = e != null ? e
				: new ConcurrentHashMap<String, AtomicInteger>();
		Thread[] ts = new Thread[threads];
		for (int t = 0; t < threads; t++) {
			final int tid = t;
			ts[t] = new Thread(() -> {
				Random rnd = new Random(tid + 1L);
				for (int i = 0; i < ops; i++)
					doTxn(ds, rows, payload, rnd, errs);
			});
			ts[t].start();
		}
		for (Thread t : ts)
			t.join();
	}

	static void record(ConcurrentHashMap<String, AtomicInteger> errors,
			SQLException e) {
		String msg = e.getMessage() == null ? e.getClass().getSimpleName()
				: e.getMessage();
		String key = msg.contains("BUSY_SNAPSHOT") ? "BUSY_SNAPSHOT"
				: msg.toLowerCase().contains("locked") ? "BUSY/locked"
						: e.getClass().getSimpleName();
		errors.computeIfAbsent(key, x -> new AtomicInteger()).incrementAndGet();
	}

	static DataSource buildDataSource(String mode, String dir) throws Exception {
		Properties props = new Properties();
		if (mode.startsWith("derby")) {
			Class.forName(SQL.getDriver(SQL.SQLType.EMBEDDED_DERBY));
			String url = SQL.getProtocolOrURL(SQL.SQLType.EMBEDDED_DERBY) + dir
					+ "/benchdb;create=true";
			props.put("user", "bench");
			props.put("password", "bench");
			if (mode.equals("derby-c3p0")) {
				ComboPooledDataSource cpds = new ComboPooledDataSource();
				cpds.setDriverClass(SQL.getDriver(SQL.SQLType.EMBEDDED_DERBY));
				cpds.setJdbcUrl(url);
				cpds.setUser("bench");
				cpds.setPassword("bench");
				cpds.setMaxPoolSize(100);
				return cpds;
			}
			return new SimpleDataSource(url, props, 8, 0);
		}
		// sqlite
		Class.forName(SQL.getDriver(SQL.SQLType.EMBEDDED_SQLITE));
		String url = SQL.getProtocolOrURL(SQL.SQLType.EMBEDDED_SQLITE) + dir
				+ "/bench.db";
		props.put("journal_mode", "WAL");
		// "-full" suffix => synchronous=FULL (fsync every commit, like Derby);
		// otherwise NORMAL (fsync only at WAL checkpoint: durable across an app
		// crash, may lose last commits on an OS/power crash).
		props.put("synchronous", mode.endsWith("-full") ? "FULL" : "NORMAL");
		props.put("busy_timeout", "30000");
		int maxTotal = 0;
		if (mode.startsWith("sqlite-immediate"))
			props.put("transaction_mode", "IMMEDIATE");
		else if (mode.startsWith("sqlite-single"))
			maxTotal = 1;
		int maxIdle = maxTotal > 0 ? maxTotal : 8;
		return new SimpleDataSource(url, props, maxIdle, maxTotal);
	}

	static String makePayload(int n) {
		StringBuilder sb = new StringBuilder(n);
		for (int i = 0; i < n; i++)
			sb.append((char) ('a' + (i % 26)));
		return sb.toString();
	}

	static double pct(long[] sorted, double p) {
		int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
		idx = Math.max(0, Math.min(sorted.length - 1, idx));
		return sorted[idx] / 1e6;
	}

	static double mean(long[] a) {
		double s = 0;
		for (long v : a)
			s += v;
		return s / a.length;
	}

	static void recursiveDelete(File f) {
		if (f == null || !f.exists())
			return;
		if (f.isDirectory())
			for (File c : f.listFiles())
				recursiveDelete(c);
		f.delete();
	}
}
