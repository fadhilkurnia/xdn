package edu.umass.cs.gigapaxos.paxosutil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * A controlled durable-write latency benchmark for RocksDB, the KV analogue of
 * {@link DBLatencyBench} (which covers the JDBC backends). It issues many
 * concurrent durable writes (put + optional fsync) of a fixed payload and
 * reports throughput and latency percentiles, so RocksDB can be compared to the
 * SQLite/Derby numbers at the same durability level.
 *
 * Usage: RocksDBLatencyBench &lt;sync|nosync&gt; &lt;threads&gt; &lt;opsPerThread&gt; &lt;payloadBytes&gt;
 */
public class RocksDBLatencyBench {
	static {
		RocksDB.loadLibrary();
	}

	public static void main(String[] args) throws Exception {
		boolean sync = args.length > 0 ? args[0].equalsIgnoreCase("sync") : true;
		int threads = args.length > 1 ? Integer.parseInt(args[1]) : 8;
		int opsPerThread = args.length > 2 ? Integer.parseInt(args[2]) : 2000;
		int payloadBytes = args.length > 3 ? Integer.parseInt(args[3]) : 1024;

		String dir = "/tmp/rocksbench_" + (sync ? "sync" : "nosync");
		recursiveDelete(new File(dir));
		new File(dir).mkdirs();

		final byte[] payload = new byte[payloadBytes];
		Arrays.fill(payload, (byte) 'x');

		try (Options options = new Options().setCreateIfMissing(true);
				RocksDB db = RocksDB.open(options, dir);
				WriteOptions wo = new WriteOptions().setSync(sync)) {

			// warmup
			for (int i = 0; i < 200; i++)
				db.put(wo, key(0, i), payload);

			final long[][] perThread = new long[threads][opsPerThread];
			final CountDownLatch start = new CountDownLatch(1);
			final CountDownLatch done = new CountDownLatch(threads);
			List<Thread> ts = new ArrayList<Thread>();
			for (int t = 0; t < threads; t++) {
				final int tid = t;
				Thread th = new Thread(() -> {
					Random rnd = new Random(tid * 7919L + 1);
					try {
						start.await();
					} catch (InterruptedException e) {
					}
					for (int i = 0; i < opsPerThread; i++) {
						long t0 = System.nanoTime();
						try {
							// read-modify-write: get then put (paxos-like)
							db.get(key(tid, rnd.nextInt(opsPerThread)));
							db.put(wo, key(tid, i), payload);
						} catch (Exception e) {
							e.printStackTrace();
						}
						perThread[tid][i] = System.nanoTime() - t0;
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

			long[] all = new long[threads * opsPerThread];
			int k = 0;
			for (long[] arr : perThread)
				for (long v : arr)
					all[k++] = v;
			Arrays.sort(all);
			double wallSec = wallNs / 1e9;
			System.out.println("=== RocksDB durable=" + sync + " threads="
					+ threads + " opsPerThread=" + opsPerThread + " payloadBytes="
					+ payloadBytes + " ===");
			System.out.printf("ops=%d  wall=%.2fs  throughput=%.0f ops/s%n",
					all.length, wallSec, all.length / wallSec);
			System.out.printf(
					"latency ms: p50=%.2f  p90=%.2f  p99=%.2f  p999=%.2f  max=%.2f  mean=%.2f%n",
					pct(all, 50), pct(all, 90), pct(all, 99), pct(all, 99.9),
					all[all.length - 1] / 1e6, mean(all) / 1e6);
			db.flush(new FlushOptions().setWaitForFlush(true));
		}
		System.exit(0);
	}

	static byte[] key(int t, int i) {
		return ("k" + t + "_" + i).getBytes();
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
