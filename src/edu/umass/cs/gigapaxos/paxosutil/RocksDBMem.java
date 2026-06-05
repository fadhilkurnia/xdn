/*
 * Copyright (c) 2015 University of Massachusetts
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package edu.umass.cs.gigapaxos.paxosutil;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.utils.Config;

/**
 * Process-wide, bounded memory configuration for all embedded RocksDB instances
 * (the {@code RocksDBPaxosLogger} and {@code RocksDBReconfiguratorDB}). RocksDB's
 * default per-column-family memory (a 64 MB write buffer and an 8 MB block cache
 * each) does not bound total memory and grows with the number of column families
 * and with write load. That is unsuitable for a small-memory reconfigurator
 * (target: a 0.5&ndash;1 GB machine).
 * <p>
 * This class creates a single shared {@link LRUCache} and {@link
 * WriteBufferManager} that ALL RocksDB instances in the JVM share, so the total
 * native memory used by RocksDB is bounded regardless of how many nodes / column
 * families exist or how write-heavy the load is:
 * <ul>
 * <li>block cache (index/filter/data blocks) is capped at
 * {@link PC#ROCKSDB_BLOCK_CACHE_MB} MB total;</li>
 * <li>memtable (write buffer) memory is capped at
 * {@link PC#ROCKSDB_MEMTABLE_BUDGET_MB} MB total via a WriteBufferManager whose
 * usage is also charged against the same block cache, so the overall RocksDB
 * native budget is essentially the block-cache size.</li>
 * </ul>
 * Per-column-family write buffers are kept small
 * ({@link PC#ROCKSDB_WRITE_BUFFER_MB} MB) so a single column family cannot by
 * itself reserve a large memtable.
 *
 * @author arun
 */
public class RocksDBMem {

	private static final long MB = 1024 * 1024;

	/** Total block-cache budget shared by all RocksDB instances. */
	public static final long BLOCK_CACHE_BYTES = Config
			.getGlobalLong(PC.ROCKSDB_BLOCK_CACHE_MB) * MB;
	/** Total memtable budget shared by all RocksDB instances. */
	public static final long MEMTABLE_BUDGET_BYTES = Config
			.getGlobalLong(PC.ROCKSDB_MEMTABLE_BUDGET_MB) * MB;
	/** Per-column-family write buffer (memtable) size. */
	public static final long WRITE_BUFFER_BYTES = Config
			.getGlobalLong(PC.ROCKSDB_WRITE_BUFFER_MB) * MB;

	// One shared cache and write-buffer manager for the whole JVM. These are
	// long-lived native objects intentionally never closed (process lifetime).
	private static final Cache SHARED_CACHE = new LRUCache(BLOCK_CACHE_BYTES);
	private static final WriteBufferManager SHARED_WBM = new WriteBufferManager(
			MEMTABLE_BUDGET_BYTES, SHARED_CACHE);

	private RocksDBMem() {
	}

	/**
	 * @return DBOptions that bound memtable memory (shared WriteBufferManager),
	 *         file handles, and background work, suitable for a small-memory
	 *         deployment. The caller owns and must close the returned object.
	 */
	public static DBOptions dbOptions() {
		return new DBOptions().setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true)
				.setWriteBufferManager(SHARED_WBM)
				.setMaxBackgroundJobs(2)
				// bound the number of open files (each costs memory)
				.setMaxOpenFiles(64);
	}

	/**
	 * @return ColumnFamilyOptions with a small write buffer and the shared block
	 *         cache (with index/filter blocks also charged to the cache so they
	 *         are bounded). The caller owns and must close the returned object.
	 */
	public static ColumnFamilyOptions cfOptions() {
		BlockBasedTableConfig table = new BlockBasedTableConfig()
				.setBlockCache(SHARED_CACHE)
				.setCacheIndexAndFilterBlocks(true)
				.setPinL0FilterAndIndexBlocksInCache(false);
		return new ColumnFamilyOptions()
				.setWriteBufferSize(WRITE_BUFFER_BYTES)
				.setMaxWriteBufferNumber(2)
				.setCompressionType(CompressionType.LZ4_COMPRESSION)
				.setCompactionStyle(CompactionStyle.LEVEL)
				.setLevelCompactionDynamicLevelBytes(true)
				.setTableFormatConfig(table);
	}
}
