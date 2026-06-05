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
package edu.umass.cs.gigapaxos;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxospackets.StatePacket;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gigapaxos.paxosutil.RocksDBMem;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * An {@link AbstractPaxosLogger} backed by the embedded RocksDB key-value store.
 * Unlike {@link SQLPaxosLogger}, which is JDBC/SQL based, this stores paxos log
 * messages and checkpoints directly as ordered key-value pairs in RocksDB column
 * families. There is no journaling: RocksDB itself is the log.
 * <p>
 * Column families (logical "tables"):
 * <ul>
 * <li>{@code checkpoint}: paxosID -&gt; checkpoint record JSON</li>
 * <li>{@code prevCheckpoint}: paxosID -&gt; final-epoch checkpoint record JSON</li>
 * <li>{@code messages}: encoded(paxosID,version,type,slot,ballot,coord) -&gt;
 * serialized PaxosPacket bytes</li>
 * <li>{@code pause}: paxosID -&gt; serialized HotRestoreInfo</li>
 * </ul>
 * Message keys are encoded so that lexicographic byte order equals numeric order
 * (fixed-width, sign-flipped big-endian ints), making the slot-range reads used
 * by recovery and coordinator catch-up simple prefix range scans.
 *
 * @author arun
 */
public class RocksDBPaxosLogger extends AbstractPaxosLogger {

	static final Charset CHARSET = Charset.forName(SQLPaxosLogger.CHARSET);
	private static final boolean BYTEIFICATION = Config
			.getGlobalBoolean(PC.BYTEIFICATION);
	private static final boolean SYNC_WRITES = Config
			.getGlobalBoolean(PC.ROCKSDB_SYNC_WRITES);
	private static final boolean DISABLE_LOGGING = Config
			.getGlobalBoolean(PC.DISABLE_LOGGING);
	private static final long MAX_FINAL_STATE_AGE = SQLPaxosLogger.MAX_FINAL_STATE_AGE;

	private static final byte SEP = 0; // paxosID/suffix separator (IDs are
										// sanitized, no null bytes)

	// checkpoint record JSON keys
	private static final String K_VERSION = "v", K_SLOT = "s", K_BALLOTNUM = "bn",
			K_COORD = "co", K_STATE = "st", K_CREATETIME = "ct", K_MEMBERS = "m";

	private static final String CF_CHECKPOINT = "checkpoint",
			CF_PREV = "prevCheckpoint", CF_MESSAGES = "messages",
			CF_PAUSE = "pause";
	private static final String[] CFS = { CF_CHECKPOINT, CF_PREV, CF_MESSAGES,
			CF_PAUSE };

	static {
		RocksDB.loadLibrary();
	}

	private static Logger log = Logger.getLogger(PaxosManager.class.getName());

	private final String strID;
	private final String dbPath;

	private RocksDB db;
	private final Map<String, ColumnFamilyHandle> cfs = new HashMap<String, ColumnFamilyHandle>();
	private final List<ColumnFamilyOptions> cfOpts = new ArrayList<ColumnFamilyOptions>();
	private DBOptions dbOptions;
	private final WriteOptions writeOptions = new WriteOptions()
			.setSync(SYNC_WRITES);

	private RocksIterator cpCursor; // checkpoint recovery cursor
	private RocksIterator msgCursor; // message recovery cursor

	private volatile boolean closed = false;

	RocksDBPaxosLogger(int id, String strID, String dbPath,
			PaxosMessenger<?> messenger) {
		super(id, dbPath, messenger);
		this.strID = strID;
		this.dbPath = this.logDirectory + "rocksdb_" + sanitize(strID);
		try {
			open();
		} catch (RocksDBException e) {
			throw new RuntimeException("Unable to open RocksDB paxos logger at "
					+ this.dbPath + ": " + e.getMessage(), e);
		}
	}

	private static String sanitize(Object id) {
		return id.toString().replace(".", "_");
	}

	private void open() throws RocksDBException {
		new File(this.dbPath).mkdirs();
		// names of column families to open: default + any pre-existing + ours
		LinkedHashSet<String> names = new LinkedHashSet<String>();
		names.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY, CHARSET));
		if (new File(this.dbPath, "CURRENT").exists())
			try (Options o = new Options()) {
				for (byte[] n : RocksDB.listColumnFamilies(o, this.dbPath))
					names.add(new String(n, CHARSET));
			}
		for (String cf : CFS)
			names.add(cf);

		List<ColumnFamilyDescriptor> descs = new ArrayList<ColumnFamilyDescriptor>();
		for (String name : names) {
			ColumnFamilyOptions opts = RocksDBMem.cfOptions();
			this.cfOpts.add(opts);
			descs.add(new ColumnFamilyDescriptor(name.getBytes(CHARSET), opts));
		}
		List<ColumnFamilyHandle> handles = new ArrayList<ColumnFamilyHandle>();
		this.dbOptions = RocksDBMem.dbOptions();
		this.db = RocksDB.open(this.dbOptions, this.dbPath, descs, handles);
		int i = 0;
		for (String name : names)
			this.cfs.put(name, handles.get(i++));
		log.log(Level.INFO, "{0} opened RocksDB paxos logger at {1}",
				new Object[] { this, this.dbPath });
	}

	private ColumnFamilyHandle cf(String name) {
		return this.cfs.get(name);
	}

	public String toString() {
		return this.getClass().getSimpleName() + this.strID;
	}

	/* ******************* key encoding ********************* */

	// sign-flipped big-endian: signed order == lexicographic byte order
	private static byte[] encodeInt(int v) {
		v ^= 0x80000000;
		return new byte[] { (byte) (v >>> 24), (byte) (v >>> 16),
				(byte) (v >>> 8), (byte) v };
	}

	private static byte[] concat(byte[]... parts) {
		int len = 0;
		for (byte[] p : parts)
			len += p.length;
		byte[] out = new byte[len];
		int off = 0;
		for (byte[] p : parts) {
			System.arraycopy(p, 0, out, off, p.length);
			off += p.length;
		}
		return out;
	}

	private static byte[] str(String s) {
		return s.getBytes(CHARSET);
	}

	// messages key: paxosID | SEP | version | type | slot | ballotnum | coord
	private static byte[] msgKey(PaxosPacket packet) {
		int[] sb = AbstractPaxosLogger.getSlotBallot(packet); // slot,bn,coord
		return concat(str(packet.getPaxosID()), new byte[] { SEP },
				encodeInt(packet.getVersion()),
				new byte[] { (byte) packet.getType().getInt() },
				encodeInt(sb[0]), encodeInt(sb[1]), encodeInt(sb[2]));
	}

	// prefix matching all messages of a paxosID
	private static byte[] msgPrefix(String paxosID) {
		return concat(str(paxosID), new byte[] { SEP });
	}

	// prefix matching messages of (paxosID, version, type)
	private static byte[] msgVTPrefix(String paxosID, int version, int type) {
		return concat(str(paxosID), new byte[] { SEP }, encodeInt(version),
				new byte[] { (byte) type });
	}

	private static boolean startsWith(byte[] key, byte[] prefix) {
		if (key.length < prefix.length)
			return false;
		for (int i = 0; i < prefix.length; i++)
			if (key[i] != prefix[i])
				return false;
		return true;
	}

	/* ******************* packet (de)serialization ******************* */

	private byte[] toBytes(PaxosPacket packet)
			throws UnsupportedEncodingException {
		if (BYTEIFICATION && IntegerMap.allInt()
				&& packet.getType() == PaxosPacketType.ACCEPT)
			return ((AcceptPacket) packet).toBytes();
		String s = this.getPaxosPacketStringifier() != null ? this
				.getPaxosPacketStringifier().paxosPacketToString(packet)
				: packet.toString();
		return s.getBytes(CHARSET);
	}

	private PaxosPacket toPaxosPacket(byte[] bytes) {
		if (bytes == null)
			return null;
		try {
			return this.getPacketizer() != null ? this.getPacketizer()
					.stringToPaxosPacket(bytes) : PaxosPacket
					.getPaxosPacket(new String(bytes, CHARSET));
		} catch (JSONException e) {
			log.severe(this + " could not deserialize a logged packet: " + e);
			e.printStackTrace();
			return null;
		}
	}

	/* ******************* checkpoint record (de)serialization ********* */

	private void putCheckpointRecord(ColumnFamilyHandle cf, String paxosID,
			int version, Set<String> members, int slot, int ballotnum,
			int coordinator, String state, long createTime)
			throws RocksDBException {
		try {
			JSONObject json = new JSONObject();
			json.put(K_VERSION, version);
			json.put(K_SLOT, slot);
			json.put(K_BALLOTNUM, ballotnum);
			json.put(K_COORD, coordinator);
			json.put(K_STATE, state == null ? JSONObject.NULL : state);
			json.put(K_CREATETIME, createTime);
			json.put(K_MEMBERS, members == null ? JSONObject.NULL : Util
					.toJSONString(members));
			this.db.put(cf, this.writeOptions, str(paxosID),
					json.toString().getBytes(CHARSET));
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	// reads checkpoint record from cf; returns null if absent or (when
	// matchVersion) the version differs.
	private SlotBallotState readSBS(ColumnFamilyHandle cf, String paxosID,
			int version, boolean matchVersion) {
		if (this.closed)
			return null;
		try {
			byte[] val = this.db.get(cf, str(paxosID));
			if (val == null)
				return null;
			JSONObject json = new JSONObject(new String(val, CHARSET));
			int v = json.getInt(K_VERSION);
			if (matchVersion && v != version) {
				log.log(Level.INFO, "{0} asked for {1}:{2} but got version {3}",
						new Object[] { this, paxosID, version, v });
				return null;
			}
			String state = json.isNull(K_STATE) ? null : json.getString(K_STATE);
			String membersStr = json.isNull(K_MEMBERS) ? "[]" : json
					.getString(K_MEMBERS);
			return new SlotBallotState(json.getInt(K_SLOT),
					json.getInt(K_BALLOTNUM), json.getInt(K_COORD), state, v,
					json.getLong(K_CREATETIME),
					Util.stringToStringSet(membersStr));
		} catch (RocksDBException | JSONException e) {
			log.severe(this + ": " + e.getClass().getSimpleName()
					+ " reading checkpoint for " + paxosID + ": " + e);
			e.printStackTrace();
			return null;
		}
	}

	/* ******************* checkpoint methods ******************* */

	@Override
	public void putCheckpointState(String paxosID, int version,
			Set<String> group, int slot, Ballot ballot, String state, int gcSlot) {
		if (this.closed)
			return;
		try {
			putCheckpointRecord(cf(CF_CHECKPOINT), paxosID, version, group,
					slot, ballot.ballotNumber, ballot.coordinatorID, state,
					System.currentTimeMillis());
			garbageCollectMessages(paxosID, version, gcSlot);
		} catch (RocksDBException e) {
			log.severe(this + " failed putCheckpointState for " + paxosID + ": "
					+ e);
			e.printStackTrace();
		}
	}

	// delete logged ACCEPTs and DECISIONs with slot <= gcSlot for (paxosID,
	// version). PREPAREs (slot == -1) are left untouched.
	private void garbageCollectMessages(String paxosID, int version, int gcSlot) {
		if (gcSlot < 0)
			return;
		int[] types = { PaxosPacketType.ACCEPT.getInt(),
				PaxosPacketType.DECISION.getInt() };
		for (int type : types) {
			byte[] prefix = msgVTPrefix(paxosID, version, type);
			byte[] end = concat(prefix, encodeInt(gcSlot + 1));
			try {
				this.db.deleteRange(cf(CF_MESSAGES), prefix, end);
			} catch (RocksDBException e) {
				log.warning(this + " deleteRange GC failed for " + paxosID + ": "
						+ e);
			}
		}
	}

	@Override
	public boolean putCheckpointState(CheckpointTask[] tasks, boolean update) {
		if (this.closed)
			return false;
		boolean all = true;
		for (CheckpointTask task : tasks) {
			if (task == null)
				continue;
			putCheckpointState(task.paxosID, task.version, task.members,
					task.slot, task.ballot, task.state, task.gcSlot);
		}
		return all;
	}

	@Override
	public String getCheckpointState(String paxosID) {
		SlotBallotState sbs = getSlotBallotState(paxosID);
		return sbs != null ? sbs.state : null;
	}

	@Override
	public Ballot getCheckpointBallot(String paxosID) {
		SlotBallotState sbs = getSlotBallotState(paxosID);
		return sbs != null ? new Ballot(sbs.ballotnum, sbs.coordinator) : null;
	}

	@Override
	public int getCheckpointSlot(String paxosID) {
		SlotBallotState sbs = getSlotBallotState(paxosID);
		return sbs != null ? sbs.slot : -1;
	}

	@Override
	public SlotBallotState getSlotBallotState(String paxosID) {
		return readSBS(cf(CF_CHECKPOINT), paxosID, 0, false);
	}

	@Override
	public SlotBallotState getSlotBallotState(String paxosID, int version) {
		return readSBS(cf(CF_CHECKPOINT), paxosID, version, true);
	}

	@Override
	public StatePacket getStatePacket(String paxosID) {
		SlotBallotState sbs = getSlotBallotState(paxosID);
		return sbs != null ? new StatePacket(new Ballot(sbs.ballotnum,
				sbs.coordinator), sbs.slot, sbs.state) : null;
	}

	/* ******************* epoch-final checkpoint methods ******************* */

	@Override
	public boolean copyEpochFinalCheckpointState(String paxosID, int version) {
		if (this.closed)
			return false;
		// copy the current checkpoint (if its version matches) into prev CF
		SlotBallotState sbs = readSBS(cf(CF_CHECKPOINT), paxosID, version, true);
		if (sbs == null)
			return false;
		try {
			putCheckpointRecord(cf(CF_PREV), paxosID, sbs.version, sbs.members,
					sbs.slot, sbs.ballotnum, sbs.coordinator, sbs.state,
					sbs.createTime);
			return true;
		} catch (RocksDBException e) {
			log.severe(this + " failed copyEpochFinalCheckpointState for "
					+ paxosID + ": " + e);
			return false;
		}
	}

	@Override
	public StringContainer getEpochFinalCheckpointState(String paxosID,
			int version) {
		SlotBallotState sbs = readSBS(cf(CF_PREV), paxosID, version, true);
		return sbs != null
				&& (System.currentTimeMillis() - sbs.createTime < MAX_FINAL_STATE_AGE) ? new StringContainer(
				sbs.state) : null;
	}

	@Override
	public Integer getEpochFinalCheckpointVersion(String paxosID) {
		SlotBallotState sbs = readSBS(cf(CF_PREV), paxosID, 0, false);
		if (sbs != null) {
			if (System.currentTimeMillis() - sbs.createTime < MAX_FINAL_STATE_AGE)
				return sbs.version;
			deleteEpochFinalCheckpointState(paxosID, sbs.version);
		}
		return null;
	}

	@Override
	public boolean deleteEpochFinalCheckpointState(String paxosID, int version) {
		if (this.closed)
			return false;
		SlotBallotState sbs = readSBS(cf(CF_PREV), paxosID, 0, false);
		if (sbs == null)
			return true;
		if (sbs.version - version <= 0) // wraparound-safe <=
			try {
				this.db.delete(cf(CF_PREV), str(paxosID));
			} catch (RocksDBException e) {
				log.severe(this + " failed deleteEpochFinalCheckpointState: " + e);
				return false;
			}
		return true;
	}

	/* ******************* message logging methods ******************* */

	@Override
	public boolean log(PaxosPacket packet) {
		if (this.closed || DISABLE_LOGGING)
			return true;
		try {
			this.db.put(cf(CF_MESSAGES), this.writeOptions, msgKey(packet),
					toBytes(packet));
			return true;
		} catch (RocksDBException | UnsupportedEncodingException e) {
			log.severe(this + " failed to log packet: " + e);
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean logBatch(LogMessagingTask[] packets) {
		if (this.closed || DISABLE_LOGGING)
			return true;
		try (WriteBatch batch = new WriteBatch()) {
			for (LogMessagingTask lmTask : packets) {
				if (lmTask == null || lmTask.logMsg == null)
					continue;
				batch.put(cf(CF_MESSAGES), msgKey(lmTask.logMsg),
						toBytes(lmTask.logMsg));
			}
			this.db.write(this.writeOptions, batch);
			return true;
		} catch (RocksDBException | UnsupportedEncodingException e) {
			log.severe(this + " failed to log batch of " + packets.length
					+ ": " + e);
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public ArrayList<PaxosPacket> getLoggedMessages(String paxosID) {
		ArrayList<PaxosPacket> messages = new ArrayList<PaxosPacket>();
		if (this.closed)
			return messages;
		byte[] prefix = msgPrefix(paxosID);
		try (RocksIterator it = this.db.newIterator(cf(CF_MESSAGES))) {
			for (it.seek(prefix); it.isValid(); it.next()) {
				if (!startsWith(it.key(), prefix))
					break;
				PaxosPacket pp = toPaxosPacket(it.value());
				if (pp != null)
					messages.add(pp);
			}
		}
		return messages;
	}

	@Override
	public Map<Integer, PValuePacket> getLoggedAccepts(String paxosID,
			int version, int firstSlot, Integer maxSlot) {
		TreeMap<Integer, PValuePacket> accepted = new TreeMap<Integer, PValuePacket>();
		if (this.closed)
			return accepted;
		byte[] prefix = msgVTPrefix(paxosID, version,
				PaxosPacketType.ACCEPT.getInt());
		try (RocksIterator it = this.db.newIterator(cf(CF_MESSAGES))) {
			for (it.seek(prefix); it.isValid(); it.next()) {
				if (!startsWith(it.key(), prefix))
					break;
				PaxosPacket pp = toPaxosPacket(it.value());
				if (!(pp instanceof AcceptPacket))
					continue;
				AcceptPacket accept = (AcceptPacket) pp;
				int slot = accept.slot;
				if (slot - firstSlot < 0)
					continue;
				if (maxSlot != null && slot - maxSlot >= 0)
					continue;
				// keep the highest-ballot accept per slot
				if (!accepted.containsKey(slot)
						|| accepted.get(slot).ballot.compareTo(accept.ballot) < 0)
					accepted.put(slot, accept);
			}
		}
		return accepted;
	}

	@Override
	public ArrayList<PValuePacket> getLoggedDecisions(String paxosID,
			int version, int minSlot, int maxSlot) throws JSONException {
		ArrayList<PValuePacket> decisions = new ArrayList<PValuePacket>();
		if (this.closed)
			return decisions;
		byte[] prefix = msgVTPrefix(paxosID, version,
				PaxosPacketType.DECISION.getInt());
		try (RocksIterator it = this.db.newIterator(cf(CF_MESSAGES))) {
			for (it.seek(prefix); it.isValid(); it.next()) {
				if (!startsWith(it.key(), prefix))
					break;
				PaxosPacket pp = toPaxosPacket(it.value());
				if (!(pp instanceof PValuePacket))
					continue;
				PValuePacket decision = (PValuePacket) pp;
				int slot = decision.slot;
				if (slot - minSlot >= 0 && slot - maxSlot < 0)
					decisions.add(decision);
			}
		}
		return decisions;
	}

	/* ******************* pause/unpause methods ******************* */

	@Override
	protected boolean pause(String paxosID, String serialized) {
		if (this.closed)
			return false;
		try {
			this.db.put(cf(CF_PAUSE), this.writeOptions, str(paxosID),
					str(serialized));
			return true;
		} catch (RocksDBException e) {
			log.severe(this + " failed to pause " + paxosID + ": " + e);
			return false;
		}
	}

	@Override
	protected Map<String, HotRestoreInfo> pause(Map<String, HotRestoreInfo> hriMap) {
		// must return a NEW map of the successfully-paused entries: the caller
		// (PaxosManager.pause) iterates the returned map's values while
		// removing those keys from its own hriMap, so returning the same map
		// instance would throw ConcurrentModificationException.
		Map<String, HotRestoreInfo> paused = new HashMap<String, HotRestoreInfo>();
		if (this.closed)
			return paused;
		try (WriteBatch batch = new WriteBatch()) {
			for (Map.Entry<String, HotRestoreInfo> e : hriMap.entrySet())
				batch.put(cf(CF_PAUSE), str(e.getKey()), str(e.getValue()
						.toString()));
			this.db.write(this.writeOptions, batch);
			paused.putAll(hriMap); // all succeeded atomically
		} catch (RocksDBException e) {
			log.severe(this + " failed to pause batch: " + e);
			return new HashMap<String, HotRestoreInfo>();
		}
		return paused;
	}

	@Override
	protected HotRestoreInfo unpause(String paxosID) {
		if (this.closed)
			return null;
		try {
			byte[] val = this.db.get(cf(CF_PAUSE), str(paxosID));
			if (val == null)
				return null;
			HotRestoreInfo hri = new HotRestoreInfo(new String(val, CHARSET));
			this.db.delete(cf(CF_PAUSE), str(paxosID));
			return hri;
		} catch (RocksDBException e) {
			log.severe(this + " failed to unpause " + paxosID + ": " + e);
			return null;
		}
	}

	/* ******************* recovery methods ******************* */

	@Override
	public RecoveryInfo getRecoveryInfo(String paxosID) {
		SlotBallotState sbs = getSlotBallotState(paxosID);
		if (sbs == null)
			return null;
		return new RecoveryInfo(paxosID, sbs.version,
				sbs.members.toArray(new String[0]));
	}

	@Override
	public synchronized boolean initiateReadCheckpoints(boolean b) {
		if (this.closed)
			return false;
		// idempotent: callers loop `while (initiateReadCheckpoints(.));` and
		// rely on a second call returning false (cursor already created) to
		// terminate the loop.
		if (this.cpCursor != null)
			return false;
		this.cpCursor = this.db.newIterator(cf(CF_CHECKPOINT));
		this.cpCursor.seekToFirst();
		return true;
	}

	@Override
	public RecoveryInfo readNextCheckpoint(boolean readState) {
		if (this.closed || this.cpCursor == null || !this.cpCursor.isValid())
			return null;
		RecoveryInfo pri = null;
		try {
			String paxosID = new String(this.cpCursor.key(), CHARSET);
			JSONObject json = new JSONObject(new String(this.cpCursor.value(),
					CHARSET));
			int version = json.getInt(K_VERSION);
			String membersStr = json.isNull(K_MEMBERS) ? "[]" : json
					.getString(K_MEMBERS);
			String[] members = Util.stringToStringSet(membersStr).toArray(
					new String[0]);
			String state = (readState && !json.isNull(K_STATE)) ? json
					.getString(K_STATE) : null;
			pri = readState ? new RecoveryInfo(paxosID, version, members, state)
					: new RecoveryInfo(paxosID, version, members);
		} catch (JSONException e) {
			log.severe(this + " failed to parse checkpoint during recovery: " + e);
		}
		this.cpCursor.next();
		return pri;
	}

	@Override
	public synchronized boolean initiateReadMessages() {
		if (this.closed)
			return false;
		if (this.msgCursor != null)
			return false; // already initiated (see initiateReadCheckpoints)
		this.msgCursor = this.db.newIterator(cf(CF_MESSAGES));
		this.msgCursor.seekToFirst();
		return true;
	}

	@Override
	public PaxosPacket readNextMessage() {
		if (this.closed || this.msgCursor == null)
			return null;
		while (this.msgCursor.isValid()) {
			PaxosPacket pp = toPaxosPacket(this.msgCursor.value());
			this.msgCursor.next();
			if (pp != null)
				return pp;
		}
		return null;
	}

	@Override
	public void closeReadAll() {
		closeCursor(this.cpCursor);
		closeCursor(this.msgCursor);
		this.cpCursor = null;
		this.msgCursor = null;
	}

	private static void closeCursor(RocksIterator it) {
		if (it != null)
			it.close();
	}

	/* ******************* removal/close methods ******************* */

	@Override
	public boolean remove(String paxosID, int version) {
		if (this.closed)
			return false;
		try {
			this.db.delete(cf(CF_CHECKPOINT), str(paxosID));
			this.db.delete(cf(CF_PREV), str(paxosID));
			this.db.delete(cf(CF_PAUSE), str(paxosID));
			// delete all messages for this paxosID: [paxosID|SEP, paxosID|SEP+1)
			byte[] begin = msgPrefix(paxosID);
			byte[] end = concat(str(paxosID), new byte[] { (byte) (SEP + 1) });
			this.db.deleteRange(cf(CF_MESSAGES), begin, end);
			return true;
		} catch (RocksDBException e) {
			log.severe(this + " failed to remove " + paxosID + ": " + e);
			return false;
		}
	}

	@Override
	public boolean removeAll() {
		if (this.closed)
			return false;
		for (String cfName : CFS)
			try (RocksIterator it = this.db.newIterator(cf(cfName))) {
				for (it.seekToFirst(); it.isValid(); it.next())
					this.db.delete(cf(cfName), it.key());
			} catch (RocksDBException e) {
				log.severe(this + " failed removeAll on " + cfName + ": " + e);
				return false;
			}
		return true;
	}

	@Override
	public void closeImpl() {
		if (this.closed)
			return;
		this.closed = true;
		closeReadAll();
		for (ColumnFamilyHandle h : this.cfs.values())
			h.close();
		if (this.db != null)
			this.db.close();
		if (this.dbOptions != null)
			this.dbOptions.close();
		this.writeOptions.close();
		for (ColumnFamilyOptions o : this.cfOpts)
			o.close();
		log.log(Level.INFO, "{0} closed RocksDB paxos logger", this);
	}
}
