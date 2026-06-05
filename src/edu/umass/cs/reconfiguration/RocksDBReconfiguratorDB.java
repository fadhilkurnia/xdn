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
package edu.umass.cs.reconfiguration;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.rocksdb.WriteOptions;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxosutil.RocksDBMem;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.ReconfigureUponActivesChange;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.StringLocker;
import edu.umass.cs.utils.Util;

/**
 * An {@link AbstractReconfiguratorDB} backed by the embedded RocksDB key-value
 * store, an alternative to the JDBC-based {@link SQLReconfiguratorDB}. Selected
 * via the {@link RC#RECONFIGURATOR_DB} config ("ROCKSDB").
 * <p>
 * Records, demand profiles, pending markers, and node-config entries are stored
 * in RocksDB column families keyed by service name (or nodeID|version). Unlike
 * SQLReconfiguratorDB, paxos-level checkpoints of an RC group are returned
 * inline as the concatenated record JSONs rather than via a file + socket
 * transfer server; {@link AbstractReconfiguratorDB} (through paxos) treats the
 * returned string as the state directly. This keeps the implementation small;
 * the trade-off is that a single RC group's checkpoint must fit in a paxos
 * checkpoint, which is fine for the bounded number of records per RC group.
 *
 * @author arun
 *
 * @param <NodeIDType>
 */
public class RocksDBReconfiguratorDB<NodeIDType> extends
		AbstractReconfiguratorDB<NodeIDType> {

	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
	private static final boolean COMBINE_DEMAND_STATS = Config
			.getGlobalBoolean(RC.COMBINE_DEMAND_STATS);

	private static final String CF_RECORDS = "records", CF_DEMAND = "demand",
			CF_PENDING = "pending", CF_NODECONFIG = "nodeConfig";
	private static final String[] CFS = { CF_RECORDS, CF_DEMAND, CF_PENDING,
			CF_NODECONFIG };

	// wrapper JSON keys for a stored record: rcGroupName + stringified record
	private static final String W_GROUP = "g", W_RECORD = "r";

	static {
		RocksDB.loadLibrary();
	}

	private static final Logger log = ReconfigurationConfig.getLogger();

	protected String logDirectory;
	private final String dbPath;

	private RocksDB db;
	private final Map<String, ColumnFamilyHandle> cfs = new HashMap<String, ColumnFamilyHandle>();
	private final List<ColumnFamilyOptions> cfOpts = new ArrayList<ColumnFamilyOptions>();
	private DBOptions dbOptions;
	// Async writes (RocksDB WAL, no per-write fsync): the reconfigurator DB is
	// paxos-backed (records are reconstructable from the paxos log + epoch-final
	// checkpoints), so losing the last few writes on an OS crash is recovered by
	// paxos replay. This mirrors SQLReconfiguratorDB's async in-memory DiskMap.
	private final WriteOptions writeOptions = new WriteOptions().setSync(false);
	private final StringLocker stringLocker = new StringLocker();

	private RocksIterator activeCursor;
	private NodeIDType cursorActive;

	private volatile boolean closed = true;

	/**
	 * @param myID
	 * @param nc
	 */
	public RocksDBReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc) {
		super(myID, nc);
		this.logDirectory = Config.getGlobalString(PC.GIGAPAXOS_DATA_DIR) + "/"
				+ RC.RECONFIGURATION_DB_DIR.getDefaultValue() + "/";
		this.dbPath = this.logDirectory + "rocksdb_rcdb_" + sanitize(myID);
		try {
			open();
		} catch (RocksDBException e) {
			throw new RuntimeException("Unable to open RocksDB reconfigurator DB at "
					+ this.dbPath + ": " + e.getMessage(), e);
		}
		this.closed = false;
		initAdjustSoftNodeConfig();
		initAdjustSoftActiveNodeConfig();
		log.log(Level.INFO, "{0} initialized with reconfigurators {1}",
				new Object[] { this, this.consistentNodeConfig.getReconfigurators() });
	}

	private static String sanitize(Object id) {
		return id.toString().replace(".", "_");
	}

	public String toString() {
		return "RocksDBRCDB" + this.myID;
	}

	private void open() throws RocksDBException {
		new File(this.dbPath).mkdirs();
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
	}

	private ColumnFamilyHandle cf(String name) {
		return this.cfs.get(name);
	}

	private static byte[] bytes(String s) {
		return s.getBytes(CHARSET);
	}

	/* ******************* record storage primitives ******************* */

	@Override
	public synchronized ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name) {
		if (this.closed)
			return null;
		try {
			byte[] val = this.db.get(cf(CF_RECORDS), bytes(name));
			if (val == null)
				return null;
			JSONObject wrapper = new JSONObject(new String(val, CHARSET));
			ReconfigurationRecord<NodeIDType> record = new ReconfigurationRecord<NodeIDType>(
					new JSONObject(wrapper.getString(W_RECORD)),
					this.consistentNodeConfig);
			if (!wrapper.isNull(W_GROUP))
				record.setRCGroupName(wrapper.getString(W_GROUP));
			return record;
		} catch (RocksDBException | JSONException e) {
			log.severe(this + " failed to read RC record for " + name + ": " + e);
			e.printStackTrace();
			return null;
		}
	}

	private synchronized void putReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record) {
		String rcGroupName = record.getRCGroupName();
		if (rcGroupName == null)
			rcGroupName = this.getRCGroupName(record.getName());
		putReconfigurationRecord(record, rcGroupName);
	}

	private synchronized void putReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record, String rcGroupName) {
		if (this.closed)
			return;
		record.setRCGroupName(rcGroupName);
		try {
			JSONObject wrapper = new JSONObject();
			wrapper.put(W_GROUP, rcGroupName == null ? JSONObject.NULL
					: rcGroupName);
			wrapper.put(W_RECORD, record.toString());
			this.db.put(cf(CF_RECORDS), this.writeOptions,
					bytes(record.getName()), bytes(wrapper.toString()));
		} catch (RocksDBException | JSONException e) {
			log.severe(this + " failed to put RC record " + record.getName()
					+ ": " + e);
			e.printStackTrace();
		}
	}

	// low-level delete from records + pending + demand
	private boolean deleteReconfigurationRecordDB(String name, Integer epoch) {
		if (epoch != null) {
			ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
			if (record == null || record.getEpoch() != epoch)
				return false;
		}
		boolean deleted = delete(CF_RECORDS, name);
		delete(CF_PENDING, name);
		delete(CF_DEMAND, name);
		return deleted;
	}

	private boolean delete(String cfName, String key) {
		try {
			this.db.delete(cf(cfName), bytes(key));
			return true;
		} catch (RocksDBException e) {
			log.severe(this + " failed to delete " + key + " from " + cfName
					+ ": " + e);
			return false;
		}
	}

	/* ******************* overridden interface methods ******************* */

	@Override
	public synchronized boolean updateDemandStats(DemandReport<NodeIDType> report) {
		if (this.closed)
			return false;
		JSONObject update = report.getStats();
		JSONObject historic = getDemandStatsJSON(report.getServiceName());
		JSONObject combined = update;
		if (historic != null && COMBINE_DEMAND_STATS)
			// demand stats are advisory; keep the latest report when combining
			// is requested rather than replicating the profile-merge machinery.
			combined = update;
		try {
			this.db.put(cf(CF_DEMAND), this.writeOptions,
					bytes(report.getServiceName()), bytes(combined.toString()));
		} catch (RocksDBException e) {
			log.severe(this + " failed to update demand stats: " + e);
		}
		return true;
	}

	private JSONObject getDemandStatsJSON(String name) {
		try {
			byte[] val = this.db.get(cf(CF_DEMAND), bytes(name));
			return val == null ? null : new JSONObject(new String(val, CHARSET));
		} catch (RocksDBException | JSONException e) {
			log.severe(this + " failed to read demand stats for " + name + ": "
					+ e);
			return null;
		}
	}

	@Override
	public String getDemandStats(String name) {
		JSONObject stats = getDemandStatsJSON(name);
		return stats != null ? stats.toString() : null;
	}

	@Override
	public synchronized boolean setState(String name, int epoch, RCStates state) {
		return this.setStateMerge(name, epoch, state, null, null);
	}

	@Override
	public synchronized boolean setStateMerge(String name, int epoch,
			RCStates state, Set<NodeIDType> newActives, Set<String> mergees) {
		ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
		if (record == null)
			return false;
		record.setStateMerge(name, epoch, state, mergees);
		assert (state.equals(RCStates.READY) || state.equals(RCStates.READY_READY)
				|| state.equals(RCStates.WAIT_DELETE));
		if (record.isReady()) {
			record.setActivesToNewActives(newActives);
			if (record.isReconfigurationReady())
				setPending(name, false);
			record.trimRCEpochs();
		}
		putReconfigurationRecord(record);
		return true;
	}

	@Override
	public synchronized boolean setStateInitReconfiguration(String name,
			int epoch, RCStates state, Set<NodeIDType> newActives) {
		ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
		if (record == null || !record.isReady()) {
			if (record != null)
				log.log(Level.WARNING,
						"{0} {1}:{2} not ready for transition to {3}:{4}:{5}",
						new Object[] { this, name, record.getEpoch(), name, epoch,
								state });
			return false;
		}
		assert (state.equals(RCStates.WAIT_ACK_STOP));
		record.setState(name, epoch, state, newActives);
		setPending(name, true);
		putReconfigurationRecord(record);
		return true;
	}

	@Override
	public synchronized boolean deleteReconfigurationRecord(String name,
			int epoch) {
		ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
		if (record != null && record.getEpoch() == epoch)
			return delete(CF_RECORDS, name) && deletePendingAndDemand(name);
		return false;
	}

	private boolean deletePendingAndDemand(String name) {
		delete(CF_PENDING, name);
		delete(CF_DEMAND, name);
		return true;
	}

	@Override
	public synchronized boolean markDeleteReconfigurationRecord(String name,
			int epoch) {
		ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
		if (record == null)
			return false;
		assert (record.getEpoch() == epoch);
		record.setState(name, epoch, RCStates.WAIT_DELETE);
		putReconfigurationRecord(record);
		delete(CF_DEMAND, name);
		return true;
	}

	@Override
	public synchronized ReconfigurationRecord<NodeIDType> createReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record) {
		if (getReconfigurationRecord(record.getName()) != null)
			return null;
		putReconfigurationRecord(record);
		return record;
	}

	@Override
	public synchronized boolean createReconfigurationRecords(
			Map<String, String> nameStates, Set<NodeIDType> newActives,
			ReconfigureUponActivesChange policy) {
		boolean insertedAll = true;
		Set<String> inserted = new HashSet<String>();
		for (String name : nameStates.keySet()) {
			if (getReconfigurationRecord(name) != null) {
				insertedAll = false;
				break;
			}
			ReconfigurationRecord<NodeIDType> record = new ReconfigurationRecord<NodeIDType>(
					name, -1, newActives, policy).setState(name, -1,
					RCStates.WAIT_ACK_STOP);
			putReconfigurationRecord(record);
			inserted.add(name);
		}
		if (!insertedAll)
			for (String name : inserted)
				deleteReconfigurationRecordDB(name, null);
		return insertedAll;
	}

	@Override
	public boolean setStateInitReconfiguration(Map<String, String> nameStates,
			int epoch, RCStates state, Set<NodeIDType> newActives) {
		// already initialized to WAIT_ACK_STOP:-1 in batch create
		return true;
	}

	@Override
	public synchronized boolean setStateMerge(Map<String, String> nameStates,
			int epoch, RCStates state, Set<NodeIDType> newActives) {
		for (String name : nameStates.keySet()) {
			ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
			if (record == null)
				continue;
			record.setState(name, epoch, state).setActivesToNewActives();
			putReconfigurationRecord(record);
		}
		return true;
	}

	@Override
	public synchronized boolean mergeState(String rcGroupName, int epoch,
			String mergee, int mergeeEpoch, String state) {
		synchronized (this.stringLocker.get(rcGroupName)) {
			ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(rcGroupName);
			assert (record.getEpoch() == epoch);
			if (!record.hasBeenMerged(mergee))
				if (updateState(rcGroupName, state, mergee)) {
					record.insertMerged(mergee);
					putReconfigurationRecord(record);
					setMergeeStateToWaitDelete(mergee, mergeeEpoch);
				}
			if (record.isReconfigurationReady())
				setPending(rcGroupName, false);
			return record.hasBeenMerged(mergee);
		}
	}

	private boolean setMergeeStateToWaitDelete(String mergee, int mergeEpoch) {
		ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(mergee,
				mergeEpoch);
		if (record == null)
			return false;
		setStateInitReconfiguration(mergee, mergeEpoch, RCStates.WAIT_ACK_STOP,
				new HashSet<NodeIDType>());
		setState(mergee, mergeEpoch + 1, RCStates.WAIT_DELETE);
		return true;
	}

	@Override
	public synchronized void clearMerged(String rcGroupName, int epoch) {
		ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(
				rcGroupName, epoch);
		if (record == null)
			return;
		record.clearMerged();
		putReconfigurationRecord(record);
	}

	@Override
	public void setRCEpochs(ReconfigurationRecord<NodeIDType> ncRecord) {
		if (!ncRecord.getName().equals(
				AbstractReconfiguratorDB.RecordNames.RC_NODES.toString()))
			return;
		putReconfigurationRecord(ncRecord);
	}

	@Override
	public synchronized boolean mergeIntent(String name, int epoch, String mergee) {
		ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
		boolean added = record.addToMerge(mergee);
		putReconfigurationRecord(record);
		return added;
	}

	@Override
	public void delayedDeleteComplete() {
		try {
			for (String name : getPendingReconfigurations()) {
				ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(name);
				if (record != null && record.getState().equals(RCStates.WAIT_DELETE)
						&& (System.currentTimeMillis() - record.getDeleteTime() > ReconfigurationConfig
								.getMaxFinalStateAge()))
					deleteReconfigurationRecord(name, record.getEpoch());
			}
		} catch (Exception e) {
			log.severe(this + " exception in delayedDeleteComplete: " + e);
		}
	}

	@Override
	public void garbageCollectedDeletedNode(NodeIDType node) {
		// no file-based checkpoints to clean up in the inline-checkpoint design
	}

	/* ******************* pending ******************* */

	private boolean setPending(String name, boolean set) {
		try {
			if (set)
				this.db.put(cf(CF_PENDING), this.writeOptions, bytes(name),
						new byte[0]);
			else
				this.db.delete(cf(CF_PENDING), bytes(name));
		} catch (RocksDBException e) {
			log.severe(this + " failed to set pending " + name + "=" + set + ": "
					+ e);
		}
		return true;
	}

	@Override
	public String[] getPendingReconfigurations() {
		Set<String> pending = new HashSet<String>();
		if (this.closed)
			return new String[0];
		try (RocksIterator it = this.db.newIterator(cf(CF_PENDING))) {
			for (it.seekToFirst(); it.isValid(); it.next())
				pending.add(new String(it.key(), CHARSET));
		}
		return pending.toArray(new String[0]);
	}

	@Override
	public void removePending(String name) {
		delete(CF_PENDING, name);
	}

	/* ******************* RC groups ******************* */

	@Override
	public Map<String, Set<NodeIDType>> getRCGroups() {
		Map<String, Set<NodeIDType>> rcGroups = new HashMap<String, Set<NodeIDType>>();
		for (String rcGroupName : getRCGroupNames()) {
			ReconfigurationRecord<NodeIDType> record = getReconfigurationRecord(rcGroupName);
			if (record != null)
				rcGroups.put(rcGroupName, record.getActiveReplicas());
		}
		return rcGroups;
	}

	@Override
	public Set<String> getRCGroupNames() {
		// RC group names are exactly the service names that appear as some
		// record's rcGroupName; equivalently, the names that are RC group names.
		Set<String> groups = new HashSet<String>();
		if (this.closed)
			return groups;
		try (RocksIterator it = this.db.newIterator(cf(CF_RECORDS))) {
			for (it.seekToFirst(); it.isValid(); it.next()) {
				String name = new String(it.key(), CHARSET);
				if (isRCGroupName(name))
					groups.add(name);
			}
		}
		groups.remove(AbstractReconfiguratorDB.RecordNames.RC_NODES.toString());
		return groups;
	}

	/* ******************* node config ******************* */

	@Override
	public boolean addActiveReplica(NodeIDType node, InetSocketAddress sockAddr,
			int version) {
		return addToNodeConfig(node, sockAddr, version, false);
	}

	@Override
	public boolean addReconfigurator(NodeIDType node, InetSocketAddress sockAddr,
			int version) {
		return addToNodeConfig(node, sockAddr, version, true);
	}

	// nodeConfig key: nodeID | 0x00 | version(string); value: {a:addr, p:port,
	// rc:isReconfigurator, n:nodeID, v:version}
	private boolean addToNodeConfig(NodeIDType node, InetSocketAddress sockAddr,
			int version, boolean isReconfigurator) {
		try {
			JSONObject json = new JSONObject();
			json.put("a", sockAddr.toString());
			json.put("p", sockAddr.getPort());
			json.put("rc", isReconfigurator);
			json.put("n", node.toString());
			json.put("v", version);
			this.db.put(cf(CF_NODECONFIG), this.writeOptions,
					nodeConfigKey(node.toString(), version),
					bytes(json.toString()));
			return true;
		} catch (RocksDBException | JSONException e) {
			log.severe(this + " failed to add node config for " + node + ": " + e);
			return false;
		}
	}

	private static byte[] nodeConfigKey(String nodeID, int version) {
		return bytes(nodeID + " " + version);
	}

	@Override
	public boolean garbageCollectOldReconfigurators(int version) {
		if (this.closed)
			return false;
		try (RocksIterator it = this.db.newIterator(cf(CF_NODECONFIG))) {
			for (it.seekToFirst(); it.isValid(); it.next())
				try {
					JSONObject json = new JSONObject(new String(it.value(), CHARSET));
					if (json.getInt("v") == version)
						this.db.delete(cf(CF_NODECONFIG), it.key());
				} catch (JSONException | RocksDBException e) {
					log.warning(this + " GC node config parse/delete error: " + e);
				}
		}
		return true;
	}

	private Integer getMaxNodeConfigVersion(boolean reconfigurators) {
		Integer max = null;
		try (RocksIterator it = this.db.newIterator(cf(CF_NODECONFIG))) {
			for (it.seekToFirst(); it.isValid(); it.next())
				try {
					JSONObject json = new JSONObject(new String(it.value(), CHARSET));
					if (json.getBoolean("rc") == reconfigurators) {
						int v = json.getInt("v");
						if (max == null || v > max)
							max = v;
					}
				} catch (JSONException e) {
					log.warning(this + " node config parse error: " + e);
				}
		}
		return max;
	}

	private Map<NodeIDType, InetSocketAddress> getRCNodeConfig(boolean maxOnly,
			boolean reconfigurators) {
		Integer version = getMaxNodeConfigVersion(reconfigurators);
		if (version == null)
			return null;
		int threshold = maxOnly ? version : version - 1;
		Map<NodeIDType, InetSocketAddress> map = new HashMap<NodeIDType, InetSocketAddress>();
		try (RocksIterator it = this.db.newIterator(cf(CF_NODECONFIG))) {
			for (it.seekToFirst(); it.isValid(); it.next())
				try {
					JSONObject json = new JSONObject(new String(it.value(), CHARSET));
					if (json.getBoolean("rc") != reconfigurators)
						continue;
					int v = json.getInt("v");
					if (maxOnly ? v == threshold : v >= threshold) {
						NodeIDType node = this.consistentNodeConfig.valueOf(json
								.getString("n"));
						map.put(node, Util.getInetSocketAddressFromString(json
								.getString("a")));
					}
				} catch (JSONException e) {
					log.warning(this + " node config parse error: " + e);
				}
		}
		return map;
	}

	private boolean copySoftNodeConfigToDB(boolean isReconfigurators) {
		boolean added = true;
		if (isReconfigurators)
			for (NodeIDType rc : this.consistentNodeConfig.getReconfigurators())
				added = added
						&& addReconfigurator(rc, this.consistentNodeConfig
								.getNodeSocketAddress(rc), 0);
		else
			for (NodeIDType ar : this.consistentNodeConfig.getActiveReplicas())
				added = added
						&& addActiveReplica(ar, this.consistentNodeConfig
								.getNodeSocketAddress(ar), 0);
		return added;
	}

	private void initAdjustSoftNodeConfig() {
		Map<NodeIDType, InetSocketAddress> rcMapAll = getRCNodeConfig(false, true);
		if ((rcMapAll == null || rcMapAll.isEmpty()) && copySoftNodeConfigToDB(true))
			return;
		ReconfigurationRecord<NodeIDType> ncRecord = getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.RC_NODES
				.toString());
		if (ncRecord == null || rcMapAll == null || rcMapAll.isEmpty())
			return;
		Set<NodeIDType> latestRCs = ncRecord.getNewActives();
		for (NodeIDType node : rcMapAll.keySet())
			this.consistentNodeConfig.addReconfigurator(node, rcMapAll.get(node));
		for (NodeIDType node : this.consistentNodeConfig.getReconfigurators())
			if (!rcMapAll.containsKey(node))
				this.consistentNodeConfig.removeReconfigurator(node);
			else if (!latestRCs.contains(node))
				this.consistentNodeConfig.slateForRemovalReconfigurator(node);
	}

	private void initAdjustSoftActiveNodeConfig() {
		Map<NodeIDType, InetSocketAddress> arMapAll = getRCNodeConfig(false, false);
		if ((arMapAll == null || arMapAll.isEmpty()) && copySoftNodeConfigToDB(false))
			return;
		ReconfigurationRecord<NodeIDType> ncRecord = getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.AR_NODES
				.toString());
		if (ncRecord == null || arMapAll == null || arMapAll.isEmpty())
			return;
		Set<NodeIDType> latestARs = ncRecord.getNewActives();
		for (NodeIDType node : arMapAll.keySet())
			this.consistentNodeConfig.addActiveReplica(node, arMapAll.get(node));
		for (NodeIDType node : this.consistentNodeConfig.getActiveReplicas())
			if (!arMapAll.containsKey(node))
				this.consistentNodeConfig.removeActiveReplica(node);
			else if (!latestARs.contains(node))
				this.consistentNodeConfig.slateForRemovalActive(node);
	}

	/* ******************* checkpoint / restore (inline) ******************* */

	@Override
	public String checkpoint(String rcGroup) {
		synchronized (this.stringLocker.get(rcGroup)) {
			StringBuilder sb = new StringBuilder();
			try (RocksIterator it = this.db.newIterator(cf(CF_RECORDS))) {
				for (it.seekToFirst(); it.isValid(); it.next())
					try {
						JSONObject wrapper = new JSONObject(new String(it.value(),
								CHARSET));
						String g = wrapper.isNull(W_GROUP) ? null : wrapper
								.getString(W_GROUP);
						if (rcGroup.equals(g))
							sb.append(wrapper.getString(W_RECORD)).append("\n");
					} catch (JSONException e) {
						log.warning(this + " checkpoint parse error: " + e);
					}
			}
			return sb.toString();
		}
	}

	@Override
	public boolean restore(String rcGroup, String state) {
		return updateState(rcGroup, state, null);
	}

	private boolean updateState(String rcGroup, String state, String mergee) {
		synchronized (this.stringLocker.get(rcGroup)) {
			if (state == null)
				return true;
			for (String line : state.split("\n")) {
				if (line.trim().isEmpty())
					continue;
				try {
					putReconfigurationRecordIfNotName(
							new ReconfigurationRecord<NodeIDType>(new JSONObject(
									line), this.consistentNodeConfig), rcGroup,
							mergee);
				} catch (JSONException e) {
					log.severe(this + " unable to restore record line: " + e);
				}
			}
			return true;
		}
	}

	private synchronized boolean putReconfigurationRecordIfNotName(
			ReconfigurationRecord<NodeIDType> record, String rcGroupName,
			String mergee) {
		if (isRCGroupName(record.getName())
				&& !record.getName().equals(rcGroupName))
			return false;
		else if (record.getName().equals(mergee))
			return false;
		putReconfigurationRecord(record, rcGroupName);
		if (!record.isReady())
			setPending(record.getName(), true);
		return true;
	}

	@Override
	public String fetchAndAggregateMergeeStates(Map<String, String> finalStates,
			String mergerGroup, int mergerGroupEpoch) {
		// in the inline-checkpoint design each mergee final state is itself the
		// newline-separated record string, so aggregation is concatenation.
		StringBuilder sb = new StringBuilder();
		for (String mergee : finalStates.keySet()) {
			String s = finalStates.get(mergee);
			if (s != null && !s.isEmpty()) {
				sb.append(s);
				if (!s.endsWith("\n"))
					sb.append("\n");
			}
		}
		return sb.toString();
	}

	/* ******************* active-record cursor ******************* */

	@Override
	public synchronized boolean initiateReadActiveRecords(NodeIDType active) {
		if (this.activeCursor != null || this.closed)
			return false;
		this.cursorActive = active;
		this.activeCursor = this.db.newIterator(cf(CF_RECORDS));
		this.activeCursor.seekToFirst();
		return true;
	}

	@Override
	public synchronized ReconfigurationRecord<NodeIDType> readNextActiveRecord(
			boolean add) {
		if (this.activeCursor == null)
			return null;
		while (this.activeCursor.isValid()) {
			ReconfigurationRecord<NodeIDType> record = null;
			try {
				JSONObject wrapper = new JSONObject(new String(this.activeCursor
						.value(), CHARSET));
				record = new ReconfigurationRecord<NodeIDType>(new JSONObject(
						wrapper.getString(W_RECORD)), this.consistentNodeConfig);
			} catch (JSONException e) {
				log.warning(this + " readNextActiveRecord parse error: " + e);
			}
			this.activeCursor.next();
			if (record == null)
				continue;
			String name = record.getName();
			if (name.equals(AbstractReconfiguratorDB.RecordNames.AR_NODES.toString())
					|| name.equals(AbstractReconfiguratorDB.RecordNames.RC_NODES
							.toString()))
				continue;
			if (record.getActiveReplicas() != null
					&& !isRCGroupName(name)
					&& this.consistentNodeConfig.getActiveReplicas().containsAll(
							record.getActiveReplicas())
					&& (add || ((record.getActiveReplicas().contains(
							this.cursorActive) || record
							.getReconfigureUponActivesChangePolicy() == ReconfigureUponActivesChange.REPLICATE_ALL) && record
							.isReconfigurationReady())))
				return record;
		}
		return null;
	}

	@Override
	public synchronized boolean closeReadActiveRecords() {
		if (this.activeCursor != null)
			this.activeCursor.close();
		this.activeCursor = null;
		return true;
	}

	/* ******************* lifecycle ******************* */

	@Override
	public void close() {
		if (this.closed)
			return;
		this.closed = true;
		closeReadActiveRecords();
		for (ColumnFamilyHandle h : this.cfs.values())
			h.close();
		if (this.db != null)
			this.db.close();
		if (this.dbOptions != null)
			this.dbOptions.close();
		this.writeOptions.close();
		for (ColumnFamilyOptions o : this.cfOpts)
			o.close();
		log.log(Level.INFO, "{0} closed RocksDB reconfigurator DB", this);
	}
}
