/* Copyright (c) 2015 University of Massachusetts
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * Initial developer(s): V. Arun */
package edu.umass.cs.gigapaxos;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxospackets.*;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxosutil.*;
import edu.umass.cs.gigapaxos.testing.TESTPaxosApp;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.*;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.*;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.utils.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @param <NodeIDType> <p>
 *                     PaxosManager is the primary interface to create and use paxos by
 *                     creating a paxos instance.
 *                     <p>
 *                     PaxosManager manages all paxos instances at a node. There is
 *                     typically one paxos manager per machine. This class could be
 *                     static, but it is not so that we can test emulations involving
 *                     multiple "machines" within a JVM.
 *                     <p>
 *                     PaxosManager has four functions at a machine that are useful
 *                     across paxos instances of all applications on the machine: (1)
 *                     logging, (2) failure detection, (3) messaging, and (4) paxos
 *                     instance mapping. The fourth is key to allowing the manager to
 *                     demultiplex incoming messages to the appropriate application paxos
 *                     instance.
 * @author V. Arun
 */
public class PaxosManager<NodeIDType> {

    // final
    private final AbstractPaxosLogger paxosLogger; // logging
    private final FailureDetection<NodeIDType> FD; // failure detection
    private final PaxosMessenger<NodeIDType> messenger; // messaging
    private final int myID;
    private final Replicable myApp; // default app for all paxosIDs

    // background deactivation/cremation tasks, all else event-driven
    private final ScheduledExecutorService executor;
    // paxos instance mapping
    private final MultiArrayMap<String, PaxosInstanceStateMachine> pinstances;
    // stopped paxos instances about to be incinerated
    private final HashMap<String, PaxosInstanceStateMachine> corpses;
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    private final Stringifiable<NodeIDType> unstringer;
    private final RequestBatcher requestBatcher;
    private final PaxosPacketBatcher ppBatcher;

    private int outOfOrderLimit = PaxosInstanceStateMachine.SYNC_THRESHOLD;
    private int interCheckpointInterval = PaxosInstanceStateMachine.INTER_CHECKPOINT_INTERVAL;
    private int checkpointTransferTrigger = PaxosInstanceStateMachine.MAX_SYNC_DECISIONS_GAP;
    private long minResyncDelay = PaxosInstanceStateMachine.MIN_RESYNC_DELAY;
    private final boolean nullCheckpointsEnabled;
    private final Outstanding outstanding = new Outstanding();
    private final LargeCheckpointer largeCheckpointer;
    private PendingDigests pendingDigests;

    /**
     * aditya:
     * The thread pool is for executing app.execute()
     * in the single node case. Otherwise,
     * in the single node case the app.execute()
     * method would get called from AbstractPaxosLogger thread.
     */
    private final ScheduledExecutorService appExecuteThreadPool;

    private static final boolean USE_GC_MAP = Config
            .getGlobalBoolean(PC.USE_GC_MAP);

    /**
     *
     */
    public static class RequestAndCallback {
        /**
         *
         */
        protected RequestPacket requestPacket;
        /**
         *
         */
        final ExecutedCallback callback;

        RequestAndCallback(RequestPacket request, ExecutedCallback callback) {
            this.requestPacket = request;
            this.callback = callback;
        }

        /**
         * @return RequestPacket
         */
        public RequestPacket getRequestPacket() {
            return this.requestPacket;
        }

        protected AcceptPacket setAcceptPacket(AcceptPacket accept) {
            return (AcceptPacket) (this.requestPacket = accept);
        }
    }

    private class Outstanding {
		int totalRequestSize = 0;
		long lastIncremented = System.currentTimeMillis();
		ConcurrentHashMap<Long, RequestAndCallback> requests = USE_GC_MAP ? new GCConcurrentHashMap<Long, RequestAndCallback>(
				new GCConcurrentHashMapCallback() {
					@Override
					public void callbackGC(Object key, Object value) {
						PaxosManager.this.callbackRequestTimeout(((RequestAndCallback) value).requestPacket);
					}
				}, REQUEST_TIMEOUT)
				: new ConcurrentHashMap<Long, RequestAndCallback>();
		HashMap<RequestPacket, RequestAndCallback> conflictIDRequests = new HashMap<RequestPacket, RequestAndCallback>();
		ConcurrentHashMap<RequestPacket, RequestResponseAndCallback> responses = USE_GC_MAP ? new GCConcurrentHashMap<RequestPacket, RequestResponseAndCallback>(
				new GCConcurrentHashMapCallback() {
					@Override
					public void callbackGC(Object key, Object value) {
					}
				}, REQUEST_TIMEOUT)
				: new ConcurrentHashMap<RequestPacket, RequestResponseAndCallback>();

		private void enqueue(RequestAndCallback rc) {
			assert (rc.requestPacket.getType() != PaxosPacketType.ACCEPT || rc.requestPacket
					.hasRequestValue());
			RequestAndCallback prev = null;
			synchronized (this.requests) {
				if ((prev = this.requests.putIfAbsent(rc.requestPacket.requestID, rc)) == null)
					totalRequestSize += rc.requestPacket.lengthEstimate();
				else if (!rc.requestPacket.equals(prev.requestPacket)) {
					// just skip accept packets
					if (!(rc.requestPacket instanceof AcceptPacket))
						// insert in overflow
						this.conflictIDRequests.put(rc.requestPacket, rc);
				} 
				// replace callback if equal request
				else if(rc.callback!=null) this.requests.put(rc.requestPacket.requestID, rc);
			}
			this.lastIncremented = System.currentTimeMillis();
		}

		// called by executed callback
		private RequestAndCallback dequeue(RequestPacket request) {
			RequestAndCallback queued = this.requests.get(request.requestID);
			if (queued != null && queued.requestPacket.equals(request))
				return this.requests.remove(request.requestID);
			else
				return this.conflictIDRequests.remove(request);
		}

		private long generateUnusedID() {
			Long requestID = null;
			do {
				requestID = (long) (Math.random() * Long.MAX_VALUE);
			} while (this.requests.containsKey(requestID));
			return requestID;
		}
	}

	static final long REQUEST_TIMEOUT = Config
			.getGlobalLong(PC.REQUEST_TIMEOUT) * 1000;
	private static final long FADE_OUTSTANDING_TIMEOUT = REQUEST_TIMEOUT;

	private void GC() {
		if (this.outstanding.requests instanceof GCConcurrentHashMap)
			((GCConcurrentHashMap<Long, RequestAndCallback>) this.outstanding.requests)
					.tryGC(REQUEST_TIMEOUT);
		else if (System.currentTimeMillis() - this.outstanding.lastIncremented > PaxosManager.FADE_OUTSTANDING_TIMEOUT) {
			if (this.outstanding.requests.size() > MAX_OUTSTANDING_REQUESTS)
				PaxosConfig.log.severe(this
						+ " clearing clogged outstanding queue");
			this.outstanding.requests.clear();
		}
	}

	static class RequestResponseAndCallback extends RequestAndCallback {
		/* this is actually the request whose getResponse contains the response. */
		final Request clientRequest;

		RequestResponseAndCallback(RequestPacket request, Request response,
				ExecutedCallback callback) {
			super(request, callback);
			this.clientRequest = response;
		}

	}

	private static final boolean NO_RESPONSE = Config
			.getGlobalBoolean(PC.NO_RESPONSE);

	// default callback tries to send back response
	private void defaultCallback(Request response, InetSocketAddress clientAddress, InetSocketAddress listenAddress) {
		if (response == null || !(response instanceof ClientRequest))
			return;
		// waiting for others to remove this method
		if (clientAddress != null && response != null && !NO_RESPONSE) {
			try {
				this.send(clientAddress, response,
						listenAddress);
			} catch (JSONException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	// default callback tries to send back response to the sending client
	private void defaultCallback(RequestPacket requestPacket, Request request) {
		if (request == null || !(request instanceof ClientRequest))
			return;
		this.defaultCallback(((ClientRequest) request).getResponse(),
				/* We might as well use requestPacket's clientAddress here
				 * because in the case when requestPacket directly came
				 * over the wire, it will have the right client address
				 * and in the case it was constructed from request, 
				 * either a callback was provided in which case the
				 * default callback is irrelevant or we already copied
				 * over the client address while constructing requestPacket
				 * from request.
				 */
				(requestPacket).getClientAddress(),
				requestPacket.getListenAddress());
	}

	// called by PaxosInstanceStateMachine as execute callback
	protected boolean executed(RequestPacket requestPacket, Request request,
			boolean sendResponse) {
		RequestAndCallback rc = this.outstanding.dequeue(requestPacket);
		if (rc != null)
			this.outstanding.totalRequestSize -= rc.requestPacket.lengthEstimate();
		RequestInstrumenter.remove(requestPacket.requestID);
		// cache response
		if(ENABLE_RESPONSE_CACHING && request instanceof ClientRequest)
			this.outstanding.responses.put(requestPacket,
					new RequestResponseAndCallback(requestPacket,
							((ClientRequest) request),
							rc != null ? rc.callback : null));
		// only called if executed
		if (rc != null && rc.callback != null)
			rc.callback.executed(request, true);
		else if (sendResponse)
			this.defaultCallback(requestPacket, request);
		assert (requestPacket.batchSize() == 0);
		return rc != null;
	}

	protected boolean retransmittedRequest(RequestPacket requestPacket) {
		RequestResponseAndCallback rrc = null;
		if (ENABLE_RESPONSE_CACHING
				&& (rrc = this.outstanding.responses.get((requestPacket))) != null) {
			RequestAndCallback rc = this.outstanding.dequeue(requestPacket);
			if (rc!=null && rc.callback != null)
				rc.callback.executed(rrc.clientRequest, false);
			else if (rrc.callback != null)
				rrc.callback.executed(rrc.clientRequest, false);
			else
				this.defaultCallback(rrc.clientRequest,
						requestPacket.getClientAddress(), requestPacket.getListenAddress());
			assert (requestPacket.batchSize() == 0);
		}
		return rrc != null;
	}

	// non-final
	private boolean hasRecovered = false;

	// need this to be static so DB can be closed gracefully
	private static boolean closed = false;

	// need this to be static so DB can be closed gracefully
	private static int processing = 0;

	/* Note: PaxosManager itself maintains no NIO transport instance as it
	 * delegates all communication related activities to other objects.
	 * PaxosManager is only responsible for managing state for and
	 * demultiplexing incoming packets to a number of paxos instances at this
	 * node. */

	/**
	 * @param id
	 *            My node ID.
	 * @param unstringer
	 *            An instance of Stringifiable that can convert String to
	 *            NodeIDType.
	 * @param niot
	 *            InterfaceNIOTransport or InterfaceMessenger object used for
	 *            messaging.
	 * @param pi
	 *            InterfaceReplicable application controlled by gigapaxos.
	 *            Currently, all paxos instances must correspond to a single
	 *            umbrella application even though each createPaxosInstance
	 *            method explicitly specifies the app and this information is
	 *            stored explicitly inside a paxos instance. The reason for the
	 *            single umbrella app restriction is that we won't have a
	 *            pointer to the appropriate app upon recovery otherwise.
	 * @param paxosLogFolder
	 *            Paxos logging folder.
	 * @param enableNullCheckpoints
	 *            Whether null checkpoints are enabled. We need this flag to be
	 *            enabled if we intend to reconfigure paxos groups managed by
	 *            this PaxosManager. Otherwise, we can not distinguish between a
	 *            null checkpoint and no checkpoint, so the next epoch members
	 *            may be waiting forever for the previous epoch's final state
	 *            (that happens to be null). This flag needs to be set at
	 *            construction time and can not be changed thereafter.
	 */
	public PaxosManager(NodeIDType id, Stringifiable<NodeIDType> unstringer,
			InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable pi,
			String paxosLogFolder, boolean enableNullCheckpoints) {		
		this.myID = this.integerMap.put(id);// id.hashCode();
		this.executor = Executors.newScheduledThreadPool(1,
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = Executors.defaultThreadFactory()
								.newThread(r);
						thread.setName(PaxosManager.class.getSimpleName()
								+ myID);
						return thread;
					}
				});
		
		// TODO: aditya: Need to check if we can just increase the
		// number of threads in this.executor thread pool, instead
		// of creating a separate appExecuteThreadPool.
		appExecuteThreadPool = Executors.newScheduledThreadPool
				(Config.getGlobalInt(PC.PACKET_DEMULTIPLEXER_THREADS),
						new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = Executors.defaultThreadFactory()
								.newThread(r);
						thread.setName(PaxosManager.class.getSimpleName()
								+ myID+"appExecuteThreadPool"+thread.getId());
						return thread;
						}
					});
		
		
		this.unstringer = unstringer;
		this.largeCheckpointer = new LargeCheckpointer(paxosLogFolder, 
				id.toString());
		this.myApp = LargeCheckpointer.wrap(pi, largeCheckpointer);
		this.FD = new FailureDetection<NodeIDType>(id, niot, paxosLogFolder);
		this.pinstances = new MultiArrayMap<String, PaxosInstanceStateMachine>(
				Config.getGlobalInt(PC.PINSTANCES_CAPACITY));
		this.corpses = new HashMap<String, PaxosInstanceStateMachine>();
		// this.activePaxii = new HashMap<String, ActivePaxosState>();
		this.messenger = (new PaxosMessenger<NodeIDType>(niot, this.integerMap));
		this.paxosLogger = new SQLPaxosLogger(this.myID, id.toString(),
				paxosLogFolder, this.wrapMessenger(this.messenger));
		this.nullCheckpointsEnabled = enableNullCheckpoints;
		// periodically remove active state for idle paxii
		executor.scheduleWithFixedDelay(new Deactivator(), 0,
				Config.getGlobalInt(PC.DEACTIVATION_PERIOD),
				TimeUnit.MILLISECONDS);
		this.pendingDigests = new PendingDigests(this.outstanding.requests,
				Config.getGlobalInt(PC.NUM_MESSAGE_DIGESTS),
				new PendingDigests.PendingDigestCallback() {
					public void callback(AcceptPacket accept) {
						PaxosManager.this.callbackDigestedAcceptTimeout(accept);
					}
				});
		this.initOutstandingMonitor();
		(this.requestBatcher = new RequestBatcher(this)).start();
		(this.ppBatcher = new PaxosPacketBatcher(this)).start();
		testingInitialization();
		// needed to unclose when testing multiple runs of open and close
		open();
		// so paxos packets will come to me before anyone else
		niot.precedePacketDemultiplexer(Config.getGlobalString(PC.JSON_LIBRARY)
				.equals("org.json") ? new JSONDemultiplexer()
				: new FastDemultiplexer());
		initiateRecovery(id);
		if (!Config.getGlobalBoolean(PC.DELAY_PROFILER))
			DelayProfiler.disable();
	}

	private void initOutstandingMonitor() {
		final long monitorIterval = Config.getGlobalLong(PC.DEBUG_MONITOR);
		this.executor.scheduleWithFixedDelay(
				new Runnable() {
					@Override
					public void run() {
						try {
							if ((PaxosManager.this.outstanding.requests.size() > 0 || PaxosManager.this.pendingDigests
									.size() > 0)
									&& (System.currentTimeMillis()
											- PaxosManager.this.outstanding.lastIncremented > REQUEST_TIMEOUT)
									|| monitorIterval > 0) {
								HashMap<Long, String> instances = new HashMap<Long, String>();
								for (RequestAndCallback rc : PaxosManager.this.outstanding.requests
										.values())
									instances.put(rc.requestPacket.requestID,
											rc.requestPacket.getPaxosID() + ":"
													+ rc.requestPacket.getSummary());
								PaxosConfig.log.log(Level.INFO,
										"{0} |outstanding|={1}; {2}; |unpaused|={3}; \n|pending|={4}; {5}",
										new Object[] {
												PaxosManager.this,
												PaxosManager.this.outstanding.requests
														.size(),
												Util.truncatedLog(
														instances.entrySet(),
														10),
												PaxosManager.this.pinstances
														.size(),
												PaxosManager.this.pendingDigests,
												DelayProfiler.getStats() });
								if (!PaxosManager.this.outstanding.requests
										.isEmpty()
										&& PaxosManager.this.outstanding.requests instanceof GCConcurrentHashMap)
									((GCConcurrentHashMap<Long, RequestAndCallback>) PaxosManager.this.outstanding.requests)
											.tryGC(REQUEST_TIMEOUT);

							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}, 0, monitorIterval > 0 ? monitorIterval : REQUEST_TIMEOUT,
				TimeUnit.SECONDS);
	}

	/**
	 * Refer
	 * {@link #PaxosManager(Object, Stringifiable, InterfaceNIOTransport, Replicable, String, boolean)}
	 * .
	 * 
	 * @param id
	 * @param nc
	 * @param niot
	 * @param app
	 */
	public PaxosManager(NodeIDType id, Stringifiable<NodeIDType> nc,
			InterfaceNIOTransport<NodeIDType, JSONObject> niot, Replicable app) {
		this(id, nc, niot, (app), null,
				PaxosInstanceStateMachine.ENABLE_NULL_CHECKPOINT_STATE);
	}

	/* We need to be careful with pause/unpause and createPaxosInstance as there
	 * are potential cyclic dependencies. The call chain is as below, where
	 * "info" is the information needed to create a paxos instance. The
	 * notation"->?" means the call may or may not happen, which is why the
	 * recursion breaks after at most one step.
	 * 
	 * On recovery: initiateRecovery() -> recover(info) ->
	 * createPaxosInstance(info) -> getInstance(paxosID, tryHotRestore) ->?
	 * unpause(paxosID) ->? createPaxosInstance(info, hri). An inifinite
	 * recursion is prevented because createPaxosInstance will not again call
	 * getInstance with the tryHotRestore option if hri!=null.
	 * 
	 * Upon createPaxosInstance any time after recovery, the same chain as above
	 * is followed.
	 * 
	 * On deactivation: deactivate(paxosID) ->? pause(paxosID) // may or may not
	 * be successful
	 * 
	 * On incoming packet: handleIncomingPacket() -> getInstance(paxosID) ->?
	 * unpause(paxosID) ->? createPaxosInstance(info) */

	/**
	 * Returns members of this paxos instance if it is active, and null
	 * otherwise. Note that this means that we return null even if the paxos
	 * instance is being created (but has not yet completed recovery). A
	 * non-null answer to this method necessarily means that the paxos instance
	 * was ready to take more requests at the time of the query.
	 * 
	 * @param paxosID
	 *            The name of the object or service being managed as a
	 *            replicated state machine.
	 * @param version
	 *            The reconfiguration version.
	 * @return Set of members in the paxos instance paxosID:version.
	 */
	public Set<NodeIDType> getReplicaGroup(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version && pism.isActive())
			return this.integerMap.getIntArrayAsNodeSet(pism.getMembers());
		return null;
	}

	/**
	 * @param paxosID
	 * @return Set of members in the paxos instance named paxosID. There can
	 *         only be one version of a paxos instance at a node, so we don't
	 *         really have to specify a version unless we want to explicitly get
	 *         the members for a specific version, in which case,
	 *         {@link #getReplicaGroup(String, int)} should be used.
	 */
	public Set<NodeIDType> getReplicaGroup(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.isActive())
			return this.integerMap.getIntArrayAsNodeSet(pism.getMembers());
		return null;
	}

	// is current and not stopped (and may not yet be active)
	protected boolean isCurrent(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version)
			return true;
		return false;
	}

	private void synchronizedNoop(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version)
			pism.synchronizedNoop();
	}

	/**
	 * A method that specifies the minimum number of necessary arguments to
	 * create a paxos instance. The version is assumed to be 0 here. The use of
	 * this method is encouraged only if reconfiguration is not desired. The
	 * initialState argument can be {@code null} if
	 * {@link #isNullCheckpointStateEnabled()} is true.
	 * 
	 * @param paxosID
	 * @param gms
	 * @param initialState
	 * @return Returns true if the paxos instance paxosID:version or one with a
	 *         higher version number was successfully created.
	 */

	public boolean createPaxosInstance(String paxosID, Set<NodeIDType> gms,
			String initialState) {
		return this.createPaxosInstance(paxosID, 0, gms, myApp, initialState,
				null, true) != null;
	}

	/**
	 * Paxos instance creation with an initial state specified.
	 * 
	 * @param paxosID
	 *            Paxos group name.
	 * @param version
	 *            Paxos group version (or epoch number).
	 * @param gms
	 *            Group members.
	 * @param app
	 *            Application controlled by paxos.
	 * @param initialState
	 *            Initial application state.
	 * @return Whether this paxos instance or higher got created.
	 */
	public boolean createPaxosInstance(String paxosID, int version,
			Set<NodeIDType> gms, Replicable app, String initialState) {
		return this.createPaxosInstance(paxosID, version, gms, app,
				initialState, null, true) != null;
	}

	private PaxosInstanceStateMachine createPaxosInstance(String paxosID,
			int version, Set<NodeIDType> gms, Replicable app,
			String initialState, HotRestoreInfo hri, boolean tryRestore) {
		return this.createPaxosInstance(paxosID, version, gms, app,
				initialState, hri, tryRestore, false);
	}

	private static final boolean SNEAKY_BATCH_CREATION = true;

	private void waitPinstancesSize() {
		while (this.pinstances.size() >= this.pinstances.capacity()) {
			synchronized (this.pinstances) {
				try {
					this.pinstances.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @param nameStates
	 * @param gms
	 * @return True if all successfully created.
	 */
	public boolean createPaxosInstance(Map<String, String> nameStates,
			Set<NodeIDType> gms) {
		waitPinstancesSize();
		synchronized (this) {
			int[] members = Util.setToIntArray(this.integerMap.put(gms));
			PaxosConfig.log.log(Level.INFO,
					"{0} batch-inserting initial checkpoints for {1} names: {2}",
					new Object[] { this, nameStates.size(),
							Util.truncatedLog(nameStates.entrySet(), 8) });
			if (SNEAKY_BATCH_CREATION)
				this.paxosLogger.insertInitialCheckpoints(nameStates,
						Util.setToStringSet(gms), members);
			boolean created = true;
			for (String name : nameStates.keySet()) {
				assert (nameStates.get(name) != null);
				created = created
						&& (SNEAKY_BATCH_CREATION ? this.createPaxosInstance(
								name, 0, gms, this.myApp, nameStates.get(name),
								HotRestoreInfo.createHRI(name, members,
										PaxosInstanceStateMachine
												.roundRobinCoordinator(name,
														members, 0)), false)
								: this.createPaxosInstance(name, gms,
										nameStates.get(name))) != null;
			}
			return created;
		}
	}

	private PaxosInstanceStateMachine createPaxosInstance(String paxosID,
			int version, Set<NodeIDType> gms, Replicable app,
			String initialState, HotRestoreInfo hri, boolean tryRestore,
			boolean missedBirthing) {
		this.waitPinstancesSize();
		return this.createPaxosInstanceFinal(paxosID, version, gms, app,
				initialState, hri, tryRestore, missedBirthing);
	}

	private long totalInstancesCreated = 0;
	private long totalInstancesKilled = 0;
	private boolean totalInstancesChanged = false;

	private synchronized long getNumInstances() {
		totalInstancesChanged = false;
		return totalInstancesCreated - totalInstancesKilled;
	}

	private synchronized long getNumInstancesAndResetChanged() {
		totalInstancesChanged = false;
		return this.getNumInstances();
	}

	private synchronized long getNumCreated() {
		return totalInstancesCreated;
	}

	private synchronized long incrCreated() {
		totalInstancesChanged = true;
		return ++totalInstancesCreated;
	}

	private synchronized long incrKilled() {
		totalInstancesChanged = true;
		return ++totalInstancesKilled;
	}

	/* Synchronized in order to prevent duplicate instance creation under
	 * concurrency. This is the only method that can actually create a paxos
	 * instance. All other methods just call this method eventually.
	 * 
	 * private because it ensures that initialState!=null and missedBirthing are
	 * not both true. */
	private synchronized PaxosInstanceStateMachine createPaxosInstanceFinal(
			String paxosID, int version, Set<NodeIDType> gms, Replicable app,
			String initialState, HotRestoreInfo hri, boolean tryRestore,
			boolean missedBirthing) {

		if (this.isClosed())
			return null;
		
		if (!gms.contains(this.getNodeID()))
			throw new PaxosInstanceCreationException(this.getNodeID()
					+ " can not create a paxos instance for group " + gms
					+ " to which it does not belong");

		boolean tryHotRestore = (hasRecovered() && hri == null);
		PaxosInstanceStateMachine pism = this.getInstance(paxosID,
				tryHotRestore, tryRestore);
		// if equal or higher version exists, return false
		if ((pism != null) && (pism.getVersion() - version >= 0)) {
			PaxosConfig.log.log(Level.FINE,
					"{0} paxos instance {1}:{2} or higher version currently exists",
					new Object[] { this, paxosID, version });
			return null;
		}

		// if lower version exists, return false
		if (pism != null && (pism.getVersion() - version < 0)) {
			PaxosConfig.log.log(Level.INFO,
					"{0} has pre-existing paxos instance {1} when asked to create version {2}",
					new Object[] { this, pism.getPaxosIDVersion(), version });
			// pism must be explicitly stopped first
			return null; // initialState will also be ignored here
		}
		// if equal or higher version stopped on disk, return false
		if (pism == null && equalOrHigherVersionStopped(paxosID, version)) {
			PaxosConfig.log.log(Level.INFO,
					"{0} paxos instance {1}:{2} can not be created as equal or higher "
							+ "version {3}:{4} was previously created and stopped",
					new Object[] { this, paxosID, version, paxosID,
							this.getVersion(paxosID) });
			return null;
		}

		try {
			// else try to create (could still run into exception)
			pism = new PaxosInstanceStateMachine(paxosID, version, myID,
					this.integerMap.put(gms), app != null ? app : this.myApp,
					initialState, this, hri, missedBirthing);
		} catch (Exception e) {
			e.printStackTrace();
			throw new PaxosInstanceCreationException(e.getMessage());
		}

		pinstances.put(paxosID, pism);
		incrCreated();
		this.notifyUponCreation();
		assert (this.getInstance(paxosID, false, false) != null);
		PaxosConfig.log.log(Level.FINE,
				"{0} successfully {1} paxos instance {2}",
				new Object[] { this, hri != null ? "unpaused" : "created",
						pism.getPaxosIDVersion() });
		/* Note: rollForward can not be done inside the instance as we first
		 * need to update the instance map here so that networking--even
		 * trivially sending message to self--works. */
		assert (hri == null || hasRecovered());
		if (hri == null) // not hot restore
			rollForward(paxosID, version);
		// to sync decisions initially if needed or if missed birthing
		this.syncPaxosInstance(pism, missedBirthing);

		// keepalives only if needed
		this.FD.sendKeepAlive(gms);
		this.integerMap.put(gms);
		this.addServers(gms);
		return pism;
	}

	private void syncPaxosInstance(PaxosInstanceStateMachine pism,
			boolean forceSync) {
		if (pism != null)
			pism.poke(forceSync);
	}

	/**
	 * When a node is being permanently deleted.
	 * 
	 * @param id
	 * @return True if {@code id} was being monitored.
	 */
	public boolean stopFailureMonitoring(NodeIDType id) {
		this.removeServer(id);
		return this.FD.dontSendKeepAlive(id);
	}

	private boolean canCreateOrExistsOrHigher(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		return (pism == null || (pism.getVersion() - version >= 0));
	}

	private Set<InetAddress> servers = new HashSet<InetAddress>();

	private boolean isServer(InetAddress isa) {
		// play safe if we can't distinguish servers from clients
		if (!(this.unstringer instanceof NodeConfig))
			return true;
		return servers.contains(isa);
	}

	protected void addServers(Set<NodeIDType> nodes) {
		if (this.unstringer instanceof NodeConfig)
			for (NodeIDType node : nodes) {
				this.servers.add(((NodeConfig<NodeIDType>) this.unstringer)
						.getNodeAddress(node));
			}
	}

	private void removeServer(NodeIDType node) {
		if (this.unstringer instanceof NodeConfig)
			servers.remove(((NodeConfig<NodeIDType>) this.unstringer)
					.getNodeAddress(node));
	}

	private static final int MAX_OUTSTANDING_REQUESTS = Config
			.getGlobalInt(PC.MAX_OUTSTANDING_REQUESTS);
	private final boolean DISABLE_CC = Config.getGlobalBoolean(PC.DISABLE_CC);
	private final boolean ORDER_PRESERVING_REQUESTS = Config
			.getGlobalBoolean(PC.ORDER_PRESERVING_REQUESTS);

	// older demultiplexer based purely on JSON
	class JSONDemultiplexer extends AbstractJSONPacketDemultiplexer {

		private final boolean clientFacing;

		public JSONDemultiplexer() {
			this(Config.getGlobalInt(PC.PACKET_DEMULTIPLEXER_THREADS), false);
		}

		public JSONDemultiplexer(int numThreads, boolean clientFacing) {
			super(numThreads);
			this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
			this.setThreadName(myID + (clientFacing ? "-clientFacing" : ""));
			this.clientFacing = clientFacing;
		}

		public boolean handleMessage(JSONObject jsonMsg, edu.umass.cs.nio.nioutils.NIOHeader header) {
			try {
				PaxosConfig.log.log(Level.FINEST,
						"{0} packet json demultiplexer received {1}",
						new Object[] { PaxosManager.this, jsonMsg });
				PaxosManager.this
						.handleIncomingPacket(edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexer
								.toPaxosPacket(fixNodeStringToInt(jsonMsg),
										PaxosManager.this.unstringer));
				return true;
			} catch (JSONException e) {
				log.severe(this
						+ " unable to parse JSON or unable fix node ID string to integer");
				e.printStackTrace();
			} catch (OverloadException oe) {
				if (this.clientFacing)
					PaxosPacketDemultiplexer.throttleExcessiveLoad();
			}
			return true;
		}

		@Override
		protected boolean isCongested(NIOHeader header) {
			if (DISABLE_CC)
				return false;
			if (PaxosManager.this.isServer(header.sndr.getAddress()))
				return false;
			// if(!clientFacing) return false;
			boolean congested = PaxosManager.this.isCongested();
			if (congested)
				log.log(Level.WARNING,
						"{0} congested; rate limiting requests from {1}; (outstanding={2} > {3})",
						new Object[] { this, header.sndr.getAddress(),
								PaxosManager.this.getNumOutstandingOrQueued(),
								MAX_OUTSTANDING_REQUESTS });
			return congested;
		}

		@Override
		public boolean isOrderPreserving(JSONObject msg) {
			if (!ORDER_PRESERVING_REQUESTS)
				return false;
			try {
				// only preserve order for REQUEST or PROPOSAL packets
				PaxosPacketType type = PaxosPacket.getPaxosPacketType(msg);
				return (type.equals(PaxosPacket.PaxosPacketType.REQUEST) || type
						.equals(PaxosPacket.PaxosPacketType.PROPOSAL));
			} catch (JSONException e) {
				log.severe(this + " incurred JSONException while parsing "
						+ msg);
				e.printStackTrace();
			}
			return false;
		}
	}

	protected boolean isCongested() {
		this.GC();
		return PaxosManager.this.getNumOutstandingOrQueued() > MAX_OUTSTANDING_REQUESTS;
	}

	// faster demultiplexer based on byte[] serialization
	class FastDemultiplexer extends
			edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexerFast {

		public FastDemultiplexer(int numThreads, boolean clientFacing) {
			super(numThreads);
			this.setThreadName(PaxosManager.this.intToString(myID)
					+ (clientFacing ? "-clientFacing" : ""));
			this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		}

		public FastDemultiplexer() {
			this(Config.getGlobalInt(PC.PACKET_DEMULTIPLEXER_THREADS), false);
		}

		public boolean handleMessage(Object msg, edu.umass.cs.nio.nioutils.NIOHeader header) {
			// long t = System.nanoTime();
			assert (msg != null);
			if (msg instanceof net.minidev.json.JSONObject)
				try {
					PaxosPacketType type = null;
					net.minidev.json.JSONObject jsonMsg = (net.minidev.json.JSONObject) msg;
					assert ((type = PaxosPacket.getPaxosPacketType(jsonMsg)) != PaxosPacketType.ACCEPT || jsonMsg
							.containsKey(RequestPacket.Keys.STRINGIFIED
									.toString()));
					long t = System.nanoTime();
					PaxosPacket pp = edu.umass.cs.gigapaxos.paxosutil.PaxosPacketDemultiplexerFast
							.toPaxosPacket(fixNodeStringToInt(jsonMsg),
									PaxosManager.this.unstringer);
					Level level = type == PaxosPacketType.REQUEST ? Level.FINE
							: Level.FINEST;
					PaxosConfig.log.log(
							level,
							"{0} packet fast demultiplexer received {1}",
							new Object[] { PaxosManager.this,
									pp.getSummary(PaxosConfig.log.isLoggable(level)) });

					if (PaxosMessenger.INSTRUMENT_SERIALIZATION
							&& Util.oneIn(100))
						if (pp.getType() == PaxosPacketType.REQUEST)
							DelayProfiler.updateDelayNano(
									"requestPacketization", t);

					PaxosManager.this.handleIncomingPacket(pp);
					return true;
				} catch (JSONException e) {
					log.severe(this + " incurred JSONException while parsing "
							+ msg);
					e.printStackTrace();
				}

			try {
				// else
				assert (msg instanceof PaxosPacket);
				PaxosPacketType type = ((PaxosPacket) msg).getType();
				Level level = (type == PaxosPacketType.REQUEST ? Level.FINE
						: Level.FINEST);
				PaxosConfig.log
						.log(level,
								"{0} packet fast-byte demultiplexer received {1} {2}",
								new Object[] {
										PaxosManager.this,
										((PaxosPacket) msg).getSummary(
										// log.isLoggable(level)
												),
										msg instanceof RequestPacket
												&& (((RequestPacket) msg)
														.getEntryReplica() == PaxosManager.this.myID || ((RequestPacket) msg)
														.getEntryReplica() == IntegerMap.NULL_INT_NODE) ? "from client"
												: "" });

				/* FIXME: Need to fixNodeStringToInt. Unclear how to do the
				 * reverse efficiently while sending out messages. So we
				 * currently byteify paxos packets only when all node IDs are
				 * integers. */
				PaxosManager.this.handleIncomingPacket(((PaxosPacket) msg));
			} catch (Exception | Error e) {
				e.printStackTrace();
			}
			return true;
		}

		@Override
		protected boolean matchesType(Object message) {
			return message instanceof net.minidev.json.JSONObject;
		}

		@Override
		protected boolean isCongested(NIOHeader header) {
			if (DISABLE_CC)
				return false;
			if (PaxosManager.this.isServer(header.sndr.getAddress()))
				return false;
			// if(!clientFacing) return false;
			return PaxosManager.this.isCongested();
		}

		// @Override
		public boolean isOrderPreservingFIXME_REMOVE(
				net.minidev.json.JSONObject msg) {
			if (!ORDER_PRESERVING_REQUESTS)
				return false;
			try {
				// only preserve order for REQUEST or PROPOSAL packets
				PaxosPacketType type = PaxosPacket.getPaxosPacketType(msg);
				return (type.equals(PaxosPacket.PaxosPacketType.REQUEST) || type
						.equals(PaxosPacket.PaxosPacketType.PROPOSAL));
			} catch (JSONException e) {
				log.severe(this + " incurred JSONException while parsing "
						+ msg);
				e.printStackTrace();
			}
			return false;
		}

	}

	private static final boolean BATCHING_ENABLED = Config
			.getGlobalBoolean(PC.BATCHING_ENABLED);

	private static final boolean ENABLE_RESPONSE_CACHING = Config
			.getGlobalBoolean(PC.ENABLE_RESPONSE_CACHING);

	private void handleIncomingPacket(PaxosPacket pp) {

		if (ENABLE_RESPONSE_CACHING && pp.getType() == PaxosPacketType.REQUEST
				&& this.retransmittedRequest(((RequestPacket) pp)))
			return;
		else if (pp.getType() == PaxosPacketType.BATCHED_PAXOS_PACKET)
			for (PaxosPacket packet : ((BatchedPaxosPacket) pp)
					.getPaxosPackets())
				this.handleIncomingPacket(packet);
		else if (BATCHING_ENABLED)
			this.enqueueRequest(pp);
		else
			this.handlePaxosPacket(pp);
	}

	/* If RequestPacket, hand over to batcher that will then call
	 * handleIncomingPacketInternal on batched requests. */
	private void enqueueRequest(PaxosPacket pp) {
		PaxosPacketType type = pp.getType();
		Level level = Level.FINEST;
		if ((type.equals(PaxosPacketType.REQUEST) || type
				.equals(PaxosPacketType.PROPOSAL))
				&& RequestBatcher.shouldEnqueue()
				&& !((RequestPacket) pp).isBroadcasted()) {
			if (pp.getPaxosID() != null) {
				PaxosConfig.log.log(level, "{0} enqueueing request {1}", new Object[] {
						this, pp.getSummary(PaxosConfig.log.isLoggable(level)) });
				this.requestBatcher.enqueue(((RequestPacket) pp));
			} else
				error((RequestPacket) pp);
		} else {
			PaxosConfig.log.log(level,
					"{0} handling paxos packet {1} directly without enqueueuing",
					new Object[] { this, pp.getSummary(PaxosConfig.log.isLoggable(level)) });
			this.handlePaxosPacket(pp);
		}
	}

	private void error(RequestPacket req) {
		PaxosConfig.log.warning(this + " received request with no paxosID: "
				+ req.getSummary());
	}

	@SuppressWarnings("unchecked")
	private void handlePaxosPacket(PaxosPacket request) {
		if (this.isClosed())
			return;
		else if (emulateUnreplicated(request)
				|| this.emulateLazyPropagation(request))
			return; // testing
		else
			setProcessing(true);

		Level level = Level.FINEST;
		PaxosPacketType paxosPacketType;
		try {
			// will throw exception if no PAXOS_PACKET_TYPE
			paxosPacketType = request.getType();
			switch (paxosPacketType) {
			case FAILURE_DETECT:
				processFailureDetection((FailureDetectionPacket<NodeIDType>) request);
				break;
			case FIND_REPLICA_GROUP:
				processFindReplicaGroup((FindReplicaGroupPacket) request);
				break;
			default: // paxos protocol messages

				assert (request.getPaxosID() != null) : request.toJSONSmart()
						.toString();
				if (request instanceof RequestPacket) // base and super types
					((RequestPacket) request).addDebugInfo("i", myID);

				PaxosInstanceStateMachine pism = this.getInstance(request
						.getPaxosID());

				level = pism!=null ? level : Level.FINE;
				PaxosConfig.log.log(level, "{0} received paxos message for {1} : {2}",
						new Object[] { this,
								pism != null ? pism : "non-existent instance",
								request.getSummary(PaxosConfig.log.isLoggable(level)) });
				if ((pism != null)
						&& (pism.getVersion() == request.getVersion()))
				{
					PaxosPacket.PaxosPacketType msgType = request != null ? request.getType()
							: PaxosPacket.PaxosPacketType.NO_TYPE;
					
					//In the single node case, on receiving an ACCEPT_REPLY message,
					// we handle the ACCEPT_REPLY and the subsequent DECISION
					// message in a separate thread pool, instead of AbstractPaxosLogger.
					if(msgType.equals(PaxosPacket.PaxosPacketType.ACCEPT_REPLY) 
							&& pism.getMembers().length == 1)
					{
						this.appExecuteThreadPool.execute(new Runnable() 
								{ 
									public void run() { try {
									pism.handlePaxosMessage((request));
										} catch (JSONException je) {
											PaxosConfig.log.log(Level.SEVERE, 
													"{0} received bad JSON message: {1}",
													new Object[]{myID, request});
											je.printStackTrace();
										}
									}
								});
					}
					else
					{
						pism.handlePaxosMessage(request);
					}
				}
				else
					// for recovering group created while crashed
					this.findPaxosInstance(request);
				break;
			}
		} catch (JSONException je) {
			PaxosConfig.log.severe(this.myID + " received bad JSON message: "
					+ request);
			je.printStackTrace();
		} finally {
			setProcessing(false);
		}
	}

	private void processFailureDetection(
			FailureDetectionPacket<NodeIDType> request) {
		if (request.getSender() != null) {
			this.servers.add(request.getSender().getAddress());
		}
		FD.receive((FailureDetectionPacket<NodeIDType>) request);
	}

	private String propose(String paxosID, RequestPacket requestPacket,
			ExecutedCallback callback) {
		if (this.isClosed())
			return null;
		boolean matched = false;
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null) {
			matched = true;
			requestPacket.putPaxosID(paxosID, pism.getVersion());

			PaxosConfig.log.log(Level.FINE, "{0} proposing to {1}: {2}",
					new Object[] { this, pism.getPaxosIDVersion(),
							requestPacket.getSummary() });
			this.outstanding.enqueue(new RequestAndCallback(requestPacket,
					callback));
			this.handleIncomingPacket(requestPacket);
		} else
			PaxosConfig.log.log(Level.INFO,
					"{0} could not find paxos instance {1} for request {2} with body {3}; "
							+ " last known version was [{4}]",
					new Object[] {
							this,
							paxosID,
							requestPacket.getSummary(),
							Util.truncate(requestPacket.getRequestValues()[0],
									64), this.getVersion(paxosID) });
		return matched ? pism.getPaxosIDVersion() : null;
	}

	// used (only) by RequestBatcher for already batched RequestPackets
	protected void proposeBatched(RequestPacket requestPacket) {
		if (requestPacket != null)
			this.handlePaxosPacket(requestPacket);
	}

	/**
	 * Propose a request to the paxos group with name paxosID.
	 * 
	 * @param paxosID
	 * @param requestPacket
	 * @param callback
	 * @return The paxosID:version represented as a String to which the request
	 *         got proposed; null if no paxos group named paxosID exists
	 *         locally.
	 */
	public String propose(String paxosID, String requestPacket,
			ExecutedCallback callback) {
		return propose(paxosID,
				(new RequestPacket(this.outstanding.generateUnusedID(),
						requestPacket, false)).setReturnRequestValue(),
				callback);
	}

	/**
	 * Avoids unnecessary conversion to string and back if request happens to be
	 * RequestPacket.
	 * 
	 * @param paxosID
	 * @param request
	 * @param callback
	 * @return Refer {@link #propose(String, String,ExecutedCallback)}.
	 */
	public String propose(String paxosID, Request request,
			ExecutedCallback callback) {
		return this.propose(paxosID, this.getRequestPacket(request), callback);
	}

	/**
	 * @param paxosID
	 * @param request
	 * @param callback
	 * @return Refer {@link #proposeStop(String, String, ExecutedCallback)}.
	 */
	public String proposeStop(String paxosID, Request request,
			ExecutedCallback callback) {
		// if (request instanceof RequestPacket)
		// return this.propose(paxosID, (RequestPacket) request, callback);
		return this.propose(paxosID, this.getRequestPacket(request, true),
				callback);
	}

	@SuppressWarnings("deprecation")
	private RequestPacket getRequestPacket(Request request, boolean stop) {
		return request instanceof RequestPacket && request.getRequestType().getInt()==PaxosPacket.PaxosPacketType.REQUEST.getInt() ?
		// return as-is
		(stop == ((RequestPacket) request).isStopRequest()) ? (RequestPacket) request
				:
				// fix stop
				new RequestPacket(((RequestPacket) request).requestID,
						((RequestPacket) request).requestValue, stop,
						(RequestPacket) request)
				:
				// copy over ClientRequest info
				request instanceof ClientRequest ? 
						(new RequestPacket(
						((ClientRequest) request).getRequestID(),
						((ClientRequest) request).toString(), stop,
						((ClientRequest) request).getClientAddress()))
						.setReturnRequestValue() :
				// default
						new RequestPacket(this.outstanding.generateUnusedID(),
								request.toString(), stop)
								.setReturnRequestValue();
	}

	private RequestPacket getRequestPacket(Request request) {
		return this.getRequestPacket(request, false);
	}

	/**
	 * @param paxosID
	 * @param version
	 * @param request
	 * @param callback
	 * @return Refer {@link #proposeStop(String, int, Request,ExecutedCallback)}
	 *         .
	 */
	public String proposeStop(String paxosID, int version, Request request,
			ExecutedCallback callback) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version) {
			return this.propose(paxosID, (RequestPacket) (this
					.getRequestPacket(request, true).putPaxosID(paxosID,
					version)), callback);
		} else
			PaxosConfig.log.log(Level.INFO,
					"{0} failed to propose stop request for {1}:{2}; {3}",
					new Object[] {
							this,
							paxosID,
							version,
							(pism == null ? " no paxos instance found " : pism
									.getPaxosIDVersion() + " pre-exists") });
		return null;
	}

	/**
	 * Proposes a request to stop a specific version of paxosID. There is no way
	 * to stop a paxos replicated state machine other than by issuing a stop
	 * request. The only way to know for sure that the RSM is stopped is by the
	 * application receiving the stop request.
	 * 
	 * @param paxosID
	 * @param value
	 * @param version
	 * @param callback
	 * @return The paxosID:version represented as a String to which this request
	 *         was proposed.
	 */
	public String proposeStop(String paxosID, int version, String value,
			ExecutedCallback callback) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version) {
			return this.propose(paxosID, (RequestPacket) ((new RequestPacket(
					this.outstanding.generateUnusedID(), value, true))
					.setReturnRequestValue().putPaxosID(paxosID, version)),
					callback);
		} else
			PaxosConfig.log.log(Level.INFO,
					"{0} failed to propose stop request for {1}:{2}; {3}",
					new Object[] {
							this,
							paxosID,
							version,
							(pism == null ? " no paxos instance found " : pism
									.getPaxosIDVersion() + " pre-exists") });
		return null;
	}

	/**
	 * Stop the paxos instance named paxosID on this machine irrespective of the
	 * version. Note that there can be at most one paxos version with a given
	 * name on a machine.
	 * 
	 * @param paxosID
	 * @param value
	 * @param callback
	 * @return The paxosID:version string to which the request was proposed.
	 */
	public String proposeStop(String paxosID, String value,
			ExecutedCallback callback) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism == null)
			return null;
		else
			return this
					.proposeStop(paxosID, pism.getVersion(), value, callback);
	}

	/**
	 * @param paxosID
	 * @param version
	 * @return The final state wrapped in a StringContainer. We use
	 *         StringContainer to distinguish between null state and no state
	 *         when null checkpoint state is enabled.
	 */
	public StringContainer getFinalState(String paxosID, int version) {
		/* The wait below is a hack to force a little wait through
		 * synchronization. If a stop request is being executed concurrently
		 * while a getFinalState request arrives, it is better for the stop
		 * request to finish and the final checkpoint to be created before we
		 * issue getEpochFinalCheckpoint. Otherwise, we would return a null
		 * response for getEpochFinalCheckpointState here and the requester of
		 * the checkpoint is forced to time out and resend the request after a
		 * coarse-grained timeout. This wait is just an optimization and is not
		 * necessary for safety. */
		this.synchronizedNoop(paxosID, version);
		return this.paxosLogger.getEpochFinalCheckpointState(paxosID, version);
	}

	/**
	 * We are only allowed to delete a stopped paxos instance. There is no
	 * public method to force-kill a paxos instance other than by successfully
	 * creating a paxos instance with the same name and higher version. The only
	 * way to stop a paxos instance is to get a stop request committed.
	 * 
	 * @param paxosID
	 * @param version
	 * @return Returns true if the paxos instance {@code paxosID:version} exists
	 *         and is stopped. If so, the instance will be deleted.
	 */
	public boolean deleteStoppedPaxosInstance(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version)
			// nothing to do here as stopped paxos instances are auto-killed
			if (pism.synchronizedNoop() && pism.isStopped())
				return true;
		return false;
	}

	/**
	 * @param paxosID
	 * @param version
	 * @return Returns true if the final state was successfully deleted or there
	 *         was nothing to delete.
	 */
	public boolean deleteFinalState(String paxosID, int version) {
		// might as well force-kill the instance at this point
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && pism.getVersion() == version)
			this.kill(pism);
		// needed for concurrent epoch final state creation to finish
		this.synchronizedNoop(paxosID, version);
		assert (pism == null || pism.getVersion() != version || pism
				.isStopped());
		return this.paxosLogger.deleteEpochFinalCheckpointState(paxosID,
				version);
	}

	/**
	 * The Integer return value as opposed to int is convenient to say that
	 * there is no epoch.
	 * 
	 * @param paxosID
	 * @return Integer version of paxos instance named {@code paxosID}.
	 */
	public Integer getVersion(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null)
			return (int) pism.getVersion();
		else {
			return this.paxosLogger.getEpochFinalCheckpointVersion(paxosID);
		}
	}

	/**
	 * @param myAddress
	 * @param niot
	 * @return {@code this}
	 */
	public PaxosManager<NodeIDType> initClientMessenger(
			InetSocketAddress myAddress,
			InterfaceNIOTransport<NodeIDType, ?> niot) {
		PaxosManager<NodeIDType> pm = this.initClientMessenger(myAddress,
				false, niot);
		if (SSL_MODES.valueOf(Config.getGlobalString(PC.CLIENT_SSL_MODE)) != SSL_MODES.CLEAR)
			pm = pm.initClientMessenger(myAddress, true, niot);
		return pm;
	}

	/**
	 * @param myAddress
	 * @param ssl
	 * @return {@code this}
	 */
	private PaxosManager<NodeIDType> initClientMessenger(
			InetSocketAddress myAddress, boolean ssl,
			InterfaceNIOTransport<NodeIDType, ?> nioTransport) {
		Messenger<InetSocketAddress, JSONObject> cMsgr = null;
		SSLMessenger<NodeIDType, ?> msgr = (nioTransport instanceof Messenger ? (SSLMessenger<NodeIDType, ?>) nioTransport
				: null);

		try {
			int clientPortOffset = ssl ? Config
					.getGlobalInt(PC.CLIENT_PORT_SSL_OFFSET) : Config
					.getGlobalInt(PC.CLIENT_PORT_OFFSET);

			if (clientPortOffset > 0) {
				InetSocketAddress myAddressOffsetted = new InetSocketAddress(
						myAddress.getAddress(), myAddress.getPort()
								+ clientPortOffset);
				PaxosConfig.log.log(Level.INFO,
						"{0} creating client messenger at {1}; (offset={2}{3})",
						new Object[] { this, myAddressOffsetted,
								clientPortOffset, ssl ? "/SSL" : "" });

				MessageNIOTransport<InetSocketAddress, JSONObject> createdNIOTransport = null;

				cMsgr = new JSONMessenger<InetSocketAddress>(
						createdNIOTransport = new MessageNIOTransport<InetSocketAddress, JSONObject>(
								myAddressOffsetted.getAddress(),
								myAddressOffsetted.getPort(),
								/* Client facing demultiplexer is single
								 * threaded to keep clients from overwhelming
								 * the system with request load. */
								(Config.getGlobalString(PC.JSON_LIBRARY)
										.equals("org.json") ? new JSONDemultiplexer(
										0, true)
										: new FastDemultiplexer(
												Config.getGlobalInt(PC.CLIENT_DEMULTIPLEXER_THREADS),
												true)),
								ssl ? SSLDataProcessingWorker.SSL_MODES.valueOf(Config
										.getGlobalString(PC.CLIENT_SSL_MODE))
										: SSL_MODES.CLEAR).setName(this.intToString(this.myID)));
				if (Config.getGlobalBoolean(PC.STRICT_ADDRESS_CHECKS)
						&& !createdNIOTransport.getListeningSocketAddress()
								.equals(myAddressOffsetted))
					// Note: will throw false positive exception on EC2
					throw new IOException(
							"Unable to listen on specified socket address at "
									+ myAddressOffsetted
									+ " != "
									+ createdNIOTransport
											.getListeningSocketAddress());
				assert (msgr != null);
				if (ssl)
					msgr.setSSLClientMessenger(cMsgr);
				else
					msgr.setClientMessenger(cMsgr);
			}
		} catch (IOException e) {
			e.printStackTrace();
			PaxosConfig.log.severe(e.getMessage());
			System.exit(1);
		}
		return this;
	}

	/**
	 * Forces a checkpoint, but not guaranteed to happen immediately.
	 * 
	 * @param paxosID
	 */
	public void forceCheckpoint(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null)
			pism.forceCheckpoint();
	}

	/**
	 * Specifies the level of reordering of decisions that prompts a
	 * sync-decisions request.
	 * 
	 * @param limit
	 */
	public void setOutOfOrderLimit(int limit) {
		this.outOfOrderLimit = Math.max(limit, 1);
	}

	protected int getOutOfOrderLimit() {
		return this.outOfOrderLimit;
	}

	private int getNumOutstandingOrQueued() {
		return (this.outstanding.requests.size())
				+ this.requestBatcher.getQueueSize();
	}

	// queue of outstanding requests
	protected void incrOutstanding(RequestPacket request) {
		request.setEntryReplicaAndReturnCount(this.myID);

		// if (request.getEntryReplica() == getMyID())
		this.outstanding.enqueue(new RequestAndCallback(request, null));
		if (request.batchSize() > 0)
			for (RequestPacket req : request.getBatched())
				// if (request.getEntryReplica() == getMyID())
				this.outstanding.enqueue(new RequestAndCallback(req, null));
		if (Util.oneIn(10))
			DelayProfiler.updateMovAvg("outstanding",
					this.outstanding.requests.size());
	}

	/**
	 * @param interval
	 */
	public void setInterCheckpointInterval(int interval) {
		this.interCheckpointInterval = interval;
	}

	/**
	 * @return Inter-checkpoint interval.
	 */
	public int getInterCheckpointInterval() {
		return this.interCheckpointInterval;
	}

	/**
	 * @param maxGap
	 */
	public void setMaxSyncDecisionsGap(int maxGap) {
		this.checkpointTransferTrigger = maxGap;
	}

	/**
	 * @return Minimum delay between successive sync decisions for the same
	 *         paxos instance. This rate limit prevents unnecessary sync'ing
	 *         under high load when a nontrivial extent of out-of-order-ness is
	 *         expected.
	 */

	public long getMinResyncDelay() {
		return this.minResyncDelay;
	}

	/**
	 * @param minDelay
	 */
	public void setMinResyncDelay(long minDelay) {
		this.minResyncDelay = minDelay;
	}

	/**
	 * @return Maximum reordering among received decisions that when exceeded
	 *         prompts a checkpoint transfer as opposed to a sync-decisions
	 *         operation.
	 */
	public int getMaxSyncDecisionsGap() {
		return this.checkpointTransferTrigger;
	}

	/**
	 * @return Returns true if null checkpoints are enabled.
	 */
	public boolean isNullCheckpointStateEnabled() {
		return this.nullCheckpointsEnabled;
	}

	/**
	 * Removes all persistent state. Just avoid using this deprecated method.
	 */
	@Deprecated
	protected synchronized void resetAll() {
		this.pinstances.clear();
		this.corpses.clear();
		this.paxosLogger.removeAll();
	}

	private static final synchronized void open() {
		{
			closed = false;
		}
	}

	private static final synchronized void closeAll() {
		{
			closed = true;
		}
	}

	private static final synchronized boolean allClosed() {
		{
			return closed;
		}
	}

	private boolean isClosed() {
		return allClosed();
	}

	/**
	 * Gracefully closes PaxosManager.
	 */
	public final void close() {
		/* The static method closeAll sets the closed flag so as to prevent any
		 * further new packet processing across all instances of PaxosManager. */
		closeAll();

		/* The static method waitToFinishAll waits until the static method
		 * getProcessing returns true, i.e., there is some PaxosManager that has
		 * started processing a packet (via handlePaxosMessage) but not finished
		 * processing it. Once closeAll returns and then waitToFinishAll
		 * returns, there can be no ongoing or future packet processing by any
		 * instance of PaxosManager in this JVM. */
		waitToFinishAll();

		/* Close logger, FD, messenger, request batcher, executor */
		this.paxosLogger.close();
		this.FD.close();
		this.messenger.stop();
		this.requestBatcher.stop();
		this.ppBatcher.stop();
		this.largeCheckpointer.close();
		this.executor.shutdownNow();
		this.appExecuteThreadPool.shutdownNow();
		if(this.myApp instanceof GigapaxosShutdownable) ((GigapaxosShutdownable)this.myApp).shutdown();

		for (Iterator<PaxosInstanceStateMachine> pismIter = this.pinstances
				.concurrentIterator(); pismIter.hasNext();)
			PaxosConfig.log.log(Level.FINE,
					"{0} terminating with paxos instance state {1}",
					new Object[] { this, pismIter.next().toStringLong() });
	}

	/**
	 * @return Idle period after which paxos instances are paused.
	 */
	protected static final long getDeactivationPeriod() {
		return Config.getGlobalInt(PC.DEACTIVATION_PERIOD);
	}

	/********************* End of public methods ***********************/

	int totalRcvd = 0;

	synchronized void incrTotalRcvd(int n) {
		totalRcvd += n;
	}

	synchronized int getTotalRcvd(int n) {
		return totalRcvd;
	}

	private static final boolean EMULATE_UNREPLICATED = Config
			.getGlobalBoolean(PC.EMULATE_UNREPLICATED);

	private final boolean emulateUnreplicated(PaxosPacket request) {
		if (!EMULATE_UNREPLICATED || !(request instanceof RequestPacket))
			return false;
		// else will finally return true
		// pretend-execute new requests

		PaxosInstanceStateMachine.execute(null, this, myApp,
				((RequestPacket) request).setEntryReplica(getMyID()), false);
		return true;
	}

	private static final boolean LAZY_PROPAGATION = Config
			.getGlobalBoolean(PC.LAZY_PROPAGATION);

	private final boolean emulateLazyPropagation(PaxosPacket request) {
		if (!LAZY_PROPAGATION || !(request instanceof RequestPacket))
			return false;

		// else will finally return true
		try {
			// extract newly received requests
			request = ((RequestPacket) request)
					.getEntryReplicaRequestsAsBatch(getMyID())[1];

			if (request != null) {
				((RequestPacket) request).setEntryReplica(getMyID());
				// broadcast newly received requests to others
				PaxosInstanceStateMachine pism = this.getInstance(request
						.getPaxosID());
				MessagingTask mtask = pism != null ? new MessagingTask(
						pism.otherGroupMembers(), request) : null;
				this.send(mtask);

				// pretend-execute newly received requests
				PaxosInstanceStateMachine.execute(null, this, myApp,
						((RequestPacket) request).setEntryReplica(getMyID()),
						false);
			}
		} catch (JSONException e) {
			PaxosConfig.log.severe(this + " unable to parse " + request.getSummary());
			e.printStackTrace();
		} catch (IOException e) {
			PaxosConfig.log.severe(this + " IOException while lazy-propagating "
					+ request.getSummary());
			e.printStackTrace();
		}
		return true;
	}

	protected Set<NodeIDType> getNodesFromStringSet(Set<String> strNodes) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (String strNode : strNodes) {
			nodes.add(this.unstringer.valueOf(strNode));
		}
		return nodes;
	}

	protected Set<String> getStringNodesFromIntArray(int[] members) {
		Set<String> nodes = new HashSet<String>();
		for (int member : members) {
			nodes.add(this.integerMap.get(member).toString());
		}
		return nodes;
	}

	private/* synchronized */PaxosInstanceStateMachine getInstance(
			String paxosID, boolean tryHotRestore, boolean tryRestore) {
		// long methodEntryTime = System.currentTimeMillis();
		PaxosInstanceStateMachine pism = null;
		synchronized (this) {
			// atomic get and mark active to prevent concurrent pause
			if ((pism = pinstances.get(paxosID)) != null)
				pism.markActive();
		}
		if (pism == null
				&& ((tryHotRestore && (pism = this.unpause(paxosID)) != null) || (tryRestore && (pism = this
						.restore(paxosID)) != null)))
			// nothing here
			;
		// DelayProfiler.updateDelay("getInstance", methodEntryTime);
		return pism != null ? pism : pinstances.get(paxosID);
	}

	private PaxosInstanceStateMachine getInstance(String paxosID) {
		return this.getInstance(paxosID, true, true);
	}

	private boolean isPauseEnabled() {
		return Config.getGlobalBoolean(PC.PAUSE_OPTION);
	}

	private boolean isHibernateable() {
		return Config.getGlobalBoolean(PC.HIBERNATE_OPTION);
	}

	/* For each paxosID in the logs, this method creates the corresponding paxos
	 * instance and rolls it forward from the last checkpointed state.
	 * 
	 * Synchronized because this method invokes an incremental read on the
	 * database that currently does not support parallelism. But the
	 * "synchronized" qualifier here is not necessary for correctness. */
	private synchronized void initiateRecovery(NodeIDType id) {
		boolean found = false;
		int groupCount = 0, freq = 1;
		long initTime = System.currentTimeMillis();
		PaxosConfig.log.log(Level.INFO, "{0} beginning to recover checkpoints",
				new Object[] { this });
		while (this.paxosLogger.initiateReadCheckpoints(true))
			; // acquires lock
		RecoveryInfo pri = null;
		while ((pri = this.paxosLogger.readNextCheckpoint(true)) != null) {
			found = true;
			assert (pri.getPaxosID() != null);
			// start paxos instance, restore app state from checkpoint if any
			// and roll forward
			try {
				this.recover(pri.getPaxosID(), pri.getVersion(), this.myID,
						getNodesFromStringSet(pri.getMembers()), myApp,
						pri.getState());
			} catch (PaxosInstanceCreationException pice) {
				// should we remove this checkpoint?
				pice.printStackTrace();
				PaxosConfig.log.severe(this + " unable to create paxos instance "
						+ pri.getPaxosID());
			}
			if ((++groupCount) % freq == 0) {
				freq *= 2;
			}
		}
		this.paxosLogger.closeReadAll(); // releases lock
		PaxosConfig.log.log(Level.INFO,
				"{0} has recovered checkpoints for {1} paxos groups",
				new Object[] { this, groupCount });
		if (!found) {
			PaxosConfig.log.warning("No checkpoint state found for node "
					+ id
					+ ". This can only happen if\n"
					+ "(1) the node is newly joining the system, or\n(2) the node previously crashed before "
					+ "completing even a single checkpoint, or\n(3) the node's checkpoint was manually deleted.");
		}
		int logCount = 0;
		freq = 1;
		// roll forward all logged messages in a single pass
		PaxosConfig.log.log(Level.INFO, "{0} beginning to roll forward logged messages",
				new Object[] { this });
		while (this.paxosLogger.initiateReadMessages())
			; // acquires lock
		String paxosPacketString = null;
		PaxosPacket paxosPacket = null;
		/**
		 * Set packetizer for logger. We need this in order to have the benefits
		 * of caching the original string form of received accepts to reduce
		 * serialization overhead. Without a packetizer, the logger doesn't know
		 * how to convert original string node IDs (that came over the network)
		 * in the logged messages to integer IDs. The alternative would be to
		 * just store integer IDs in logged messages (trusting that we will
		 * always be able to have IntegerMap be able to convert back to the
		 * original node IDs because checkpoint recovery happens before rolling
		 * forward logs), but that means we wouldn't have the stringified
		 * caching optimization.
		 */
		this.paxosLogger
				.setPacketizer(new AbstractPaxosLogger.PaxosPacketizer() {

					@Override
					protected PaxosPacket stringToPaxosPacket(String str) {
						try {
							PaxosPacket pp = PaxosPacket
									.getPaxosPacket(PaxosManager.this
											.fixNodeStringToInt(new JSONObject(
													str)));
							assert (pp != null) : str;
							return pp;
						} catch (JSONException e) {
							e.printStackTrace();
							try {
								PaxosConfig.log.severe(PaxosManager.this
										+ " unable to decode string of byte length "
										+ str.getBytes("ISO-8859-1").length);
							} catch (UnsupportedEncodingException e1) {
								e1.printStackTrace();
							}
						}
						return null;
					}

					@Override
					protected PaxosPacket stringToPaxosPacket(byte[] bytes) {
						PaxosPacketType type = PaxosPacket.getType(bytes);
						try {
							if (type == PaxosPacketType.ACCEPT) {
								return new AcceptPacket(bytes);
							}
							// else
							return this.stringToPaxosPacket(MessageExtractor
									.decode(bytes));
						} catch (UnsupportedEncodingException
								| UnknownHostException e) {
							// likely an undecodeable accept
							e.printStackTrace();
							return null;
						}
					}
				});

		/**
		 * We need this for {@link SQLPaxosLogger} for logging messages with int
		 * IDs converted back to the original string IDs. The logger will invoke
		 * stringToPaxosPacket(String) above while reading logged messages from
		 * disk. Disk I/O is no different from network I/O in that integer IDs
		 * have meaning outside of PaxosInstanceStateMachine soft state.
		 * 
		 * We don't always have to convert int to string IDs because if we have
		 * {@code stringified} already stored, it is already in a
		 * network-friendly form.
		 */
		this.paxosLogger
				.setPaxosPacketStringifier(new AbstractPaxosLogger.PaxosPacketStringifier() {

					@Override
					protected String paxosPacketToString(PaxosPacket paxosPacket) {
						PaxosPacket.PaxosPacketType type = paxosPacket
								.getType();
						String stringified = null;
						// three loggable types
						if (type == PaxosPacket.PaxosPacketType.ACCEPT
								|| type == PaxosPacket.PaxosPacketType.PREPARE
								|| type == PaxosPacket.PaxosPacketType.DECISION) {
							if (paxosPacket instanceof RequestPacket
									&& (stringified = ((RequestPacket) paxosPacket)
											.getStringifiedSelf()) != null)
								return stringified;
							else
								try {
									net.minidev.json.JSONObject jsonSmart = paxosPacket
											.toJSONSmart();
									stringified = jsonSmart != null ? PaxosManager.this.messenger
											.fixNodeIntToString(jsonSmart)
											.toString() :
									// prepares don't have toJSONSmartImpl()
											paxosPacket.toString();
								} catch (JSONException je) {
									// exception will never be thrown
									assert (false);
									// at least use default toString in any case
									stringified = paxosPacket.toString();
								}
						}
						return stringified;
					}
				});

		try {
			while ((paxosPacket = this.paxosLogger.readNextMessage()) != null) {
				paxosPacket = PaxosPacket.markRecovered(paxosPacket);
				Level level = Level.FINEST;
				PaxosConfig.log.log(level,
						"{0} rolling forward logged message {1}",
						new Object[] { this,
								paxosPacket.getSummary(PaxosConfig.log.isLoggable(level)) });
				this.handlePaxosPacket((paxosPacket));
				if ((++logCount) % freq == 0) {
					freq *= 2;
				}
			}
		} catch (NumberFormatException e) {
			Util.suicide(PaxosConfig.log, this + " recovery interrupted while parsing "
					+ paxosPacketString
					+ ";\n Exiting because it is unsafe to continue recovery.");
			e.printStackTrace();
		}
		this.paxosLogger.closeReadAll(); // releases lock
		PaxosConfig.log.log(Level.INFO,
				"{0} rolled forward {1} messages total across {2} paxos groups",
				new Object[] { this, logCount, groupCount });

		// need to make another pass to mark all instances as active
		while (this.paxosLogger.initiateReadCheckpoints(true))
			; // acquires lock
		while ((pri = this.paxosLogger.readNextCheckpoint(true)) != null) {
			found = true;
			assert (pri.getPaxosID() != null);
			PaxosInstanceStateMachine pism = getInstance(pri.getPaxosID());
			if (pism != null) {
				pism.setActive();
				// force sync upon reboot
				if(System.currentTimeMillis()-initTime < Config.getGlobalLong
						(PC.DEACTIVATION_PERIOD))
					pism.poke(true)
							;
			}
			Boolean isActive = pism != null ? pism.isActive() : null;
			PaxosConfig.log.log(Level.INFO,
					"{0} recovered paxos instance {1}; isActive = {2} ",
					new Object[] { this,
							pism != null ? pism.toStringLong() : null, isActive });
		}
		this.paxosLogger.closeReadAll(); // releases lock

		this.hasRecovered = true;
		this.notifyRecovered();
		PaxosConfig.log.log(Level.INFO,
				"------------------{0} recovery complete-------------------",
				new Object[] { this });
	}

	protected boolean hasRecovered() {
		return this.hasRecovered;
	}

	protected boolean hasRecovered(PaxosInstanceStateMachine pism) {
		// if (ONE_PASS_RECOVERY)
		return this.hasRecovered()
		// else
		// !ONE_PASS_RECOVERY
		// return
				&& (pism != null && pism.isActive());
	}

	private static final boolean BATCHED_ACCEPT_REPLIES = Config
			.getGlobalBoolean(PC.BATCHED_ACCEPT_REPLIES);

	private PaxosMessenger<NodeIDType> wrapMessenger(
			PaxosMessenger<NodeIDType> msgr) {
		return new PaxosMessenger<NodeIDType>(PaxosManager.this.messenger) {
			public void send(MessagingTask mtask) throws JSONException,
					IOException {
				PaxosManager.this.send(mtask, BATCHED_ACCEPT_REPLIES, false);
			}

			public void send(MessagingTask[] mtasks) throws JSONException,
					IOException {
				PaxosManager.this.notifyLoggedDecisions(mtasks);
				super.send(mtasks);
			}
		};
	}

	private void notifyLoggedDecisions(MessagingTask[] mtasks) {
		PaxosInstanceStateMachine pism = null;
		for (Map.Entry<String, Integer> entry : PaxosPacketBatcher
				.getMaxLoggedDecisionMap(mtasks).entrySet())
			if ((pism = PaxosManager.this.getInstance(entry.getKey())) != null)
				pism.garbageCollectDecisions(entry.getValue());
	}

	/* All messaging is done using PaxosMessenger and MessagingTask. This method */
	protected void send(MessagingTask mtask, boolean coalesce, boolean logMsg)
			throws JSONException, IOException {
		if (mtask == null)
			return;

		if (logMsg && mtask instanceof LogMessagingTask) {
			AbstractPaxosLogger.logAndMessage(this.paxosLogger,
					(LogMessagingTask) mtask);// , this.messenger);
		} else {
			this.sendOrLoopback(coalesce ? PaxosManager.this.ppBatcher
					.coalesce(mtask) : mtask);
		}
	}

	protected void send(MessagingTask mtask) throws JSONException, IOException {
		this.send(mtask, true, true);
	}

	private void sendOrLoopback(MessagingTask mtask) throws JSONException,
			IOException {
		MessagingTask local = MessagingTask.getLoopback(mtask, myID);
		if (local != null && !local.isEmptyMessaging())
			for (PaxosPacket pp : local.msgs)
				if (pp.getType() == PaxosPacketType.BATCHED_PAXOS_PACKET)
					for (PaxosPacket packet : ((BatchedPaxosPacket) pp)
							.getPaxosPackets())
						this.handlePaxosPacket((packet));
				else
					this.handlePaxosPacket((pp));
		this.messenger.send(MessagingTask.getNonLoopback(mtask, myID));
	}

	protected void send(InetSocketAddress sockAddr, Request request,
			InetSocketAddress listenSockAddr) throws JSONException, IOException {
		try {
		this.messenger.sendClient(
				sockAddr,
				request instanceof RequestPacket ? (((RequestPacket) request)
						.toBytes()) : request, listenSockAddr);
		} catch(AssertionError e) {
			System.out.println(this + " incurred assertion error for request " + request);
			throw e;
		}
	}

	/* A clean kill completely removes all trace of the paxos instance (unlike
	 * pause or hibernate or an unclean kill). The copy of the instance kept in
	 * corpses will not be revived; in fact, is there temporarily only to make
	 * sure that the instance does not get re-created as a "missed birthing"
	 * instance creation.
	 * 
	 * synchronized because any changes to pinstances must be synchronized as
	 * createPaxosInstance and other methods also use it. Also, we want to move
	 * the paxos instance atomically from pinstances to corpses. If not atomic,
	 * it can result in the corpse (to-be) getting resurrected if a packet for
	 * the instance arrives in between.
	 * 
	 * Note: pause or hibernate or crashAndRecover like options just invoke
	 * softCrash and not kill that is more fatal. None of them do any of the
	 * following: (1) wipe out checkpoint state, (2) nullify app state, or (3)
	 * move pism to corpses, all of which kill does.
	 * 
	 * Invariant: Only a paxos instance currently inserted in the map can be
	 * killed. */
	protected synchronized void kill(PaxosInstanceStateMachine pism,
			boolean clean) {
		assert (pism != null);
		if (this.isClosed())
			throw new PaxosInstanceDestructionException(this
					+ " has already been closed");

		/* Do nothing if existing version not same as kill target. Note that if
		 * existing is null, we still kill pism. It is unlikely but possible for
		 * existing to be null, say, because the instance pism just got paused,
		 * but for pism or this paxos manager to issue a kill immediately after. */
		PaxosInstanceStateMachine existing = this
				.getInstance(pism.getPaxosID());
		if (existing != null && (pism.getVersion() - existing.getVersion() < 0)) {
			PaxosConfig.log.log(Level.INFO,
					"{0} unable to kill {1} because {2} already exists in the map",
					new Object[] { this, pism, existing });
			return;
		}

		// else got murder work to do (even if existing==null)
		while (!pism.kill(clean))
			PaxosConfig.log.severe("Problem stopping paxos instance " + pism.getPaxosID()
					+ ":" + pism.getVersion());
		incrKilled();
		this.softCrash(pism);
		this.corpses.put(pism.getPaxosID(), pism);
		executor.schedule(new Cremator(pism.getPaxosID(), this.corpses),
				Config.getGlobalInt(PC.MORGUE_DELAY), TimeUnit.MILLISECONDS);
		this.notifyUponKill();
	}

	/* Default kill is clean. Unclean kill is used by PaxosInstanceStateMachine
	 * when it is unable to execute an app request successfully. An unclean kill
	 * does not wipe out checkpoint state or app state, but does move the paxos
	 * instance to corpses because it is as good as dead at this point.
	 * 
	 * Why not just do a softCrash instead of corpsing out in the case of an app
	 * execution error? The instance might get re-created as a "missed birthing"
	 * case, but that would still be safe and, maybe, the roll forward this time
	 * around might succeed. There isnt' a strong reason one way or the other. */
	private void kill(PaxosInstanceStateMachine pism) {
		this.kill(pism, true);
	}

	// soft crash means stopping and "forgetting" the instance
	private synchronized void softCrash(PaxosInstanceStateMachine pism) {
		assert (pism != null);
		pism.forceStop();
		this.pinstances.remove(pism.getPaxosID());
	}

	/* For testing. Similar to hibernate but without forcing a checkpoint and
	 * followed immediately by a restore from the most recent (but possibly
	 * still quite old) checkpoint. */
	protected void crashAndRecover(PaxosInstanceStateMachine pism) {
		if (pism == null || this.isClosed()
				|| !this.pinstances.containsValue(pism))
			return;
		PaxosConfig.log.warning(this + " crash-and-recovering paxos instance " + pism);
		this.softCrash(pism);
		this.recover(pism.getPaxosID(), pism.getVersion(), pism.getMyID(),
				this.integerMap.get(Util.arrayToIntSet(pism.getMembers())),
				pism.getApp()); // state will be auto-recovered
	}

	// Checkpoint and go to sleep on disk. Currently not used.
	protected synchronized boolean hibernate(PaxosInstanceStateMachine pism) {
		if (pism == null || this.isClosed()
				|| !this.pinstances.containsValue(pism)
				|| !this.isHibernateable())
			return false;

		// else hibernate
		boolean hibernated = false;
		PaxosConfig.log.log(Level.INFO, "{0} trying to hibernate {1}", new Object[] { this,
				pism });
		boolean stopped = pism.tryForcedCheckpointAndStop();

		if (stopped) {
			this.softCrash(pism);
			hibernated = true;
			PaxosConfig.log.log(Level.INFO, "{0} sucessfully hibernated {1}", new Object[] {
					this, pism });
		}
		return hibernated;
	}

	// Undo hibernate. Will rollback, so not very efficient.
	private synchronized PaxosInstanceStateMachine restore(String paxosID) {
		if (this.isClosed() || !this.isHibernateable())
			return null;
		PaxosInstanceStateMachine pism = null;
		if ((pism = this.pinstances.get(paxosID)) != null)
			return pism;
		PaxosConfig.log.log(Level.INFO, "{0} trying to restore instance {1}", new Object[] {
				this, paxosID });
		RecoveryInfo pri = this.paxosLogger.getRecoveryInfo(paxosID);
		if (pri != null)
			pism = this.recover(paxosID, pri.getVersion(), this.myID,
					this.getNodesFromStringSet(pri.getMembers()), this.myApp,
					pri.getState());
		if (pism != null)
			PaxosConfig.log.log(Level.INFO,
					"{0} successfully restored hibernated instance {1}",
					new Object[] { this, pism });
		else
			PaxosConfig.log.log(Level.WARNING, "{0} unable to restore paxos instance {1}",
					new Object[] { this, paxosID });
		return pism;
	}

	/* Pausing is like hibernate but without checkpointing or subsequent
	 * recovery overhead.
	 * 
	 * We need batched pausing for efficiency, otherwise a deluge of pauses can
	 * take a long time and slow down regular request processing. The method
	 * below pauses one instance at time, which will hold up regular request
	 * processing for that much time because of the synchronization below, but
	 * if we pause in modest-sized batches, we will hold up regular request
	 * processing for slightly longer but get done with pausing all pausable
	 * instances much more quickly. */
	protected HotRestoreInfo pause(PaxosInstanceStateMachine pism) {
		if (pism == null || this.isClosed()
				|| !this.pinstances.containsValue(pism) || !isPauseEnabled())
			return null;
		PaxosConfig.log.log(Level.FINE, "{0} trying to pause {1}", new Object[] { this,
				pism.getPaxosIDVersion() });

		// else try to pause
		long pauseInitTime = System.currentTimeMillis();
		HotRestoreInfo hri = pism.tryPause();
		if (hri != null) {
			/* crash means the same as removing from pinstances as well as
			 * activePaxii for an already stopped paxos instance. */
			assert (pism.isStopped());
			this.softCrash(pism);
			if (Util.oneIn(Integer.MAX_VALUE))
				DelayProfiler.updateDelay("pause", pauseInitTime);
			PaxosConfig.log.log(Level.FINE, "{0} successfully paused {1}", new Object[] {
					this, pism.getPaxosIDVersion() });
		} else
			PaxosConfig.log.log(Level.FINE, "{0} failed to pause {1}", new Object[] { this,
					pism });
		return hri;
	}

	// FIXME: unused but may be needed if we shift to DiskMap
	protected final Diskable<String, PaxosInstanceStateMachine> disk = new Diskable<String, PaxosInstanceStateMachine>() {

		@Override
		public Set<String> commit(
				Map<String, PaxosInstanceStateMachine> toCommit)
				throws IOException {
			return PaxosManager.this.pause(toCommit, false);
		}

		@Override
		public PaxosInstanceStateMachine restore(String key) throws IOException {
			return PaxosManager.this.unpause(key);
		}
	};

	/* Batched pausing can speed up pause throughput by leveraging batching in a
	 * database. It doesn't help unpausing though. */
	protected synchronized Set<String> pause(
			Map<String, PaxosInstanceStateMachine> pauseBatch, boolean dequeue) {
		if (this.isClosed() || !isPauseEnabled())
			return null;

		long t = System.currentTimeMillis();
		// pause paxos instances
		Map<String, HotRestoreInfo> hriMap = new HashMap<String, HotRestoreInfo>();
		for (PaxosInstanceStateMachine pism : pauseBatch.values()) {
			String paxosID = pism.getPaxosID();
			HotRestoreInfo hri = dequeue ? this.pause(pism) : pism.tryPause();
			// unpause here can return null unless synchronized
			if (hri != null)
				hriMap.put(paxosID, hri);
		}

		// write paused state to disk
		Map<String, HotRestoreInfo> pausedHRIMap = this.paxosLogger
				.pause(hriMap);
		for (HotRestoreInfo pausedHRI : pausedHRIMap.values())
			hriMap.remove(pausedHRI.paxosID);

		// roll back failed pause attempts
		for (HotRestoreInfo hri : hriMap.values()) {
			boolean rolledBack = this.createPaxosInstance(hri.paxosID,
					hri.version,
					this.integerMap.get(Util.arrayToIntSet(hri.members)),
					this.myApp, null, hri, false) != null;
			if (rolledBack)
				PaxosConfig.log.log(Level.INFO, "{0} rolled back pausing of {1}",
						new Object[] { this, hri.paxosID });
			else
				PaxosConfig.log.log(Level.SEVERE,
						"{0} unable to roll back failed pausing of {1}",
						new Object[] { this, hri.paxosID });
		}
		DelayProfiler.updateDelay("pause", t, pausedHRIMap.size());
		return pausedHRIMap.keySet();
	}

	private StringLocker stringLocker = new StringLocker();

	// Hot restores from disk, i.e., restores quickly without need for rollback
	private/* synchronized */PaxosInstanceStateMachine unpause(String paxosID) {
		if (this.isClosed() || !this.hasRecovered() || !this.isPauseEnabled())
			return null;

		PaxosInstanceStateMachine restored = null;
		if ((restored = this.pinstances.get(paxosID)) != null)
			return restored;

		long unpauseInitTime = System.currentTimeMillis();
		/* stringLocker allows concurrent unpause of different paxosIDs while
		 * serializing unpause attempts of the same paxosID. We need to
		 * serialize unpause attempts to the same paxosID as otherwise, even if
		 * paused state exists, only the first unpause attempt will retrieve it
		 * but one of the latter attempts that found no paused state could still
		 * race ahead causing getInstance to return null resulting in a packet
		 * drop even though the instance exists locally. Such packet drops will
		 * not affect safety but it is good to avoid them. */
		synchronized (this.stringLocker.get(paxosID)) {
			PaxosConfig.log.log(Level.FINE, "{0} about to try to unpause instance {1}",
					new Object[] { this, paxosID });

			HotRestoreInfo hri = this.paxosLogger.unpause(paxosID);

			if (hri != null) {
				PaxosConfig.log.log(Level.FINE,
						"{0} successfully unpaused paused instance {1}",
						new Object[] { this, paxosID });
				restored = this.createPaxosInstance(hri.paxosID, hri.version,
						this.integerMap.get(Util.arrayToIntSet(hri.members)),
						this.myApp, null, hri, false);
				// if (restored != null) restored.markActive();
			} else
				PaxosConfig.log.log(Level.FINE, "{0} unable to unpause instance {1}",
						new Object[] { this, paxosID });
		}
		this.stringLocker.remove(paxosID);

		if (restored != null)
			assert (restored.isActive());
		if (restored != null)
			DelayProfiler.updateDelay("unpause", unpauseInitTime);
		return restored;
	}

	/* Create paxos instance restoring app state from checkpoint if any and roll
	 * forward. */
	private PaxosInstanceStateMachine recover(String paxosID, int version,
			int id, Set<NodeIDType> members, Replicable app, String state) {
		PaxosConfig.log.log(Level.FINE, "{0} {1}:{2} {3} recovering", new Object[] { this,
				paxosID, version, members });
		PaxosInstanceStateMachine pism = this.createPaxosInstance(paxosID,
				version, (members), app, null, null, false);
		return pism;
	}

	private PaxosInstanceStateMachine recover(String paxosID, int version,
			int id, Set<NodeIDType> members, Replicable app) {
		// state will be auto-recovered if it exists
		return this.recover(paxosID, version, id, members, app, null);
	}

	/* After rollForward, recovery is complete. In particular, we don't have to
	 * wait for any more processing of messages, e.g., out of order decisions to
	 * "settle", because the only important thing is to replay and process
	 * ACCEPTs and PREPAREs so as to bring the acceptor state up to speed, which
	 * is a purely local and non-blocking sequence of operations. Coordinator
	 * state in general is not recoverable; the easiest way to recover it is to
	 * simply call checkRunForCoordinator, which will happen automatically upon
	 * the receipt of any external packet. */
	private void rollForward(String paxosID, int version) {
		if (/* !ONE_PASS_RECOVERY || */this.hasRecovered()) {
			PaxosConfig.log.log(Level.FINE, "{0} about to roll forward {1}:{2}",
					new Object[] { this, paxosID, version });
			AbstractPaxosLogger.rollForward(paxosLogger, paxosID, messenger);
			PaxosInstanceStateMachine pism = (this.getInstance(paxosID, true,
					false));
			pism.setActive();
			assert (this.getInstance(paxosID, false, false) != null);
			// if (pism != null) pism.poke();
		}
		// TESTPaxosConfig.setRecovered(this.myID, paxosID, true); // testing
	}

	private void findPaxosInstance(PaxosPacket pp) throws JSONException {
		if (!this.hasRecovered()) {
			this.logPacketDrop(pp);
			return;
		}
		assert (pp.getPaxosID() != null);
		/* If it is possible for there to be no initial state checkpoint, under
		 * missed birthing, an acceptor may incorrectly report its gcSlot as -1,
		 * and if a majority do so (because that majority consists all of missed
		 * birthers), a coordinator may propose a proposal for slot 0 even
		 * though an initial state does exist, which would end up overwriting
		 * the initial state. So we can not support ambiguity in whether there
		 * is initial state or not. If we force initial state checkpoints (even
		 * null state checkpoints) to always exist, so that missed birthers can
		 * always set the initial gcSlot to 0. */
		if (!this.isNullCheckpointStateEnabled()) {
			this.logPacketDrop(pp);
			return;
		}
		PaxosInstanceStateMachine zombie = this.corpses.get(pp.getPaxosID());
		if (zombie == null || (zombie.getVersion() - pp.getVersion()) < 0)
			findReplicaGroup(pp);
		else
			this.logPacketDrop(pp);
	}

	private void logPacketDrop(PaxosPacket pp) {
		PaxosConfig.log.log(PaxosPacket.isRecovery(pp) ? Level.FINE : Level.INFO,
				"{0} dropping packet {1} as unable to find active paxos instance",
				new Object[] { this, pp.getSummary() });
	}

	/* The two methods, heardFrom and isNodeUp, below are the only ones that
	 * invoke nodeMap.get(int). They are only invoked after the corresponding
	 * NodeIDType is already inserted in the map. */
	protected void heardFrom(int id) {
		try {
			this.FD.heardFrom(this.integerMap.get(id));
		} catch (RuntimeException re) {
			// do nothing, can happen during recovery
			PaxosConfig.log.log(Level.INFO, "{0} has no NodeIDType entry for integer {1}",
					new Object[] { this, id });
		}
	}

	protected boolean isNodeUp(int id) {
		return (FD != null ? FD.isNodeUp(this.integerMap.get(id)) : false);
	}

	protected boolean lastCoordinatorLongDead(int id) {
		return (FD != null ? FD
				.lastCoordinatorLongDead(this.integerMap.get(id)) : true);
	}

	protected long getDeadTime(int id) {
		return (FD != null ? FD.getDeadTime(this.integerMap.get(id)) : System
				.currentTimeMillis());
	}

	protected AbstractPaxosLogger getPaxosLogger() {
		return paxosLogger;
	}

	protected PaxosMessenger<NodeIDType> getMessenger() {
		return this.messenger;
	}

	/****************** Start of methods to gracefully finish processing **************/
	private static final synchronized void setProcessing(boolean b) {
		if (b)
			processing++;
		else
			processing--;
		if (processing == 0)
			PaxosManager.class.notify();
	}

	private static final synchronized boolean getProcessing() {
		return processing > 0;
	}

	protected static final synchronized void waitToFinishAll() {
		try {
			while (getProcessing()) {
				PaxosManager.class.wait();
			}
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	private synchronized void timedWaitCanCreateOrExistsOrHigher(
			String paxosID, int version, long timeout) {
		try {
			if (!this.canCreateOrExistsOrHigher(paxosID, version))
				wait(timeout);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	// wait until timeout for creation
	private synchronized void timedWaitForExistence(String paxosID,
			int version, long timeout) {
		try {
			long waitStartTime = System.currentTimeMillis();
			while (!this.equalOrHigherVersionExists(paxosID, version)
					&& System.currentTimeMillis() - waitStartTime < timeout)
				wait(timeout);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	protected synchronized void waitCanCreateOrExistsOrHigher(String paxosID,
			int version) {
		try {
			while (!this.canCreateOrExistsOrHigher(paxosID, version))
				wait();
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}

	}

	protected synchronized void notifyUponKill() {
		notifyAll();
	}

	protected synchronized void notifyUponCreation() {
		notifyAll();
	}

	/**
	 * This is a common default create paxos instance creation method, i.e., we
	 * wait for lower versions to be killed if necessary but, after a timeout,
	 * create the new instance forcibly anyway. This method won't create the
	 * instance if the same or higher version already exists. One concern with
	 * this method is that force killing lower versions in order to start higher
	 * versions can cause the epoch final state to be unavailable at *any* node
	 * in immediately lower version. It is okay to kill even an immediately
	 * preceding instance only if we already have the necessary state
	 * (presumably obtained from a stopped immediately preceding instance at
	 * some other group member). In the reconfiguration protocol, we must ensure
	 * this by starting a new epoch only after receiving a successful
	 * acknowledgment for stopping the previous epoch. However, if say only one
	 * node has executed the previous epoch's stop and then starts the new epoch
	 * and causes other new epoch replicas to force-create the new epoch, and
	 * then crashes, the new epoch is stuck.
	 * 
	 * @param paxosID
	 * @param version
	 * @param gms
	 * @param app
	 * @param state
	 * @param timeout
	 * @return Returns true if this paxos instance or one with a higher version
	 *         number was successfully created.
	 */
	public boolean createPaxosInstanceForcibly(String paxosID, int version,
			Set<NodeIDType> gms, Replicable app, String state, long timeout) {
		if (timeout < Config.getGlobalInt(PC.CAN_CREATE_TIMEOUT))
			timeout = Config.getGlobalInt(PC.CAN_CREATE_TIMEOUT);
		// still do a timed wait
		this.timedWaitCanCreateOrExistsOrHigher(paxosID, version, timeout);
		return this.createPaxosInstanceForcibly(paxosID, version, gms, app,
				state);
	}

	private synchronized boolean createPaxosInstanceForcibly(String paxosID,
			int version, Set<NodeIDType> gms, Replicable app, String state) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		// if equal or higher instance exists
		if (pism != null && (pism.getVersion() - version >= 0))
			return true;

		if (pism != null && pism.getVersion() - version < 0) {
			PaxosConfig.log.log(Level.INFO,
					"{0} forcibly killing {1} in order to create {2}:{3}",
					new Object[] { this, pism.getPaxosIDVersion(), paxosID,
							version });
			this.kill(pism); // will succeed or throw exception
		}
		boolean created = this.createPaxosInstance(paxosID, version, gms, app,
				state);
		return created;
	}

	/**
	 * @param paxosID
	 * @param version
	 * @return Returns true if the paxos instance {@code paxosID:version} exists
	 *         or a higher version exists.
	 */
	public boolean equalOrHigherVersionExists(String paxosID, int version) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism != null && (pism.getVersion() - version >= 0))
			return true;
		return false;
	}

	/**
	 * Unlike the method {@link #equalOrHigherVersionExists(String, int)}, this
	 * method checks for whether an equal or higher version was previously
	 * stopped. We need this check to prevent going back in time, i.e., starting
	 * an epoch after it has been stopped.
	 * 
	 * @param paxosID
	 * @param version
	 * @return
	 */
	private boolean equalOrHigherVersionStopped(String paxosID, int version) {
		// lastVersion may be active or recently stopped
		Integer lastStoppedVersion = this.paxosLogger
				.getEpochFinalCheckpointVersion(paxosID);
		if (lastStoppedVersion != null && lastStoppedVersion - version >= 0)
			return true;
		return false;
	}

	/****************** End of methods to gracefully finish processing **************/

	private String printLog(String paxosID) {
		return ("State for " + paxosID + ": Checkpoint: " + this.paxosLogger
				.getStatePacket(paxosID));
	}

	// send a request asking for your group
	private void findReplicaGroup(PaxosPacket pp) throws JSONException {
		FindReplicaGroupPacket findGroup = new FindReplicaGroupPacket(
				this.myID, pp); // paxosID and version should be within
		int nodeID = FindReplicaGroupPacket.getNodeID(pp);
		NodeIDType node = nodeID > 0 ? this.integerMap.get(nodeID) : 
				(pp instanceof RequestPacket && (nodeID=((RequestPacket) pp).getEntryReplica()) > 0 ? 
				this.integerMap.get(nodeID)
				: null)
				;
		if (nodeID >= 0) {
			try {
				PaxosConfig.log.log(Level.INFO,
						"{0} received paxos {1} for non-existent instance {2}; contacting {3} for help",
						new Object[] { this, pp.getSummary(), pp.getPaxosID(),
								node });
				this.send(new MessagingTask(nodeID, findGroup));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		} else if(!this.corpses.containsKey(pp.getPaxosID())) {
			PaxosConfig.log.log(pp instanceof RequestPacket ? Level.INFO : Level.INFO,
					"{0} cant find group member in {1} {2}",
					new Object[] {
							this,
							pp.getSummary(),
							pp instanceof RequestPacket ? ((((RequestPacket) pp)
									.getEntryReplica() != this.myID ? "forwarded by entry node "
									+ node
									: "received from client"
											+ ((RequestPacket) pp)
													.getClientAddress()))
									+ ", which may be because the paxos instance has not yet been created or has been recently deleted."
									: "" });
		}
	}

	// process a request or send an answer
	private void processFindReplicaGroup(FindReplicaGroupPacket findGroup)
			throws JSONException {
		MessagingTask mtask = null;
		if (findGroup.group == null && findGroup.nodeID != this.myID) {
			// process a request
			PaxosInstanceStateMachine pism = this.getInstance(findGroup
					.getPaxosID());
			if (pism != null && pism.getVersion() == findGroup.getVersion()) {
				FindReplicaGroupPacket frgReply = new FindReplicaGroupPacket(
						pism.getMembers(), findGroup);
				mtask = new MessagingTask(findGroup.nodeID, frgReply);
			}
		} else if (findGroup.group != null && findGroup.nodeID == this.myID) {
			// process an answer
			PaxosInstanceStateMachine pism = this.getInstance(findGroup
					.getPaxosID());
			if (pism == null
					|| (pism.getVersion() - findGroup.getVersion()) < 0) {
				// wait to see if it gets created anyway;
				this.timedWaitForExistence(findGroup.getPaxosID(),
						findGroup.getVersion(),
						Config.getGlobalInt(PC.WAIT_TO_GET_CREATED_TIMEOUT));

				// wait and kill lower versions if any
				if (pism != null
						&& (pism.getVersion() - findGroup.getVersion() < 0)) {
					// wait to see if lower versions go away anyway
					this.timedWaitCanCreateOrExistsOrHigher(
							findGroup.getPaxosID(), findGroup.getVersion(),
							Config.getGlobalInt(PC.CAN_CREATE_TIMEOUT));
					this.kill(pism);
				}

				// create out of "nothing"
				boolean created = this.createPaxosInstance(findGroup
						.getPaxosID(), findGroup.getVersion(), this.integerMap
						.get(Util.arrayToIntSet(findGroup.group)), myApp, null,
						null, false, true) != null;
				if (created)
					PaxosConfig.log.log(Level.INFO,
							"{0} created paxos instance {1}:{2} from nothing because it apparently missed its birthing",
							new Object[] { this, findGroup.getPaxosID(),
									findGroup.getVersion() });
			}
		}
		try {
			if (mtask != null)
				this.send(mtask);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/*************************** Start of activePaxii related methods **********************/

	private static final double PAUSE_SIZE_THRESHOLD = Config
			.getGlobalDouble(PC.PAUSE_SIZE_THRESHOLD);
	private long lastDeactivationAttempt = 0;

	private boolean shouldTryDeactivation() {
		synchronized (this) {
			if ((System.currentTimeMillis() - lastDeactivationAttempt < Config
					.getGlobalLong(PC.DEACTIVATION_PERIOD))
					|| (this.pinstances.size() < this.pinstances.capacity()
							* PAUSE_SIZE_THRESHOLD))
				return false;
			lastDeactivationAttempt = System.currentTimeMillis();
			return true;
		}
	}

	private static final int FORCE_PAUSE_FACTOR = 10;
	private static final double PAUSE_RATE_LIMIT = Config
			.getGlobalDouble(PC.PAUSE_RATE_LIMIT);

	/* We currently make a periodic pass over all active instances to sync and
	 * check for deactivation. However, deactivation can be delegated to a
	 * general-purpose DiskMap like structure and can be done more efficiently
	 * in *expectation* than a full active sweep (at a ~2x higher memory cost)
	 * every iteration. The sync also does not need a sweep if we maintain a
	 * separate set of paxosIDs (with a commensurate memory cost) that are not
	 * caught up and we sync just those. A sweep thread (like below or in
	 * DiskMap) is needed in order to have the benefit of reducing the memory
	 * footprint down to as small as necessary in steady-state without slowing
	 * down the critical path. The bad case for a sweep is when most of the
	 * mapped instances are active, so the sweeps end up being redundant; it is
	 * unclear if this actually affects performance much given the low priority
	 * hint for the thread and the explict rate limit. */
	private void syncAndDeactivate() {
		if (isClosed() || this.pinstances.size() == 0)
			return;
		if (!this.shouldTryDeactivation())
			return;

		long t0 = System.currentTimeMillis();
		RateLimiter rateLimiter = new RateLimiter(PAUSE_RATE_LIMIT);
		PaxosConfig.log.log(Level.FINE,
				"{0} initiating deactivation attempt, |activePaxii| = {1}",
				new Object[] { this, this.pinstances.size() });
		int numPaused = 0;
		Map<String, PaxosInstanceStateMachine> batch = new HashMap<String, PaxosInstanceStateMachine>();

		// cuckoo hashmap now supports an efficient iterator
		for (Iterator<PaxosInstanceStateMachine> pismIter = this.pinstances
				.concurrentIterator(); pismIter.hasNext();) {
			PaxosInstanceStateMachine pism = pismIter.next();

			String paxosID = pism.getPaxosID();

			if (pism.isLongIdle()
			// if size > capacity/2, pause 1/FORCE_PAUSE_FACTOR fraction
					|| (this.pinstances.size() > this.pinstances.capacity() / 2 && numPaused < this.pinstances
							.capacity() / FORCE_PAUSE_FACTOR)) {
				PaxosConfig.log.log(Level.FINER, "{0} trying to pause {1} [{2}]",
						new Object[] { this, paxosID, pism });
				/* The sync below ensures that, at least once every deactivation
				 * period, we sync decisions for an active paxos instance. This
				 * is handy when a paxos instance is not caught up but doesn't
				 * get any new messages either, say, because nothing new is
				 * happening, then it has no reason to do anything but will
				 * remain unnecessarily active; the sync here allows it to
				 * potentially catch up and possibly be paused in the next
				 * deactivation round if there is still no action by then. The
				 * sync is useful irrespective of whether or not the instance is
				 * caught up for pausability
				 * 
				 * Overhead: This sync imposes a message overhead of up to A
				 * messages A is the number of active paxos instances. For
				 * example, with 10K active instances, this method could send
				 * 10K messages, which is high. However, for each instance, the
				 * sync message will get sent only if it has not recently sent a
				 * sync message *and* it is out of sync or it has just started
				 * up and has a very low outOfOrder limit. Consequently, we
				 * should avoid having a large number of paxos instances with a
				 * very low outOfOrderLimit, especially if all they plan to do
				 * is to start up and do nothing, otherwise, they will cause a
				 * one-time deluge of sync messages before being paused.
				 * 
				 * If active instances are generally busy but out of sync, we
				 * could impose a bandwidth overhead of A/D where D is the
				 * deactivation thread's period, e.g., A=10K, D=30secs => an
				 * overhead of 333 messages/sec, which although seems high is
				 * possible only if none of those paxos instances sent a sync
				 * reauest in the last S seconds, where S is the minimum
				 * inter-sync interval for each instance (default 1 second). In
				 * expectation, a high overhead relative to the inevitable paxos
				 * commit overhead (of 3 messages per replica per decision) is
				 * unlikely unless the workload and network behave
				 * adversarially, i.e., in every D period, each active paxos
				 * instance executes just enough decisions for it to be possible
				 * for its outOfOrder threshold to be triggered and the network
				 * reorders some of those decisions. If the instance commits
				 * many more decisions than the outOfOrder threshold, then the
				 * sync message adds only a small relative overhead, e.g., if
				 * the outOfOrder threshold is 10, roughly 33 messages (accept,
				 * acceptReply, decision) would be required to commit at least
				 * 11 decisions that would then trigger just one sync message.
				 * If the outOfOrder threshold is 1, then the sync message could
				 * add one message to every 6 expected messages (for 2 paxos
				 * commits) at this replica, a ~15% overhead. But with such a
				 * low outOfOrder threshold, we should not be having a large
				 * number of paxos instances in the first place. */
				this.syncPaxosInstance(pism, false);
				// rate limit if well under capacity
				if (this.pinstances.size() < this.pinstances.capacity()
						/ FORCE_PAUSE_FACTOR)
					rateLimiter.record();
				batch.put(pism.getPaxosID(), pism);
				if (batch.size() >= PAUSE_BATCH_SIZE) {
					Set<String> batchPaused = pause(batch, true);
					if (batchPaused != null)
						numPaused += batchPaused.size();
					PaxosConfig.log.log(Level.FINE, "{0} paused {1}", new Object[] { this,
							batchPaused });
					this.printPauseLog(batchPaused);
					batch.clear();
				}
			}
		}
		if (!batch.isEmpty()) {
			this.printPauseLog(this.pause(batch, true));
		}
		DelayProfiler.updateDelay("deactivation", t0);
	}

	private void printPauseLog(Collection<String> paused) {
		if(this.isClosed() || !this.isPauseEnabled() || paused==null) return;
		
		// can not call synchronized methods inside log statements
		long totalCreated = this.getNumCreated();
		long totalCurrent = this.getNumInstancesAndResetChanged();

		PaxosConfig.log.log(paused.size() > 0 || this.totalInstancesChanged ? Level.INFO
				: Level.FINE,
				"{0} deactivated {1} idle instances {2}; has {3} active instances{4}; avg_pause_delay = {5};"
						+ " avg_deactivation_loop_delay = {6}; total_instances_created = {7}; total_current_instances = {8}",
				new Object[] {
						this,
						paused.size(),
						Util.truncatedLog(paused, PRINT_LOG_SIZE),
						this.pinstances.size(),
						this.pinstances.size() < 10 ? Arrays
								.asList(this.pinstances.keySet().toArray())
								: "", Util.nmu(DelayProfiler.get("pause")),
						Util.ms(DelayProfiler.get("deactivation")),
						totalCreated, totalCurrent });
	}

	private static final int PRINT_LOG_SIZE = 16;
	private static final int PAUSE_BATCH_SIZE = Config
			.getGlobalInt(PC.PAUSE_BATCH_SIZE);

	/*************************** End of activePaxii related methods **********************/

	private class Cremator implements Runnable {
		String id = null;
		HashMap<String, PaxosInstanceStateMachine> map = null;

		Cremator(String paxosID,
				HashMap<String, PaxosInstanceStateMachine> zombies) {
			this.id = paxosID;
			this.map = zombies;
		}

		public void run() {
			synchronized (map) {
				map.remove(id);
			}
		}
	}

	/* Both deactivates, i.e., removes temporary active paxos state, and pauses,
	 * i.e., swaps safety-critical paxos state to disk. */
	private class Deactivator implements Runnable {
		public void run() {
			/* There is a good reason not to slow down this thread as that will
			 * essentially slow down the rate of group creation. Groups can at
			 * most be created as fast as they can be paused, otherwise we will
			 * run out of memory. */
			// Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
			try {
				syncAndDeactivate();
			} catch (Exception e) {
				// must continue running despite any exceptions
				e.printStackTrace();
			}
		}
	}

	protected int getMyID() {
		return this.myID;
	}

	/**
	 * @return NodeIDType of this.
	 */
	public NodeIDType getNodeID() {
		return this.messenger.getMyID();
	}

	private synchronized void waitToRecover() {
		try {
			this.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private synchronized void notifyRecovered() {
		this.notifyAll();
	}

	// convert string -> NodeIDType -> int (can *NOT* convert string directly to
	// int)
	private JSONObject fixNodeStringToInt(JSONObject json) throws JSONException {
		// long t = System.nanoTime();
		if (!PaxosMessenger.ENABLE_INT_STRING_CONVERSION)
			return json;
		// FailureDetectionPacket already has generic NodeIDType
		if (PaxosPacket.getPaxosPacketType(json) == PaxosPacket.PaxosPacketType.FAILURE_DETECT)
			return json;

		if (json.has(PaxosPacket.NodeIDKeys.B.toString())) {
			// fix ballot string
			String ballotString = json.getString(PaxosPacket.NodeIDKeys.B
					.toString());
			NodeIDType nodeID = this.unstringer.valueOf(Ballot
					.getBallotCoordString(ballotString));
			if (nodeID != null)
				// assert (coordInt != null);
				json.put(PaxosPacket.NodeIDKeys.B.toString(),
						new Ballot(Ballot.getBallotNumString(ballotString),
								this.integerMap.put(nodeID)).toString());
		} else if (json.has(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			// fix group string (JSONArray)
			JSONArray jsonArray = json
					.getJSONArray(PaxosPacket.NodeIDKeys.GROUP.toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				String memberString = jsonArray.getString(i);
				int memberInt = this.integerMap.put(this.unstringer
						.valueOf(memberString));
				jsonArray.put(i, memberInt);
			}
			json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), jsonArray);
		} else
			for (PaxosPacket.NodeIDKeys key : PaxosPacket.NodeIDKeys.values()) {
				if (json.has(key.toString())) {
					// fix default node string
					String nodeString = json.getString(key.toString());
					if (!nodeString.equals(IntegerMap.NULL_STR_NODE)) {
						int nodeInt = this.integerMap.put(this.unstringer
								.valueOf(nodeString));
						json.put(key.toString(), nodeInt);
					}
				}
			}
		// if(Util.oneIn(100))
		// DelayProfiler.updateDelayNano("fixNodeStringToIntJSON", t);
		return json;
	}

	private net.minidev.json.JSONObject fixNodeStringToInt(
			net.minidev.json.JSONObject json) {
		// long t = System.nanoTime();
		// FailureDetectionPacket already has generic NodeIDType
		if ((Integer) json.get(PaxosPacket.Keys.PT.toString()) == PaxosPacket.PaxosPacketType.FAILURE_DETECT
				.getInt())
			return json;

		if (json.containsKey(PaxosPacket.NodeIDKeys.B.toString())) {
			// fix ballot string
			String ballotString = (String) json.get(PaxosPacket.NodeIDKeys.B
					.toString());
			json.put(
					PaxosPacket.NodeIDKeys.B.toString(),
					new Ballot(Ballot.getBallotNumString(ballotString),
							this.integerMap.put(this.unstringer.valueOf(Ballot
									.getBallotCoordString(ballotString))))
							.toString());
		} else if (json.containsKey(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			// fix group string (JSONArray)
			Collection<?> jsonArray = (Collection<?>) json
					.get(PaxosPacket.NodeIDKeys.GROUP.toString());

			Set<Integer> group = new HashSet<Integer>();
			for (Object element : jsonArray) {
				String memberString = element.toString();
				int memberInt = this.integerMap.put(this.unstringer
						.valueOf(memberString));
				group.add(memberInt);
			}
			json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), group);
		} else
			for (PaxosPacket.NodeIDKeys key : PaxosPacket.NodeIDKeys.values()) {
				if (json.containsKey(key.toString())) {
					// fix default node string
					String nodeString = json.get(key.toString()).toString();
					if (!nodeString.equals(IntegerMap.NULL_STR_NODE)) {
						int nodeInt = this.integerMap.put(this.unstringer
								.valueOf(nodeString));
						json.put(key.toString(), nodeInt);
					}
				}
			}
		// if(Util.oneIn(100))
		// DelayProfiler.updateDelayNano("fixNodeStringToIntJSONSmart", t);
		return json;
	}

	public String toString() {
		return this.getClass().getSimpleName() + ":"
				+ this.integerMap.get(myID);
	}

	protected String intToString(int id) {
		return this.integerMap.get(id).toString();
	}

	/* ********************** Testing methods below ********************* */

	private void testingInitialization() {
		if (cleanDB)
			while (!this.paxosLogger.removeAll())
				;

	}

	private static boolean cleanDB = false;

	/**
	 * If set to true, {@link PaxosManager} will clear the DB upon creation.
	 * This static flag applies only to PaxosManager instances created after
	 * this flag has been set to true.
	 * 
	 * @param clean
	 */
	public static final void startWithCleanDB(boolean clean) {
		cleanDB = clean;
	}

	/**
	 * This test method is deprecated and will either be removed or
	 * significantly revamped. Use TESTPaxosMain instead to run a single machine
	 * test with multiple virtual nodes.
	 * 
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws JSONException
	 */
	@Deprecated
	static void test(String[] args) throws InterruptedException, IOException,
			JSONException {
		int[] members = TESTPaxosConfig.getDefaultGroup();
		int numNodes = members.length;

		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>(2000);
		snc.localSetup(Util.arrayToIntSet(members));

		@SuppressWarnings("unchecked")
		PaxosManager<Integer>[] pms = new PaxosManager[numNodes];
		TESTPaxosApp[] apps = new TESTPaxosApp[numNodes];

		/* We always test with the first member crashed. This also ensures that
		 * the system is fault-tolerant to the failure of the default
		 * coordinator, which in our policy is the first (or lowest numbered)
		 * node. */
		TESTPaxosConfig.crash(members[0]);
		/* We disable sending replies to client in PaxosManager's unit-test. To
		 * test with clients, we rely on other tests in TESTPaxosMain
		 * (single-machine) or on TESTPaxosNode and TESTPaxosClient for
		 * distributed testing. */
		TESTPaxosConfig.setSendReplyToClient(false);

		/* This setting is "guilty until proven innocent", i.e., each node will
		 * start out assuming that all other nodes are dead. This is probably
		 * too pessimistic as it will cause every node to run for coordinator
		 * when it starts up but is good for testing. */
		FailureDetection.setParanoid();

		// Set up paxos managers and apps with nio
		for (int i = 0; i < numNodes; i++) {
			System.out.println("Initiating PaxosManager at node " + members[i]);
			JSONNIOTransport<Integer> niot = new JSONNIOTransport<Integer>(
					members[i], snc, new PacketDemultiplexerDefault(), true);
			apps[i] = new TESTPaxosApp(niot); // app, PM reuse nio
			pms[i] = new PaxosManager<Integer>(members[i], snc, niot, apps[i]);
		}

		System.out.println("Initiated all " + numNodes
				+ " paxos managers with failure detectors..\n");

		/* We don't rigorously test with multiple groups as they are
		 * independent, but this is useful for memory testing. */
		int numPaxosGroups = 2;
		String[] names = new String[numPaxosGroups];
		for (int i = 0; i < names.length; i++)
			names[i] = "paxos" + i;

		System.out.println("Creating " + numPaxosGroups
				+ " paxos groups each with " + numNodes
				+ " members each, one each at each of the " + numNodes
				+ " nodes");
		for (int node = 0; node < numNodes; node++) {
			int k = 1;
			for (int group = 0; group < numPaxosGroups; group++) {
				// creating a paxos instance may induce recovery from disk
				pms[node].createPaxosInstance(names[group], 0,
						Util.arrayToIntSet(members), apps[node], null, null,
						false);
				if (numPaxosGroups > 1000
						&& ((group % k == 0 && ((k *= 2) > 0)) || group % 100000 == 0)) {
					System.out.print(group + " ");
				}
			}
			System.out.println("..node" + members[node] + " done");
		}
		Thread.sleep(1000);

		/* Wait for all paxos managers to finish recovery. Recovery is finished
		 * when initiateRecovery() is complete. At this point, all the paxos
		 * groups at that node would have also rolled forward. */
		int maxRecoverySlot = -1;
		int maxRecoveredNode = -1;
		for (int i = 0; i < numNodes; i++) {
			while (!TESTPaxosConfig.isCrashed(members[i])
					&& !TESTPaxosConfig.getRecovered(members[i], names[0])) {
				PaxosConfig.log.info("Waiting for node " + members[i] + " to recover ");
				pms[i].waitToRecover();
			}
			PaxosConfig.log.info(members[i]
					+ " finished recovery including rollback;\n" + names[0]
					+ " recovered at slot " + apps[i].getNumCommitted(names[0]));
			// need max recovery slot for names[0] below
			maxRecoverySlot = Math.max(maxRecoverySlot,
					apps[i].getNumCommitted(names[0]));
			maxRecoveredNode = i;
		}

		System.out.println("all nodes done creating groups.");

		/*********** Finished creating paxos instances for testing *****************/

		/************* Begin ClientRequestTask **************************/
		ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);
		class ClientRequestTask implements Runnable {
			private final RequestPacket request;
			private final PaxosManager<Integer> paxosManager;

			ClientRequestTask(RequestPacket req, PaxosManager<Integer> pm) {
				request = req;
				paxosManager = pm;
			}

			public void run() {
				try {
					JSONObject reqJson = request.toJSONObject();
					JSONPacket.putPacketType(reqJson,
							PaxosPacketType.PAXOS_PACKET.getInt());
					paxosManager.propose(request.getPaxosID(), request, null);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}
		/************* End ClientRequestTask **************************/

		/* Create and schedule requests. All requests are scheduled immediately
		 * to test concurrency */
		int numRequests = 1000;
		RequestPacket[] reqs = new RequestPacket[numRequests];
		ScheduledFuture<?>[] futures = new ScheduledFuture[numRequests];
		int numExceptions = 0;
		double scheduledDelay = 0;
		for (int i = 0; i < numRequests; i++) {
			reqs[i] = new RequestPacket(i, "[ Sample write request numbered "
					+ i + " ]", false);
			reqs[i].putPaxosID(names[0], 0);
			JSONObject reqJson = reqs[i].toJSONObject();
			JSONPacket.putPacketType(reqJson,
					PaxosPacketType.PAXOS_PACKET.getInt());
			try {
				ClientRequestTask crtask = new ClientRequestTask(reqs[i],
						pms[1]);
				futures[i] = (ScheduledFuture<?>) execpool.schedule(crtask,
						(long) scheduledDelay, TimeUnit.MILLISECONDS);
				scheduledDelay += 0;
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		/* Any exceptions below could occur because of exceptions inside paxos.
		 * Scheduling a request will invoke PaxosManager.propose() that will
		 * cause it to send the request to the corresponding
		 * PaxosInstanceStateMachine. */
		PaxosConfig.log.info("Waiting for request scheduling to complete.");
		for (int i = 0; i < numRequests; i++) {
			try {
				futures[i].get();
			} catch (Exception e) {
				e.printStackTrace();
				numExceptions++;
			}
		}
		PaxosConfig.log.info("Request scheduling complete; numExceptions=" + numExceptions);
		Thread.sleep(1000);

		/* Wait for scheduled requests to finish being processed by paxos. We
		 * check for this by checking that at least one node has executed up to
		 * the slot number maxRecoverySlot + numRequests. */
		while (apps[maxRecoveredNode].getNumCommitted(names[0]) < maxRecoverySlot
				+ numRequests) {
			apps[maxRecoveredNode].waitToFinish();
			;
		}
		PaxosConfig.log.info(maxRecoveredNode + " has executed up to slot "
				+ (maxRecoverySlot + numRequests));

		/* The code below waits for all uncrashed replicas to finish executing
		 * up to the same slot and will then assert the SMR invariant, i.e.,
		 * they all made the same state transitions up to that slot. */
		int numCommitted = 0;
		for (int i = 0; i < numNodes; i++) {
			for (int j = i + 1; j < numNodes; j++) {
				if (TESTPaxosConfig.isCrashed(members[i])
						|| TESTPaxosConfig.isCrashed(members[j]))
					continue; // ignore crashed nodes

				int committed1 = apps[i].getNumCommitted(names[0]);
				int committed2 = apps[j].getNumCommitted(names[0]);
				// Wait for the other node to catch up
				while (committed1 != committed2) {
					if (committed1 > committed2)
						apps[j].waitToFinish(names[0], committed1);
					else if (committed1 < committed2)
						apps[i].waitToFinish(names[0], committed2);
					PaxosConfig.log.info("Waiting : (slot1,hash1)=(" + committed1 + ","
							+ apps[i].getHash(names[0]) + "(; (slot2,hash2="
							+ committed2 + "," + apps[j].getHash(names[0])
							+ ")");
					Thread.sleep(1000);
					committed1 = apps[i].getNumCommitted(names[0]);
					committed2 = apps[j].getNumCommitted(names[0]);
				}
				// Both nodes caught up to the same slot
				assert (committed1 == committed2) : "numCommitted@" + i + "="
						+ committed1 + ", numCommitted@" + j + "=" + committed2;
				// Assert state machine replication invariant
				numCommitted = apps[i].getNumCommitted(names[0]);
				assert (apps[i].getHash(names[0]) == apps[j].getHash(names[0])) : ("Waiting : (slot1,hash1)=("
						+ committed1
						+ ","
						+ apps[i].getHash(names[0])
						+ "(; (slot2,hash2="
						+ committed2
						+ ","
						+ apps[j].getHash(names[0]) + ")");
				; // end of SMR invariant
			}
		}

		/* Print preempted requests if any. These could happen during
		 * coordinator changes. Preempted requests are converted to no-ops and
		 * forwarded to the current presumed coordinator by paxos. */
		String preemptedReqs = "[ ";
		int numPreempted = 0;
		for (int i = 0; i < numRequests; i++) {
			if (!TESTPaxosConfig.isCommitted(reqs[i].requestID)) {
				preemptedReqs += (i + " ");
				numPreempted++;
			}
		}
		preemptedReqs += "]";

		System.out
				.println("\n\nTest completed. Executed "
						+ numCommitted
						+ " requests consistently including "
						+ (numRequests - numPreempted)
						+ " of "
						+ numRequests
						+ " received requests;\nPreempted requests = "
						+ preemptedReqs
						+ "; numExceptions="
						+ numExceptions
						+ "; average message log time="
						+ Util.df(DelayProfiler.get("logDelay"))
						+ "ms.\n"
						+ "\nNote that it is possible for the test to be successful even if the number of consistently\n"
						+ "executed requests is less than the number of received requests as paxos only guarantees\n"
						+ "consistency, i.e., that all replicas executed requests in the same order, not that all requests\n"
						+ "issued will get executed. The latter property can be achieved by clients reissuing requests\n"
						+ "until successfully executed. With reissuals, clients do need to worry about double execution,\n"
						+ "so they should be careful. A client is not guaranteed to get a failure message if the request fails,\n"
						+ "e.g., if the replica receiving a request dies immediately. If the client uses a timeout to detect\n"
						+ "failure and thereupon reissue its request, it is possible that both the original and re-issued\n"
						+ "requests are executed. Clients can get around this problem by using sequence numbers within\n"
						+ "their app, reading the current sequence number, and then trying to commit their write provided the\n"
						+ "sequence number has not changed in the meantime. There are other alternatives, but all of these\n"
						+ "are application-specific; they are not paxos's problem\n");
		for (int i = 0; i < numNodes; i++) {
			System.out.println(pms[i].printLog(names[0]));
		}
		execpool.shutdownNow();
		for (PaxosManager<Integer> pm : pms)
			pm.close();
	}

	protected Replicable getApp(String paxosID) {
		return this.myApp;
	}

	protected MessageDigest getMessageDigest() {
		return RequestPacket.getMessageDigest();
	}

	private static final int DIGEST_THRESHOLD = Config
			.getGlobalInt(PC.DIGEST_THRESHOLD);
	private static final boolean DIGEST_REQUESTS = Config
			.getGlobalBoolean(PC.DIGEST_REQUESTS);

	protected boolean shouldDigest() {
		return DIGEST_REQUESTS && this.pinstances.size() <= DIGEST_THRESHOLD;
	}

	protected AcceptPacket match(AcceptPacket accept) {
		return this.pendingDigests.match(accept);
	}

	protected AcceptPacket release(RequestPacket request, boolean remove) {
		return this.pendingDigests.release(request, remove);
	}

	protected AcceptPacket release(RequestPacket request) {
		return this.release(request, true);
	}

	protected AcceptPacket digest(AcceptPacket multicastAccept) {
		return this.shouldDigest() ? multicastAccept.digest(this
				.getMessageDigest()) : multicastAccept;
	}

	/* Callback for timed out digested accept. Will send a nack accept reply in
	 * order to try to get the sender to send the undigested accept. The
	 * digested accept will be discarded in any case after this callback. */
	private void callbackDigestedAcceptTimeout(AcceptPacket accept) {
		try {
			PaxosConfig.log.log(
					Level.INFO,
					"{0} trying to release digested accept {1}",
					new Object[] { PaxosManager.this,
							accept.getSummary(PaxosConfig.log.isLoggable(Level.INFO)) });
			AcceptPacket released = PaxosManager.this.release(accept, false);
			if (released != null) {
				PaxosManager.this.handlePaxosPacket(released);
			} else
				try {
					// non-loopback send to re-insert accept
					PaxosManager.this.messenger.send(new MessagingTask(
							PaxosManager.this.getMyID(), accept));
					PaxosManager.this.send(new MessagingTask(
							accept.ballot.coordinatorID, new AcceptReplyPacket(
									PaxosManager.this.getMyID(), accept.ballot,
									accept.slot,
									/* FIXME: wraparound means that
									 * maxCheckpointedSlot of 0 can be unsafe.
									 * We need to get the correct value from the
									 * paxos instance. But for now digests are
									 * not disabled by default, so this is okay. */
									0, accept.requestID).setDigestRequest()
									.putPaxosID(accept.getPaxosID(),
											accept.getVersion())));
				} catch (JSONException | IOException e) {
					e.printStackTrace();
				}
		} catch (Exception e) {
			PaxosConfig.log.severe(this + " incurred exception on digested accept timeout");
			e.printStackTrace();
		}
	}

	private void callbackRequestTimeout(RequestPacket request) {
		PaxosPacket accept = null;
		if ((accept = PaxosManager.this.pendingDigests.release(request, true, false)) != null)
			try {
				PaxosManager.this.send(
						new MessagingTask(PaxosManager.this.getMyID(), accept),
						false, false);
			} catch (JSONException | IOException e) {
				PaxosConfig.log.severe(PaxosManager.this
						+ " incurred exception while expiring request and releasing accept "
						+ accept.getSummary());
				e.printStackTrace();
			}
		this.outstanding.totalRequestSize -= request.lengthEstimate();
		Level level = accept != null ? Level.INFO : Level.FINE;
		PaxosConfig.log.log(
				level,
				"{0} garbage collected enqueued request {1}"
						+ (accept != null ? "; released accept {2}" : ""),
				new Object[] {
						PaxosManager.this,
						request.getSummary(PaxosConfig.log.isLoggable(level)),
						accept != null ? accept.getSummary(PaxosConfig.log
								.isLoggable(level)) : "" });
	}

	/**
	 * @param nodes
	 * @return Modified argument after filtering out dead nodes using
	 *         information from its failure detector.
	 */
	public Set<NodeIDType> removeDead(Set<NodeIDType> nodes) {
		for (Iterator<NodeIDType> iter = nodes.iterator(); iter.hasNext();)
			if (!this.FD.isNodeUp(iter.next()))
				iter.remove();
		return nodes;
	}

	protected AcceptPacket getPreviouslyIssuedAccept(RequestPacket proposal) {
		RequestAndCallback rc = null;
		if (((rc = this.outstanding.requests.get(proposal.getRequestID())) != null || (rc = this.outstanding.conflictIDRequests
				.get(proposal)) != null)
				&& rc.requestPacket instanceof AcceptPacket
				&& rc.requestPacket.equals(proposal))
			return (AcceptPacket) rc.requestPacket;
		return null;
	}

	protected AcceptPacket setIssuedAccept(AcceptPacket accept) {
		assert(accept==null || accept.getRequestValue()!=null) : accept;
		RequestAndCallback rc = accept!=null ? this.outstanding.requests.get(accept
				.getRequestID()) : null;
		if (rc != null
				|| (rc = this.outstanding.conflictIDRequests.get(accept)) != null)
			rc.setAcceptPacket(accept);
		return accept;
	}

	/**
	 * isPaxosCoordinator returns true if the paxos instance that live in this
	 * node (i.e., ActiveReplica) think itself is the current paxos coordinator.
	 * Note that, due to asynchrony, it is possible that multiple nodes think
	 * itself as an active coordinator.
	 * @param paxosID instance ID, name of service (or object) being replicated.
	 * @return true if the paxos instanace in this node think itself as
	 *  a paxos coordinator.
	 */
	public boolean isPaxosCoordinator(String paxosID) {
		// use getInstance() method, instead of directly accessing `pinstances`,
		// because a paused PaxosInstanceStateMachine (PISM) could be null.
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism == null) {
			return false;
		}

		return pism.isPaxosCoordinator();
	}

	/**
	 * getPaxosCoordinator returns the node ID, which this node (Active Replica)
	 * think as the coordinator. It is possible this node to return the ID
	 * of itself if it thinks it is the current coordinator at the time of
	 * executing this method. The returned node ID (if not null) can be used
	 * by Messenger to send any protocol messages.
	 * @param paxosID instance ID, name of service (or object) being replicated.
	 * @return the ID of the coordinator node, or null if this node does not
	 *  know who is the current coordinator.
	 */
	public NodeIDType getPaxosCoordinator(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism == null) {
			return null;
		}

		return this.integerMap.get(pism.getCurrentPaxosCoordinator());
	}

	public void tryToBePaxosCoordinator(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism == null) {
			return;
		}

		pism.tryToBeCoordinator();;
	}

	public void restartFromLastCheckpoint(String paxosID) {
		PaxosInstanceStateMachine pism = this.getInstance(paxosID);
		if (pism == null) {
			return;
		}

		this.softCrash(pism);
		pism = this.getInstance(paxosID);
		assert pism != null :
				"failed to restart as pism is failed to be re-created";
		pism.poke(true);
	}

}
