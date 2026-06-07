package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.async.RequestCallbackFuture;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.Reconfigurator;

/**
 * @author arun
 * 
 *         A minimal interface defining reconfigurator server functions. This
 *         interface is implemented by {@link Reconfigurator} and must be
 *         implemented by any client or proxy for reconfigurators.
 *
 */
public interface ReconfiguratorFunctions {

	/**
	 * @param request
	 * @param callback
	 * 
	 * @return A {@link RequestCallbackFuture} object that can be used by the
	 *         caller to retrieve the processed response.
	 */
	public RequestCallbackFuture<ReconfiguratorRequest> sendRequest(
			ReconfiguratorRequest request,
			Callback<Request, ReconfiguratorRequest> callback);

	/**
	 * @param request
	 * @return The response to {@code request} that will have the same type as
	 *         the request.
	 */
	public ReconfiguratorRequest sendRequest(ReconfiguratorRequest request);

	/**
	 * Like {@link #sendRequest(ReconfiguratorRequest)} but bounded: returns
	 * {@code null} if no response arrives within {@code timeoutMs}. Lets the
	 * synchronous HTTP control plane fail fast (e.g. 504) instead of blocking
	 * forever when the reconfigurator silently drops a request.
	 *
	 * @param request
	 * @param timeoutMs maximum time to wait for the response, in milliseconds.
	 * @return The response, or {@code null} on timeout.
	 */
	public ReconfiguratorRequest sendRequest(ReconfiguratorRequest request, long timeoutMs);

	/**
	 * Read-only geo-demand for a service, as a JSON array string of
	 * {@code {"lat":<deg>,"lon":<deg>,"count":<int>}} cells, for visualization
	 * (e.g. the dashboard heatmap). Empty array if no demand has been collected or
	 * the active demand profiler has no geographic notion of demand. Must NOT
	 * mutate/reset the profile.
	 *
	 * @param serviceName service name
	 * @return JSON array string of demand cells (e.g. {@code "[]"}).
	 */
	public String getServiceDemandJson(String serviceName);

	/**
	 * All configured node locations (the candidate placement pool) and which are
	 * currently running as active replicas, as a JSON array string of
	 * {@code {"id":<name>,"lat":<deg>,"lon":<deg>,"active":<bool>}}. Candidates are
	 * nodes with a configured geolocation that are not (yet) in the active set,
	 * which is how the dashboard distinguishes potential locations from running
	 * replicas without paying for idle ones.
	 *
	 * @return JSON array string of node locations.
	 */
	public String getNodeLocationsJson();

	/**
	 * Fire-and-forget submit of a server reconfiguration (e.g. an active-node-config
	 * change that adds/removes an ActiveReplica) into the reconfigurator's own
	 * processing pipeline -- the protocol-task path that dispatches the handler AND
	 * sends the resulting coordination (StartEpoch, state transfer). Returns
	 * immediately; the change proceeds asynchronously (poll {@code /api/v2/nodes} or
	 * a service's {@code /placement} for the result). Unlike
	 * {@link #sendRequest(ReconfiguratorRequest)} (which dispatches via the
	 * callback/3-arg handler path), server reconfiguration packets only have the
	 * 2-arg protocol-task handler, which this path uses.
	 *
	 * @param request a {@code ServerReconfigurationPacket}.
	 * @return true if the event was accepted by the pipeline.
	 */
	public boolean submitServerReconfiguration(ReconfiguratorRequest request);

}
