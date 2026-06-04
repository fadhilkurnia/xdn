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

}
