package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.NodeIdsMetadataPair;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This demand profiler splits the Earth into multiple square grid cells, based on the latitude and
 * longitude. The number of rows and columns are specified by the `NUM_GRID_ROWS` and
 * `NUM_GRID_COLUMNS` constant. This profiler aggregate the number of requests coming from each grid
 * cells, as the information for RC to chose the most appropriate replica placement.
 *
 * <p>TODO: improve the implementation to consider different type of request. - number of
 * coordinated requests - number of local-execution requests - record the request rate instead pf
 * the total number of request
 */
public class XdnGeoDemandProfiler extends AbstractDemandProfile {

  /**
   * Geolocation has Latitude and Longitude component. Latitude indicates the vertical location
   * (i.e., y-axis) while Longitude indicates the horizontal location (i.e., x-axis). One degree in
   * Latitude and Longitude is roughly 111000 meters. Thus, if we want to capture 1x1 km grid, we
   * need to split earth surfaces into 40030 x 19980 matrix. Note that this is mainly true near the
   * equator.
   */
  private static final int NUM_GRID_ROWS = 10;

  private static final int NUM_GRID_COLUMNS = 10;

  // Defines the periodic time in which new demand statistics need to be reported
  // to the control plane (i.e., RC).
  private static final long MIN_DEMAND_REPORT_PERIOD_MS = 10_000; // 10 seconds.

  // Stores the accumulated number of request in all the grid cells.
  int[][] gridTotalRequests = new int[NUM_GRID_ROWS][NUM_GRID_COLUMNS];
  long totalRequests = 0;

  // Last timestamp when this profiler send the stats to the control plane.
  Long lastDemandReportTimestamp = null;

  private static final Logger LOGGER = Logger.getLogger(XdnGeoDemandProfiler.class.getName());

  // Dummy const data for server locations.
  // TODO: load from config
  private final Map<String, int[]> serverLocations =
      Map.ofEntries(
          Map.entry("AR0", new int[] {0, 0}),
          Map.entry("AR1", new int[] {1, 1}),
          Map.entry("AR2", new int[] {2, 2}),
          Map.entry("AR3", new int[] {3, 3}),
          Map.entry("AR4", new int[] {4, 4}),
          Map.entry("AR5", new int[] {5, 5}),
          Map.entry("AR6", new int[] {6, 6}),
          Map.entry("AR7", new int[] {7, 7}),
          Map.entry("AR8", new int[] {8, 8}),
          Map.entry("AR9", new int[] {9, 9}));

  /**
   * @param name The name of service handled by this demand profiler.
   */
  public XdnGeoDemandProfiler(String name) {
    super(name);

    // Initialize the accumulated number of requests.
    for (int i = 0; i < NUM_GRID_ROWS; i++) {
      for (int j = 0; j < NUM_GRID_COLUMNS; j++) {
        gridTotalRequests[i][j] = 0;
      }
    }

    this.totalRequests = 0;
  }

  public XdnGeoDemandProfiler(JSONObject stats) throws JSONException {
    super(stats.getString("name"));

    if (stats.has("num_reqs")) {
      this.totalRequests = stats.getLong("num_reqs");
    }

    Object rawData = stats.opt("grid_total_reqs");
    if (rawData == null) {
      return;
    }

    // Parses and validates the provided demand data, the type could be int[][] or JSONArray.
    if (rawData instanceof int[][] data) {
      assert data.length == NUM_GRID_ROWS : "Invalid grid dimension";
      assert data[0].length == NUM_GRID_COLUMNS : "Invalid grid dimension";
      for (int i = 0; i < NUM_GRID_ROWS; i++) {
        System.arraycopy(data[i], 0, gridTotalRequests[i], 0, NUM_GRID_COLUMNS);
      }
      return;
    }
    if (rawData instanceof JSONArray data) {
      assert data.length() == NUM_GRID_ROWS : "Invalid grid dimension";
      Object firstRowData = data.opt(0);
      assert firstRowData instanceof JSONArray : "Unexpected grid data format";
      assert data.getJSONArray(0).length() == NUM_GRID_COLUMNS : "Invalid grid dimension";
      for (int i = 0; i < NUM_GRID_ROWS; i++) {
        JSONArray rowData = data.getJSONArray(i);
        for (int j = 0; j < NUM_GRID_COLUMNS; j++) {
          gridTotalRequests[i][j] = rowData.getInt(j);
        }
      }
      return;
    }

    throw new RuntimeException(
        "Unexpected type for the statistic data of "
            + rawData.getClass().getSimpleName()
            + ". Expecting int[][] or JSONArray.");
  }

  @Override
  public boolean shouldReportDemandStats(
      Request request, InetAddress sender, ReconfigurableAppInfo nodeConfig) {
    // TODO: client-ip to fake geolocation mapping.
    LOGGER.log(
        Level.FINEST,
        ">>> XdnGeoDemandProfiler - name="
            + this.name
            + " request="
            + (request != null ? request.getClass().getSimpleName() : null)
            + " sender="
            + sender);

    // Ignores requests for different service name.
    if (request == null || !request.getServiceName().equals(this.name)) {
      return false;
    }

    this.totalRequests++;
    if (lastDemandReportTimestamp == null) {
      System.out.println(">>> XdnGeoDemandProfiler - Initializing the first timestamp ....");
      this.lastDemandReportTimestamp = System.currentTimeMillis();
    }

    // TODO: Converts IP address into Geolocation, then converts it into location in our cell
    // this.gridTotalRequests[NUM_GRID_COLUMNS - 1][NUM_GRID_COLUMNS - 1]++;
    this.gridTotalRequests[5][5]++;

    long currentTimestamp = System.currentTimeMillis();
    if (currentTimestamp - this.lastDemandReportTimestamp >= MIN_DEMAND_REPORT_PERIOD_MS) {
      System.out.println(">>> trigger reconfiguration at " + currentTimestamp);
      return true;
    }

    return false;
  }

  @Override
  public JSONObject getDemandStats() {
    JSONObject stats = new JSONObject();
    try {
      stats.put("name", this.name);
      stats.put("num_reqs", this.totalRequests);
      JSONArray gridData = new JSONArray();
      for (int i = 0; i < NUM_GRID_ROWS; i++) {
        JSONArray rowData = new JSONArray();
        for (int j = 0; j < NUM_GRID_COLUMNS; j++) {
          rowData.put(this.gridTotalRequests[i][j]);
        }
        gridData.put(rowData);
      }
      stats.put("grid_total_reqs", gridData);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    return stats;
  }

  @Override
  public void combine(AbstractDemandProfile update) {
    assert update instanceof XdnGeoDemandProfiler : "Invalid profiler type";
    XdnGeoDemandProfiler incomingProfiler = (XdnGeoDemandProfiler) update;
    assert incomingProfiler.name.equals(this.name) : "Expecting profiler for the same service";
    assert incomingProfiler.gridTotalRequests.length == NUM_GRID_ROWS : "Invalid grid dimension";
    assert incomingProfiler.gridTotalRequests[0].length == NUM_GRID_COLUMNS
        : "Invalid grid dimension";

    this.totalRequests += incomingProfiler.totalRequests;

    for (int i = 0; i < NUM_GRID_ROWS; i++) {
      for (int j = 0; j < NUM_GRID_COLUMNS; j++) {
        gridTotalRequests[i][j] += incomingProfiler.gridTotalRequests[i][j];
      }
    }
  }

  @Override
  public Set<String> reconfigure(Set<String> curActives, ReconfigurableAppInfo appInfo) {
    NodeIdsMetadataPair<String> result = this.getNewActivesPlacement(curActives, appInfo);
    if (result == null) return null;
    return result.nodeIds();
  }

  @Override
  public NodeIdsMetadataPair<String> getNewActivesPlacement(
      Set<String> curActives, ReconfigurableAppInfo appInfo) {
    if (this.totalRequests == 0) {
      return null;
    }

    Map<String, InetSocketAddress> actives = appInfo.getAllActiveReplicas();
    for (Map.Entry<String, InetSocketAddress> entry : actives.entrySet()) {
      System.out.println(">>>> " + entry.getKey() + " " + entry.getValue());
    }

    // TODO: pick servers based on the demand stats
    //   - calculate centroids
    //   - pick the closest leader
    //   - pick other machines
    //   Currently we return null, which means no reconfiguration is needed.

    // Find the 3 closest servers to the centroid
    int[] centroid = this.calculateCentroid();
    Set<String> closestServers = this.findClosestServers(centroid[0], centroid[1], 3);
    System.out.println(">>> centroid " + Arrays.toString(centroid));
    System.out.println(">>> servers: " + closestServers);

    // Metadata stores the Node ID of the preferred leader
    Set<String> centroidServer = this.findClosestServers(centroid[0], centroid[1], 1);
    String preferredCoordinatorNodeId = centroidServer.iterator().next();
    JSONObject metadataJson = new JSONObject();
    try {
      metadataJson.put(Keys.PREFERRED_COORDINATOR.toString(), preferredCoordinatorNodeId);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    String metadata = metadataJson.toString();
    LOGGER.log(Level.FINE, "metadata: " + metadata);

    return new NodeIdsMetadataPair<>(closestServers, metadata);
  }

  private int[] calculateCentroid() {
    // Iterate over the matrix to calculate the weighted sums
    var matrix = this.gridTotalRequests;
    double totalWeight = 0;
    double weightedRowSum = 0;
    double weightedColSum = 0;
    for (int row = 0; row < matrix.length; row++) {
      for (int col = 0; col < matrix[row].length; col++) {
        int weight = matrix[row][col];
        totalWeight += weight;
        weightedRowSum += row * weight;
        weightedColSum += col * weight;
      }
    }

    // Handle edge case: if total weight is zero, return the center of the matrix
    if (totalWeight == 0) {
      return new int[] {0, 0};
    }

    // Calculate the centroid as weighted averages
    double centroidRow = weightedRowSum / totalWeight;
    double centroidCol = weightedColSum / totalWeight;

    return new int[] {(int) centroidRow, (int) centroidCol};
  }

  private Set<String> findClosestServers(int row, int col, int numClosestServers) {
    record ServerDistance(String id, double distance) {}
    ;
    List<ServerDistance> servers = new ArrayList<>();

    for (Map.Entry<String, int[]> entry : this.serverLocations.entrySet()) {
      int serverRow = entry.getValue()[0];
      int serverCol = entry.getValue()[1];
      String serverId = entry.getKey();

      double distance = Math.sqrt(Math.pow(col - serverCol, 2) + Math.pow(row - serverRow, 2));

      servers.add(new ServerDistance(serverId, distance));
    }

    servers.sort(Comparator.comparingDouble(s -> s.distance));

    Set<String> result = new HashSet<>();
    for (int i = 0; i < numClosestServers; i++) {
      result.add(servers.get(i).id);
    }
    return result;
  }

  // TODO: update reconfigure to also compute metadata (e.g., which node should be the leader).

  @Override
  public void justReconfigured() {
    // do nothing
  }
}
