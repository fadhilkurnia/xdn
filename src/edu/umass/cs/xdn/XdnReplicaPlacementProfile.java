package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import java.net.InetAddress;
import java.util.Set;
import org.json.JSONObject;

// TODO: implement me!!!
//  - keep an in-memory data, mapping IP prefixes to demand
//  - for every 5 seconds, send demand profile to the Reconfigurator.
//  - reconfigure

public class XdnReplicaPlacementProfile extends AbstractDemandProfile {

  /**
   * @param name The service name of the reconfiguree replica group.
   */
  public XdnReplicaPlacementProfile(String name) {
    super(name);
  }

  @Override
  public boolean shouldReportDemandStats(
      Request request, InetAddress sender, ReconfigurableAppInfo nodeConfig) {
    return false;
  }

  @Override
  public JSONObject getDemandStats() {
    return null;
  }

  @Override
  public void combine(AbstractDemandProfile update) {}

  @Override
  public Set<String> reconfigure(Set<String> curActives, ReconfigurableAppInfo appInfo) {
    return null;
  }

  @Override
  public void justReconfigured() {}
}
