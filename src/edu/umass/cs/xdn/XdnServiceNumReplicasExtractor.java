/*
 * Copyright (c) 2023 University of Massachusetts
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
 *
 * Initial developer(s): Fadhil Kurnia
 */

package edu.umass.cs.xdn;

import edu.umass.cs.reconfiguration.interfaces.InitialStateNumReplicasExtractor;
import edu.umass.cs.xdn.service.ServiceProperty;
import org.json.JSONException;

/**
 * Extracts a per-service replica-count override from the XDN-specific initial state format
 * ("xdn:init:<json>"). Returns {@code null} when the initial state is absent, malformed, or does
 * not carry a {@code num_replicas} key, so the Reconfigurator will fall back to its default
 * placement.
 */
public class XdnServiceNumReplicasExtractor implements InitialStateNumReplicasExtractor {

  @Override
  public Integer extract(String initialState) {
    if (initialState == null || initialState.isEmpty()) {
      return null;
    }
    String prefix = ServiceProperty.XDN_INITIAL_STATE_PREFIX;
    if (!initialState.startsWith(prefix)) {
      return null;
    }
    String json = initialState.substring(prefix.length());
    try {
      ServiceProperty property = ServiceProperty.createFromJsonString(json);
      return property.getNumReplicas();
    } catch (JSONException | RuntimeException e) {
      // Validation happens earlier via InitialStateValidator; return null here
      // so a malformed body still hits the default placement path and any
      // subsequent validator produces the authoritative error.
      return null;
    }
  }
}
