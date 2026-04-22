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

package edu.umass.cs.reconfiguration.interfaces;

/**
 * Optional hook that lets the application layer override the default initial
 * replica group size for a create-service request based on the raw initial
 * state bytes. Returning {@code null} means "no override" and the Reconfigurator
 * falls back to its default placement using DEFAULT_NUM_REPLICAS.
 *
 * <p>This interface keeps the Reconfigurator itself application-agnostic: XDN
 * (or another application) supplies an implementation that parses its own
 * initial-state format.
 */
public interface InitialStateNumReplicasExtractor {

    /**
     * @param initialState the raw initial state string from the create-service request
     * @return the desired number of replicas, or {@code null} if the initial state
     *         does not specify an override
     */
    Integer extract(String initialState);
}
