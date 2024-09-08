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
 * TODO: use this in the Reconfigorator
 */
public interface InitialStateValidator {

    /**
     * validateInitialState validates the given initialState provided to create a Replica Group.
     * The method must throw InvalidInitialStateException when the provided initialState is
     * invalid. Otherwise, the initialState is assumed to be correct and will be used to create a
     * Replica Group. Doing the validation before Replica Group creation is preferred so all
     * Replicas will be successfully instantiated.
     *
     * <p> Note that the validation *must* be deterministic since it will be executed independently
     * in all replicas and must yield the same result.
     *
     * @param initialState the initialState provided to start Replicable instance
     * @throws InvalidInitialStateException when the given initialState is invalid
     */
    void validateInitialState(String initialState) throws InvalidInitialStateException;

    class InvalidInitialStateException extends Exception {
        public InvalidInitialStateException(String message) {
            super(message);
        }
    }
}
