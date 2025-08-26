package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.Set;

/**
 * Pairs a set of Node IDs with String metadata. One use case of this record is for
 * reconfiguration where the metadata stores ID of the preferable coordinator among the IDs
 * in the Node IDs set.
 *
 * @param nodeIds           The set of Node IDs
 * @param placementMetadata String metadata for replica placement
 *                          (e.g., which node is preferred as coordinator)
 */
public record NodeIdsMetadataPair<NodeIdType>(Set<NodeIdType> nodeIds, String placementMetadata) {
}
