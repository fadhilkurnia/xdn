# Self-clustering Cassandra for XDN cluster mode: leaderless Dynamo-style
# quorum replication — no stable coordinator, every write fans out from the
# request's coordinator node to the replica set, gossip among all members.
#
# The entrypoint maps XDN_CLUSTER_* onto Cassandra's config: the overlay IP
# (resolved from the replica-N alias via Docker's embedded DNS, which is the
# address peers can actually dial) becomes listen/broadcast address, and
# replica-0 serves as the seed.
#
# Build: docker build -t xdn-cassandra-cluster:test services/cassandra-cluster/
FROM cassandra:4.1
COPY entrypoint.sh /xdn-entrypoint.sh
RUN chmod +x /xdn-entrypoint.sh
ENTRYPOINT ["/xdn-entrypoint.sh"]
