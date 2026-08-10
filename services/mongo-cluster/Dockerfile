# Self-clustering MongoDB replica set for XDN cluster mode: single-primary
# replication where secondaries PULL the oplog from their sync source (vs
# the push in Raft/Group Replication), acks trickle back as
# replSetUpdatePosition, and a secondary may chain-sync from another
# secondary — a distinct wire signature among the leader-based protocols.
#
# Ordinal 0 initiates the replica set with members addressed by the
# replica-N overlay aliases once every member's mongod answers. No keyfile
# auth: this is a measurement reference image.
#
# Build: docker build -t xdn-mongo-cluster:test services/mongo-cluster/
FROM mongo:7
COPY entrypoint.sh /xdn-entrypoint.sh
RUN chmod +x /xdn-entrypoint.sh
ENTRYPOINT ["/xdn-entrypoint.sh"]
