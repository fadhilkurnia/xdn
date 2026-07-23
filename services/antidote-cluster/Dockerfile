# Self-clustering AntidoteDB for XDN cluster mode: CRDT store with causal+
# consistency. Each replica runs as an independent antidote data center;
# writes commit locally with ZERO synchronous coordination and replicate
# asynchronously to peer DCs over ZeroMQ pub/sub (ports 8086 pubsub, 8085
# log reader) — the eventual-consistency extreme of the pattern space.
#
# The entrypoint pins the Erlang node name to the overlay IP (resolved from
# the replica-N alias) so peer DCs dial a reachable address. DC linking is a
# post-start step driven from outside via `bin/antidote eval`: export each
# node's connection descriptor (base64-encoded term) and have every node
# subscribe_updates_from the other DCs' descriptors. See the antidote driver
# in eval/measure_cluster_bw.py for the load side.
#
# Build: docker build -t xdn-antidote-cluster:test services/antidote-cluster/
FROM antidotedb/antidote:latest
COPY entrypoint.sh /xdn-entrypoint.sh
COPY xdnselflink.escript /xdnselflink.escript
# XDN starts cluster containers under a non-root --user; the relx start
# script substitutes env vars into vm.args/sys.config inside the release
# dir at boot, so the whole tree must be writable.
RUN chmod +x /xdn-entrypoint.sh /xdnselflink.escript && chmod -R a+w /antidote
ENTRYPOINT ["/xdn-entrypoint.sh"]
