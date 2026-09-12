# Chained Redis replication for XDN cluster mode.
#
# Ordinal 0 is the master; every other ordinal replicates from its predecessor
# (replica-N replicates replica-(N-1)), forming a linear chain. Writes go to
# the head and relay hop by hop, so the coordination graph is a path — the
# chain-replication *topology* signature — while the replication semantics stay
# Redis's asynchronous propagation (not CRAQ-style strong chain replication).
#
# Build: docker build -t xdn-redis-chain:test services/redis-chain/
FROM redis:7-alpine
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
