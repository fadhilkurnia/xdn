# Self-clustering CorfuDB for XDN cluster mode: the canonical CHAIN
# REPLICATION protocol (van Renesse) — but note Corfu's chain is
# CLIENT-DRIVEN: the client library writes to each log unit in chain order
# itself, so replica-to-replica traffic is management/layout plane only
# while the data path fans out from the client. The measured signature is
# the instructive inverse of a relay chain (cf. services/redis-chain).
#
# Members run in memory mode (-m): no log path, so nothing fights XDN's
# non-root --user over file permissions (the stock image's JAVA_OPTS gc-log
# paths under /var/log/corfu are also overridden for the same reason).
#
# Bootstrap is a post-start step: push a layout JSON listing all
# replica-N:<port> endpoints (layoutServers = sequencers = one
# CHAIN_REPLICATION segment) into any member and run
# `corfu_bootstrap_cluster -l <layout.json> --connection-timeout 5000`.
#
# Build: docker build -t xdn-corfu-cluster:test services/corfu-cluster/
FROM corfudb/corfu-server:0.9.2.0-SNAPSHOT
COPY entrypoint.sh /xdn-entrypoint.sh
RUN chmod +x /xdn-entrypoint.sh && \
    mkdir -p /var/log/corfu /image/corfu-server/temp && \
    chmod -R a+w /var/log/corfu /image
ENTRYPOINT ["/xdn-entrypoint.sh"]
