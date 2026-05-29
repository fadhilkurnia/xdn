# Thin wrapper around upstream rqlite that maps the XDN_CLUSTER_* environment contract
# (injected by xdn cluster launch) onto rqlite's own -node-id / -join / -bootstrap-expect
# flags. XDN provides identity + overlay networking; rqlite handles its own Raft.
#
# Build locally before launching:
#   docker build -t xdn-rqlite-cluster:test services/rqlite-cluster/
FROM rqlite/rqlite:8.43.4 AS rqlite-source

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=rqlite-source /bin/rqlited /usr/local/bin/rqlited
COPY --from=rqlite-source /bin/rqlite /usr/local/bin/rqlite
COPY entrypoint.sh /xdn-entrypoint.sh
RUN chmod +x /xdn-entrypoint.sh
ENTRYPOINT ["/xdn-entrypoint.sh"]
