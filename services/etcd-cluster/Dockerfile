# Thin wrapper around upstream etcd that maps the XDN_CLUSTER_* environment contract
# (injected by xdn cluster launch) onto etcd's own ETCD_* flags. XDN provides identity
# and networking; etcd handles its own consensus inside.
#
# The upstream `quay.io/coreos/etcd` image is distroless (no /bin/sh), so this is a
# two-stage build: copy the etcd + etcdctl binaries onto an alpine base that ships a
# shell our entrypoint can exec.
#
# Build locally before running the integration test:
#   docker build -t xdn-etcd-cluster:test services/etcd-cluster/
FROM quay.io/coreos/etcd:v3.5.10 AS etcd-source

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=etcd-source /usr/local/bin/etcd /usr/local/bin/etcd
COPY --from=etcd-source /usr/local/bin/etcdctl /usr/local/bin/etcdctl
COPY entrypoint.sh /xdn-entrypoint.sh
RUN chmod +x /xdn-entrypoint.sh
ENTRYPOINT ["/xdn-entrypoint.sh"]
