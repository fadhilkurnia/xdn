# Thin wrapper around upstream etcd that maps the XDN_CLUSTER_* environment contract
# (injected by xdn cluster launch) onto etcd's own ETCD_* flags. XDN provides identity
# and networking; etcd handles its own consensus inside.
#
# Build locally before running the integration test:
#   docker build -t xdn-etcd-cluster:test services/etcd-cluster/
FROM quay.io/coreos/etcd:v3.5.10

COPY entrypoint.sh /xdn-entrypoint.sh

# The base image runs as root and ships sh; no extra packages needed.
ENTRYPOINT ["/xdn-entrypoint.sh"]
