# Self-clustering MySQL for XDN cluster-mode services. Wraps the stock mysql:8.0 image and
# maps the XDN_CLUSTER_* contract (injected by `xdn cluster launch`) onto MySQL 8 Group
# Replication: a single-primary group whose members find each other over the swarm overlay
# by their stable replica-N hostnames. XDN provides identity + networking; MySQL runs its
# own consensus inside.
#
# Pinned to 8.0 (not 8.4) deliberately: Group Replication's distributed-recovery auth path
# is the best-trodden on 8.0, which keeps this CI image robust.
#
# Build locally before running the integration test:
#   docker build -t xdn-mysql-cluster:test services/mysql-cluster/
FROM mysql:8.0
COPY entrypoint.sh /xdn-entrypoint.sh
# XDN starts cluster containers with --user=<host-uid>:<gid> (not root), so the entrypoint and
# mysqld run as an arbitrary uid. Make the paths they must write — the config dir and the runtime
# socket/pid dir — writable by any uid. (The datadir is a host bind-mount, already host-owned.)
RUN chmod +x /xdn-entrypoint.sh \
 && mkdir -p /var/run/mysqld \
 && chmod 0777 /etc/mysql/conf.d /var/run/mysqld
# GR group-communication port (XDN_CLUSTER_PEER_PORT) defaults to 33061; the client/app
# port stays 3306. The entrypoint configures and starts mysqld itself.
ENTRYPOINT ["/xdn-entrypoint.sh"]
CMD ["mysqld"]
