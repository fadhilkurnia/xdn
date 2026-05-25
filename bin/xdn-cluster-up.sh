#!/bin/bash
# xdn-cluster-up.sh
#
# Brings up the 1-RC-+-3-AR XDN cluster on the CloudLab pod (10.10.1.1-4) and
# (optionally) launches a demo cluster service from the driver (10.10.1.5).
#
# Prerequisites — already done on this pod, only restate if rebuilding from
# zero:
#   - rsync'd repo at /users/fadhil/xdn-cl/ on each of .1/.2/.3/.4
#   - Docker swarm initialized across .1-.4 (`bin/xdnd dist-init` does this)
#   - xdn-etcd-cluster:test and xdn-rqlite-cluster:test images present
#     on .1/.2/.3/.4 (`docker build` in services/<name>/ on each)
#   - bookcatalog image pulled on each AR (`docker pull fadhilkurnia/xdn-bookcatalog`)
#
# Usage (from the driver at 10.10.1.5):
#   bin/xdn-cluster-up.sh                       # just start XDN, no service
#   bin/xdn-cluster-up.sh --launch-etcd         # also launch a 3-node etcd
#   bin/xdn-cluster-up.sh --launch-bookcat      # also launch bookcatalog+rqlite

set -euo pipefail

REMOTE=/users/fadhil/xdn-cl
CFG=conf/gigapaxos.xdn.cluster-launch.cloudlab.properties
JF='-ea -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore.jks -Djavax.net.ssl.trustStore=conf/trustStore.jks -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig='"$CFG"' -Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED'

echo "[1/3] starting RC (node 0) on 10.10.1.1 and ARs (nodes 1,2,3) on .2/.3/.4 ..."
for pair in "10.10.1.1 0" "10.10.1.2 1" "10.10.1.3 2" "10.10.1.4 3"; do
  read ip nid <<<"$pair"
  ssh -fn -o BatchMode=yes "$ip" "
    cd $REMOTE && mkdir -p logs
    nohup java $JF -cp \$(ls jars/*.jar | tr '\n' ':') \
      edu.umass.cs.reconfiguration.ReconfigurableNode $nid \
      > logs/node-$nid.log 2> logs/node-$nid.err < /dev/null &
  "
done

echo "[2/3] waiting for HTTP frontends ..."
for pair in "10.10.1.1 3300" "10.10.1.2 2300" "10.10.1.3 2300" "10.10.1.4 2300"; do
  read ip port <<<"$pair"
  until timeout 1 bash -c "</dev/tcp/$ip/$port" 2>/dev/null; do sleep 2; done
  echo "  $ip:$port up"
done

echo "[3/3] XDN cluster ready. Set XDN_CONTROL_PLANE=10.10.1.1 to talk to it."

case "${1:-}" in
  --launch-etcd)
    echo
    echo "→ launching etcd-demo (3 replicas)"
    XDN_CONTROL_PLANE=10.10.1.1 ./bin/xdn cluster launch etcd-demo \
        --image xdn-etcd-cluster:test \
        --port 2379 --peer-port 2380 \
        --state /etcd-data/ --num-replicas 3
    ;;
  --launch-bookcat)
    echo
    echo "→ launching bookcat (rqlite cluster member + bookcatalog sidecar)"
    XDN_CONTROL_PLANE=10.10.1.1 ./bin/xdn cluster launch bookcat \
        -f services/bookcatalog-rqlite-cluster.yaml
    echo
    echo "Note: bookcatalog can race rqlite startup. If 'curl … /api/books' returns"
    echo "'don't have any cluster info', restart the sidecars once rqlite is healthy:"
    echo "  for ip in 10.10.1.2 10.10.1.3 10.10.1.4; do"
    echo "    nid=\$((\${ip##*.} - 1))"
    echo "    ssh \$ip \"sudo docker restart c1.e0.bookcat.\$nid.xdn.io\""
    echo "  done"
    ;;
esac
