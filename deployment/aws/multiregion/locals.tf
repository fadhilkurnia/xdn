data "aws_caller_identity" "current" {}

locals {
  control_region = "us-east-1"

  # Persistent TLS cert store -- a bucket created OUTSIDE this config
  # (../../../bin/persist-cert.sh) so it survives `terraform destroy`. RC + ARs always
  # pull the wildcard cert from here (it lives in us-east-1).
  persist_tls_bucket     = "xdn-tls-persist-${data.aws_caller_identity.current.account_id}"
  persist_tls_bucket_arn = "arn:aws:s3:::xdn-tls-persist-${data.aws_caller_identity.current.account_id}"

  # ---- Topology -----------------------------------------------------------
  # The ActiveReplicas and the region/AZ each one physically lives in, plus its
  # real geolocation (surfaced in /placement and on the dashboard map). The RC
  # lives in control_region. To move/add an AR, edit this map (and ensure its
  # region has a provider alias in providers.tf + a region block in network.tf
  # and compute.tf).
  # Node ids are dash-free (us-east-1a -> useast1a): a '-' breaks the embedded
  # Derby paxos log (and is now rejected by ReconfigurableNode). The real AWS AZ
  # name (with dashes) is kept in `az`; the geolocation drives the dashboard map.
  replicas = {
    "useast1a" = { region = "us-east-1", az = "us-east-1a", geo = "39.04,-77.49" }  # N. Virginia   -- East
    "useast1b" = { region = "us-east-1", az = "us-east-1b", geo = "38.90,-77.43" }  # N. Virginia   -- East
    "useast2a" = { region = "us-east-2", az = "us-east-2a", geo = "40.10,-82.99" }  # Columbus, OH  -- Central
    "uswest2a" = { region = "us-west-2", az = "us-west-2a", geo = "45.84,-119.69" } # Boardman, OR  -- West
  }

  # AR ids grouped by region (drives the per-region subnet/instance for_each).
  ar_ids_use1 = sort([for id, r in local.replicas : id if r.region == "us-east-1"])
  ar_ids_use2 = sort([for id, r in local.replicas : id if r.region == "us-east-2"])
  ar_ids_usw2 = sort([for id, r in local.replicas : id if r.region == "us-west-2"])

  # ---- Static GLOBAL IPv6 per node (host ::10 of each subnet's /64) --------
  # Bindable (on the ENI) AND globally routable, so gigapaxos advertises + binds
  # it for consensus across regions. Derived from the subnet (known after the VPC
  # gets its AWS-assigned IPv6 block), so no dependency cycle with user_data.
  rc_ipv6 = cidrhost(aws_subnet.rc.ipv6_cidr_block, 10)
  ar_ipv6s = merge(
    { for id, s in aws_subnet.ar_use1 : id => cidrhost(s.ipv6_cidr_block, 10) },
    { for id, s in aws_subnet.ar_use2 : id => cidrhost(s.ipv6_cidr_block, 10) },
    { for id, s in aws_subnet.ar_usw2 : id => cidrhost(s.ipv6_cidr_block, 10) },
  )

  # Every cluster VPC's IPv6 /56. Consensus ports are opened only to these ranges
  # (not the open internet), even though they span regions/the public IPv6 net.
  cluster_ipv6_cidrs = [
    aws_vpc.use1.ipv6_cidr_block,
    aws_vpc.use2.ipv6_cidr_block,
    aws_vpc.usw2.ipv6_cidr_block,
  ]

  # Corefile `edge <nodeid> <ipv6>` lines (8-space indented to nest in the xdn
  # directive block) so coredns answers <nodeid>.edge.<domain> with that replica's
  # IPv6. Drives the per-replica browser-clickable links (cert: *.edge.<domain>).
  edge_node_props = join("\n", [
    for id, ip in local.ar_ipv6s : "        edge ${id} ${ip}"
  ])

  # ---- Shared security-group ingress --------------------------------------
  # Identical in every region, so defined once and rendered via a dynamic block.
  # Data-plane / management ports are open to the internet (v4+v6); the catch-all
  # consensus rule is restricted to the cluster IPv6 ranges.
  sg_ingress = [
    { desc = "SSH (IPv4)", from = 22, to = 22, proto = "tcp", v4 = ["0.0.0.0/0"], v6 = [] },
    { desc = "SSH (IPv6)", from = 22, to = 22, proto = "tcp", v4 = [], v6 = ["::/0"] },
    { desc = "DNS (UDP) to coredns", from = 53, to = 53, proto = "udp", v4 = ["0.0.0.0/0"], v6 = ["::/0"] },
    { desc = "DNS (TCP) to coredns", from = 53, to = 53, proto = "tcp", v4 = ["0.0.0.0/0"], v6 = ["::/0"] },
    { desc = "AR data-plane HTTP (:80, Caddy 301)", from = 80, to = 80, proto = "tcp", v4 = ["0.0.0.0/0"], v6 = ["::/0"] },
    { desc = "AR data-plane HTTPS (:443)", from = 443, to = 443, proto = "tcp", v4 = ["0.0.0.0/0"], v6 = ["::/0"] },
    { desc = "AR per-replica HTTP frontend (:2300, xdn service info)", from = 2300, to = 2300, proto = "tcp", v4 = ["0.0.0.0/0"], v6 = ["::/0"] },
    { desc = "RC control-plane API (HTTP :3300)", from = 3300, to = 3300, proto = "tcp", v4 = ["0.0.0.0/0"], v6 = ["::/0"] },
    { desc = "RC control-plane API (HTTPS :3400, dashboard)", from = 3400, to = 3400, proto = "tcp", v4 = ["0.0.0.0/0"], v6 = ["::/0"] },
    # Intra-cluster consensus (:2000/:3000 + offset ports) over IPv6, restricted
    # to the cluster's own VPC IPv6 ranges across all regions.
    { desc = "Intra-cluster consensus (cluster IPv6 only)", from = 0, to = 0, proto = "-1", v4 = [], v6 = local.cluster_ipv6_cidrs },
  ]

  # ---- Candidate placement locations (dashboard map; config-only, $0) ------
  # Dash-free node ids (the no-dash rule covers these geolocation-only candidates
  # too; one may later be promoted to a real node, where a dash breaks Derby). The
  # real AWS AZ/Local-Zone name is in the trailing comment.
  candidate_geolocations = {
    "useast1c"      = "38.87,-77.40"  # us-east-1c
    "useast1d"      = "39.10,-77.54"  # us-east-1d
    "useast2b"      = "40.08,-83.09"  # us-east-2b
    "useast2c"      = "39.90,-82.89"  # us-east-2c
    "uswest1a"      = "37.44,-122.00" # us-west-1a
    "uswest1b"      = "37.31,-121.85" # us-west-1b
    "uswest2b"      = "45.77,-119.59" # us-west-2b
    "uswest2c"      = "45.91,-119.81" # us-west-2c
    "lzatlanta"     = "33.75,-84.39"  # lz-atlanta
    "lzboston"      = "42.36,-71.06"  # lz-boston
    "lzchicago"     = "41.85,-87.65"  # lz-chicago
    "lzdallas"      = "32.78,-96.80"  # lz-dallas
    "lzdenver"      = "39.74,-104.99" # lz-denver
    "lzhouston"     = "29.76,-95.37"  # lz-houston
    "lzlasvegas"    = "36.17,-115.14" # lz-las-vegas
    "lzlosangeles"  = "34.05,-118.24" # lz-los-angeles
    "lzmiami"       = "25.76,-80.19"  # lz-miami
    "lzminneapolis" = "44.98,-93.27"  # lz-minneapolis
    "lznewyork"     = "40.71,-74.01"  # lz-new-york
    "lzphoenix"     = "33.45,-112.07" # lz-phoenix
    "lzportland"    = "45.52,-122.68" # lz-portland
    "lzseattle"     = "47.61,-122.33" # lz-seattle
  }
  candidate_geo_props = join("\n", [
    for id, ll in local.candidate_geolocations : "active.${id}.geolocation=\"${ll}\""
  ])

  # ---- Shared gigapaxos cluster config (handed to every node) --------------
  # Node ids are AWS-zone strings (cp0, us-east-1a, ...). Nodes advertise their
  # GLOBAL IPv6 for consensus; coredns returns those IPv6s as AAAA for the data
  # plane. Same flags as the single-region config.
  gigapaxos_properties = join("\n", concat([
    "APPLICATION=edu.umass.cs.xdn.XdnGigapaxosApp",
    "REPLICA_COORDINATOR_CLASS=edu.umass.cs.xdn.XdnReplicaCoordinator",
    "INITIAL_STATE_VALIDATOR_CLASS=edu.umass.cs.xdn.XdnServiceInitialStateValidator",
    "GIGAPAXOS_DATA_DIR=/tmp/gigapaxos",
    "NIO_MAX_PAYLOAD_SIZE=134217728",
    "BYTEIFY_NON_INT_NODE_IDS=true",
    # How long a deleted service's final epoch state is retained (ms). The gigapaxos default is 1h,
    # which also blocks reusing a deleted service NAME for that whole hour -- a re-create reuses
    # epoch 0 and the AR refuses to start it while the old epoch-0 final checkpoint lingers, so the
    # create hangs. 60s is ample for XDN's seconds-long reconfigurations and lets delete->recreate
    # of the same name work almost immediately.
    "MAX_FINAL_STATE_AGE=60000",
    # Embedded SQLite backend (vs the default Embedded Derby + C3P0 pool) for the paxos logs
    # (RC + AR) and the reconfigurator DB (RC). ~25% lower RSS and ~72 fewer JVM threads (no
    # C3P0 helper threads) -> lets the RC run on a smaller machine (t4g.nano). SQL_TYPE is shared
    # by paxos + reconfiguration. CONNECTION_POOLING=false uses the thread-free SimpleDataSource;
    # DB_MAX_CONNECTIONS stays at its default 0 (a finite cap deadlocks gigapaxos -- see
    # docs/sqlite-backend.md). The sqlite-jdbc driver is bundled into the gigapaxos fat jar.
    "SQL_TYPE=EMBEDDED_SQLITE",
    "CONNECTION_POOLING=false",
    # Place a new service at DEFAULT_NUM_REPLICAS actives (consistent-hash-chosen),
    # not at every active (the gigapaxos library default REPLICATE_ALL=true). Matches
    # conf/gigapaxos.xdn.*; the geo-demand policy re-places replicas after creation.
    "REPLICATE_ALL=false",
    "DEFAULT_NUM_REPLICAS=3",
    # Primary-backup non-deterministic init-sync mode. RECORDER ships the primary's bootstrap
    # state in-band as the first paxos-ordered ApplyStateDiff (atomic, no rsync seam / inter-node
    # SSH). Validated end-to-end; recorder defaults to FUSELOG and non-det-init defaults on.
    # NOTE: RECORDER funnels the whole init state through the JVM/paxos heap, so it OOMs the
    # small-heap ARs for LARGE initial state (~64MB) -- such services must stay on RSYNC.
    "XDN_PB_INIT_SYNC_MODE=RECORDER",
    "DEMAND_PROFILE_TYPE=edu.umass.cs.xdn.XdnGeoDemandProfiler",
    # Rolling geo-demand window in MINUTES (-1 = cumulative all-time; a positive value windows the
    # demand so the heatmap and placement reflect only the last N minutes of load). Set to -1:
    # windowing disabled, so XdnGeoDemandProfiler aggregates demand cumulatively (read/write split
    # is always on regardless).
    "XDN_DEMAND_WINDOW_MINUTES=-1",
    "ENABLE_ACTIVE_REPLICA_HTTP=true",
    "ENABLE_ACTIVE_REPLICA_HTTP_PORT_80=false",
    "ENABLE_RECONFIGURATOR_HTTP=true",
    "reconfigurator.cp0=[${local.rc_ipv6}]:3000",
    ], [
    for id, r in local.replicas : "active.${id}=[${local.ar_ipv6s[id]}]:2000"
    ], [
    for id, r in local.replicas : "active.${id}.geolocation=\"${r.geo}\""
  ]))
}
