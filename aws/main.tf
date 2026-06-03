terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Obtains the wildcard cert ONCE via ACME DNS-01 and keeps it in TF state,
    # so AR replacements reuse it (no re-issuance -> no LE rate-limit burn).
    acme = {
      source  = "vancluever/acme"
      version = "~> 2.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

provider "acme" {
  # Directory endpoint (var.acme_ca): LE production by default, staging while
  # testing. Same var the rest of the config uses.
  server_url = var.acme_ca
}

# 1. Fetch available zones (filtering out the older zone 'e')
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "zone-name"
    values = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1f"]
  }
}

# 2. Create a Custom VPC with IPv6 Enabled
resource "aws_vpc" "ipv6_vpc" {
  cidr_block                       = "10.0.0.0/16"
  assign_generated_ipv6_cidr_block = true

  tags = { Name = "VPC-DualStack" }
}

# 3. Create an Internet Gateway (Handles both IPv4 and IPv6 traffic)
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.ipv6_vpc.id
  tags   = { Name = "VPC-IGW" }
}

# 4. Create 4 separate subnets (one for each zone, enabling BOTH IPv4 and IPv6)
resource "aws_subnet" "ipv6_subnets" {
  count             = 4
  vpc_id            = aws_vpc.ipv6_vpc.id
  cidr_block        = "10.0.${count.index + 1}.0/24"
  availability_zone = element(data.aws_availability_zones.available.names, count.index)
  ipv6_cidr_block   = cidrsubnet(aws_vpc.ipv6_vpc.ipv6_cidr_block, 8, count.index + 1)

  # Enabling both ensures your Mac can connect over standard IPv4
  map_public_ip_on_launch         = true
  assign_ipv6_address_on_creation = true

  tags = {
    Name = "Subnet-${element(data.aws_availability_zones.available.names, count.index)}"
  }
}

# 5. Route Table (Routes both IPv4 and IPv6 out to the internet)
resource "aws_route_table" "routes" {
  vpc_id = aws_vpc.ipv6_vpc.id

  # Route for IPv4 traffic
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  # Route for IPv6 traffic
  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.igw.id
  }

  tags = { Name = "VPC-Routes" }
}

# 6. Link the route table to all 4 subnets
resource "aws_route_table_association" "a" {
  count          = 4
  subnet_id      = aws_subnet.ipv6_subnets[count.index].id
  route_table_id = aws_route_table.routes.id
}

# 7. Security Group (Allows incoming SSH via IPv4/IPv6, and open internal traffic)
resource "aws_security_group" "allow_ssh_ipv6" {
  name        = "allow_ssh_dualstack"
  description = "Allow SSH traffic and internal communication"
  vpc_id      = aws_vpc.ipv6_vpc.id

  # Rule A: Allow SSH from standard IPv4 internet
  ingress {
    description = "SSH via IPv4"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Rule B: Allow SSH from IPv6 internet
  ingress {
    description      = "SSH via IPv6"
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    ipv6_cidr_blocks = ["::/0"]
  }

  # Rule B2: Allow DNS queries to coredns on the RC (UDP + TCP)
  ingress {
    description      = "DNS via UDP"
    from_port        = 53
    to_port          = 53
    protocol         = "udp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  ingress {
    description      = "DNS via TCP"
    from_port        = 53
    to_port          = 53
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Rule B3: AR data-plane HTTP frontend (clients reach services on :80)
  ingress {
    description      = "HTTP service traffic to ActiveReplicas"
    from_port        = 80
    to_port          = 80
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Rule B3b: AR data-plane HTTPS frontend. Caddy on each AR terminates TLS for
  #           <service>.xdnapp.com (wildcard *.xdnapp.com cert from Let's Encrypt)
  #           and reverse-proxies to the local XDN HTTP frontend. The :80 rule
  #           above stays open so Caddy can 301-redirect HTTP->HTTPS.
  ingress {
    description      = "HTTPS service traffic to ActiveReplicas"
    from_port        = 443
    to_port          = 443
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Rule B3c: AR data-plane HTTP frontend on its offset port (2300, since
  #           ENABLE_ACTIVE_REPLICA_HTTP_PORT_80=false). Clients normally reach
  #           services via Caddy on :443, but `xdn-cli service info` and direct
  #           per-replica addressing probe each replica's frontend BY IP at the
  #           HTTP_ADDRESS port the control plane advertises (this port). Without
  #           this rule those probes time out from outside the cluster and every
  #           replica shows "unreachable". This is plaintext (same exposure class
  #           as the legacy :80 frontend); the geo-DNS service URL stays on TLS.
  ingress {
    description      = "XDN per-replica HTTP frontend (xdn-cli service info / direct replica access)"
    from_port        = 2300
    to_port          = 2300
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Rule B4: RC control-plane API, plaintext (xdn-cli, coredns geo-DNS, and
  #          trace_bw contact cp.xdnapp.com:3300 over HTTP).
  ingress {
    description      = "XDN control-plane API on the RC (HTTP)"
    from_port        = 3300
    to_port          = 3300
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Rule B4b: RC control-plane API over TLS on the SSL port (HTTP_PORT_SSL_OFFSET
  #           => 3400). HttpReconfigurator terminates TLS here with the wildcard
  #           cert so the browser dashboard (HTTPS) can reach the control plane
  #           without mixed-content blocking. The plaintext :3300 above stays for
  #           the existing HTTP clients.
  ingress {
    description      = "XDN control-plane API on the RC (HTTPS, for the dashboard)"
    from_port        = 3400
    to_port          = 3400
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # (Former Rule B5 -- intra-cluster consensus over public IPv4 EIPs -- removed:
  #  consensus now flows over IPv6 (Rule B6), and the ARs no longer have public
  #  IPv4. The RC keeps its EIP for management/DNS only.)

  # Rule B6: intra-cluster consensus over IPv6. gigapaxos now advertises the
  #          nodes' GLOBAL IPv6 addresses, so consensus (:2000/:3000) and the
  #          client-facing offset ports flow node->node over IPv6. Allow all
  #          ports from the VPC's own IPv6 range (only the cluster nodes live
  #          there) -- keeps consensus off the open internet while letting
  #          members reach each other over IPv6.
  ingress {
    description      = "Intra-cluster consensus over IPv6 (VPC range only)"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    ipv6_cidr_blocks = [aws_vpc.ipv6_vpc.ipv6_cidr_block]
  }

  # Rule C: Allow all 4 servers to talk to each other on any port (private path)
  ingress {
    description = "Allow all internal VPC traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

# 8. Link your existing local public key.
#    This key pair was imported into state (it was created out-of-band to build
#    the AMIs). The AWS API does not return public_key material on import, so a
#    naive plan would force-replace the key pair; ignore_changes avoids that
#    churn since the key material is stable.
resource "aws_key_pair" "deployer" {
  key_name   = "xdn-aws-key"
  public_key = file("~/.ssh/xdn-aws-key.pub")

  lifecycle {
    ignore_changes = [public_key]
  }
}

# 9. Per-role AMIs (built from the 'main' branch by create_rc_ami.sh /
#    create_ar_ami.sh; see last-built-amis.txt). Override on the CLI if rebuilt.
variable "rc_ami" {
  description = "AMI for the Reconfigurator (RC) + coredns node (includes docker-ce-cli for CREATE-time image validation). ARM64/Graviton image (for t4g.* rc_instance_type), built from fork/main by `ARCH=arm64 ./create_rc_ami.sh`."
  type        = string
  default     = "ami-05421989023a115de" # arm64, + control-plane HTTPS/CORS jar (dashboard phase 0)
}

variable "ar_ami" {
  description = "AMI for the ActiveReplica (AR) edge nodes. ARM64/Graviton image (for t4g.* ar_instance_type), built from the xdn-ar-arm64 branch by `ARCH=arm64 ./create_ar_ami.sh`. Requires multi-arch service images."
  type        = string
  default     = "ami-0c22f54b43a823533" # arm64, IPv6 + TLS jar + paxos node-0 fixes
}

variable "ar_count" {
  description = "Number of ActiveReplica nodes (uses subnets 2..N)."
  type        = number
  default     = 3
}

# Per-role instance sizing. The RC is a lean control-plane node (reconfigurator
# JVM + coredns; no Docker daemon, no app containers, off the data path), so it
# runs comfortably on a smaller, cheaper instance than the ARs. The RC JVM sets
# no -Xmx, so its max heap is ~1/4 of RAM -- downsizing RAM shrinks the heap
# automatically (t3.medium => ~1 GB heap, ample for a singleton control plane).
# The ARs run Docker + possibly heavy stateful apps (MySQL/Postgres), so they
# keep the larger size.
variable "rc_instance_type" {
  description = "EC2 instance type for the Reconfigurator (control plane) node. Defaults to a Graviton t4g.micro -- REQUIRES an arm64 rc_ami (build with ARCH=arm64 ./create_rc_ami.sh). t4g.micro has only 1GB RAM, so the RC JVM runs with a small (~256MB) default heap; the userdata adds a swapfile to absorb GC/boot spikes."
  type        = string
  default     = "t4g.micro" # Graviton, ~$6/mo in us-east-1 (needs arm64 rc_ami)
}

variable "ar_instance_type" {
  description = "EC2 instance type for the ActiveReplica (AR) edge nodes. Graviton (needs the arm64 ar_ami + multi-arch service images). t4g.small (2GB) suits light services/eval; bump to t4g.medium (4GB) or t4g.large (8GB) for heavy stateful apps (MySQL/Postgres) where 2GB is too tight."
  type        = string
  default     = "t4g.small" # Graviton 2GB, ~$12/mo each; use t4g.large for heavy DB apps
}

# Run the ARs as Spot instances (~60-70% cheaper than on-demand) instead of
# on-demand. The 3-way Paxos quorum tolerates losing one AR to a reclaim, so this
# fits the research/eval cluster -- not stability-critical runs. Spot price is
# variable and capacity is not guaranteed. Uses PERSISTENT spot with `stop`
# interruption behavior: a reclaimed AR is STOPPED (EBS + addresses preserved) and
# auto-restarted by AWS when capacity returns, and cluster_power.sh can still
# pause/resume it. Best paired with stop-when-idle for long experiment runs.
variable "ar_use_spot" {
  description = "Run ActiveReplicas as Spot instances (persistent, stop-on-interruption). ~60-70% cheaper; a reclaim is tolerated by the 3-way quorum. Suitable for the research cluster."
  type        = bool
  default     = false
}

# --- HTTPS / Let's Encrypt (Caddy on the ARs) ---
variable "base_domain" {
  description = "Apex domain served by coredns; Caddy obtains a wildcard *.<base_domain> cert so every <service>.<base_domain> is reachable over HTTPS."
  type        = string
  default     = "xdnapp.com"
}

variable "acme_email" {
  description = "Contact email for the Let's Encrypt ACME account (expiry notices). Empty registers anonymously."
  type        = string
  default     = ""
}

variable "acme_ca" {
  description = "ACME CA directory endpoint. Defaults to Let's Encrypt PRODUCTION. While testing, set to the staging endpoint (https://acme-staging-v02.api.letsencrypt.org/directory) to avoid burning the strict prod rate limits on the wildcard."
  type        = string
  default     = "https://acme-v02.api.letsencrypt.org/directory"
}

# Static private IPs make the full cluster view known at plan time, so the
# user_data templates can reference every node without resource cycles.
locals {
  rc_private_ip  = "10.0.1.10" # in subnet[0] = 10.0.1.0/24
  ar_private_ips = [for i in range(var.ar_count) : "10.0.${i + 2}.10"]

  # Static GLOBAL IPv6 per node, derived from each subnet's /64 (host ::10).
  # Unlike the IPv4 EIP (NAT'd, not locally bindable), an instance's IPv6 is on
  # its ENI and IS directly bindable -- so gigapaxos can advertise AND bind the
  # same IPv6 for consensus. Derived from the subnet (not the instance), so
  # using it in both ipv6_addresses and user_data creates no dependency cycle.
  # Note: the subnet IPv6 CIDR is known only after apply (AWS assigns the VPC
  # block), which is fine -- these resolve at apply time.
  rc_ipv6  = cidrhost(aws_subnet.ipv6_subnets[0].ipv6_cidr_block, 10)
  ar_ipv6s = [for i in range(var.ar_count) : cidrhost(aws_subnet.ipv6_subnets[i + 1].ipv6_cidr_block, 10)]

  # Per-AR coordinates surfaced as active.<id>.geolocation (parsed by PaxosConfig,
  # returned in the /placement API, plotted on the dashboard map). NOTE: this
  # cluster physically runs in a single region (us-east-1); these are ILLUSTRATIVE
  # coordinates spread across the US (East / Central / West) so the placement map
  # demonstrates XDN's geo-distribution. Replace with real per-region coordinates
  # for a true multi-region deployment. element() wraps if ar_count > 3.
  ar_geolocations = ["38.95,-77.45", "41.88,-87.63", "37.36,-121.92"]

  # gigapaxos cluster config shared by every node. Numeric node ids: RC=0, ARs=1..N.
  # Nodes advertise their GLOBAL IPv6 addresses for consensus (reconfigurator/
  # active) -- the gigapaxos wire format is now IPv6-capable (16-byte addresses,
  # see AddressCodec) and the JVM no longer forces the IPv4 stack. coredns returns
  # these IPv6 addresses as AAAA for <service>.xdnapp.com, so the data plane is
  # IPv6 too. IPv4 EIPs remain for SSH, the registrar NS glue, cp/rc/ns, and the
  # ACME DNS-01 path. Consensus ports (2000/3000) are reachable node->node over
  # IPv6 within the VPC (security group Rule B6), not exposed to the open internet.
  gigapaxos_properties = join("\n", concat([
    "APPLICATION=edu.umass.cs.xdn.XdnGigapaxosApp",
    "REPLICA_COORDINATOR_CLASS=edu.umass.cs.xdn.XdnReplicaCoordinator",
    "INITIAL_STATE_VALIDATOR_CLASS=edu.umass.cs.xdn.XdnServiceInitialStateValidator",
    "GIGAPAXOS_DATA_DIR=/tmp/gigapaxos",
    "NIO_MAX_PAYLOAD_SIZE=134217728",
    # Geo-demand profiling: with this set, each request's client location (the
    # X-Client-Location header, parsed into XdnHttpRequest) is accumulated per grid
    # cell by XdnGeoDemandProfiler on the ARs and aggregated at the RC. It drives
    # demand-aware re-placement AND is surfaced read-only by
    # GET /api/v2/services/<svc>/demand for the dashboard heatmap. (Matches the
    # geo-demand smoke config; the only flag needed to turn the pipeline on.)
    "DEMAND_PROFILE_TYPE=edu.umass.cs.xdn.XdnGeoDemandProfiler",
    # AR data-plane HTTP frontend (the RC ignores these flags; gated off for
    # reconfigurators in ActiveReplica.java). The frontend terminates TLS itself
    # (Netty + BoringSSL) directly on :443 -- no reverse-proxy hop. The HTTPS
    # flags + cert paths are appended PER-AR in ar-userdata.tftpl (not here) so
    # the RC's shared properties are untouched and the RC isn't replaced.
    "ENABLE_ACTIVE_REPLICA_HTTP=true",
    "ENABLE_ACTIVE_REPLICA_HTTP_PORT_80=false",
    # RC control-plane API on :3300, which xdn-cli + coredns depend on
    # (default true; set explicitly for clarity).
    "ENABLE_RECONFIGURATOR_HTTP=true",
    "reconfigurator.0=[${local.rc_ipv6}]:3000",
    ], [
    for i in range(var.ar_count) : "active.${i + 1}=[${local.ar_ipv6s[i]}]:2000"
    ], [
    # Per-AR geolocation for the dashboard placement map (illustrative; see above).
    for i in range(var.ar_count) : "active.${i + 1}.geolocation=\"${element(local.ar_geolocations, i)}\""
  ]))
}

# 9a. Reconfigurator (control plane) + coredns nameserver. Single node, subnet[0].
resource "aws_instance" "rc" {
  ami                    = var.rc_ami
  instance_type          = var.rc_instance_type
  subnet_id              = aws_subnet.ipv6_subnets[0].id
  private_ip             = local.rc_private_ip
  ipv6_addresses         = [local.rc_ipv6] # consensus advertises + binds this
  vpc_security_group_ids = [aws_security_group.allow_ssh_ipv6.id]
  key_name               = aws_key_pair.deployer.key_name

  # Read-only access to the wildcard cert in S3, so the control-plane API can
  # terminate TLS for the dashboard (the RC pulls the same PEM the ARs do).
  iam_instance_profile = aws_iam_instance_profile.rc_tls.name

  # Bootstrap: drop the gated config so the baked xdn-rc + xdn-dns units start.
  # Replace the instance when the config changes, so cloud-init re-runs with the
  # new gigapaxos.properties (an in-place user_data update would NOT re-run it).
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/rc-userdata.tftpl", {
    gigapaxos_properties = local.gigapaxos_properties
    rc_eip               = aws_eip.rc.public_ip
    base_domain          = var.base_domain
    # NS records of the delegated _acme-challenge zone; coredns refers ACME
    # resolvers here so Caddy's DNS-01 TXT (written in Route53) is reachable.
    acme_ns = aws_route53_zone.acme.name_servers
    # Cert store for the control-plane TLS. Use the bucket id + LITERAL object keys
    # (NOT aws_s3_object.*.key): the wildcard cert is issued AFTER the RC (issuance
    # needs the RC's coredns), so referencing the cert objects here would create a
    # dependency cycle. The RC pulls them once present (boot retry loop + timer);
    # until then HttpReconfigurator serves a temporary self-signed cert.
    aws_region    = "us-east-1"
    tls_bucket    = aws_s3_bucket.tls.id
    fullchain_key = "wildcard/fullchain.pem"
    privkey_key   = "wildcard/privkey.pem"
  })

  root_block_device {
    volume_size = 16 # lean control-plane node; matches the smaller RC AMI snapshot
    volume_type = "gp3"
  }

  tags = { Name = "xdn-rc" }
}

# 9b. ActiveReplica edge nodes, one per remaining subnet (subnet[i+1]).
resource "aws_instance" "ar" {
  count                       = var.ar_count
  ami                         = var.ar_ami
  instance_type               = var.ar_instance_type
  subnet_id                   = aws_subnet.ipv6_subnets[count.index + 1].id
  private_ip                  = local.ar_private_ips[count.index]
  ipv6_addresses              = [local.ar_ipv6s[count.index]] # consensus + frontend bind
  associate_public_ip_address = false                         # IPv6-only: no billed public IPv4
  vpc_security_group_ids      = [aws_security_group.allow_ssh_ipv6.id]
  key_name                    = aws_key_pair.deployer.key_name

  # Lets the baked Caddy answer the ACME DNS-01 challenge via Route53 (no keys).
  iam_instance_profile = aws_iam_instance_profile.ar_acme.name

  # Optional Spot pricing for the ARs (var.ar_use_spot). Persistent + stop so a
  # reclaim stops (not terminates) the AR -- EBS and the static private/IPv6
  # addresses survive, AWS restarts it when capacity returns, and the quorum only
  # ever loses one replica transiently. Toggling this forces AR replacement.
  dynamic "instance_market_options" {
    for_each = var.ar_use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        spot_instance_type             = "persistent"
        instance_interruption_behavior = "stop"
      }
    }
  }

  # Bootstrap: drop config + a unique numeric node id (1..N) so the baked
  # xdn-ar unit starts. The RC is node 0; ARs continue from 1.
  # Replace on config change so cloud-init re-runs with the new properties.
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/ar-userdata.tftpl", {
    gigapaxos_properties = local.gigapaxos_properties
    node_id              = count.index + 1
    aws_region           = "us-east-1"
    # Cert is issued once by Terraform (acme_certificate) and stashed in S3; the
    # AR pulls it on boot/renewal. No per-AR issuance -> no rate-limit burn.
    tls_bucket    = aws_s3_bucket.tls.id
    fullchain_key = aws_s3_object.fullchain.key
    privkey_key   = aws_s3_object.privkey.key
  })

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = { Name = "xdn-ar-${count.index}" }
}

# 10. Stable public IP for the RC (a nameserver must not change IP on stop/start).
#     Allocated standalone and associated separately so the RC's user_data can
#     reference aws_eip.rc.public_ip without creating a dependency cycle.
resource "aws_eip" "rc" {
  domain = "vpc"
  tags   = { Name = "xdn-rc-eip" }
}

resource "aws_eip_association" "rc" {
  instance_id   = aws_instance.rc.id
  allocation_id = aws_eip.rc.id
}

# 10b. (Removed) The ARs no longer have public IPv4. Their data plane is IPv6
#      (coredns returns AAAA -> the AR's global IPv6) and their egress is IPv6
#      (Docker Hub/S3-dualstack/apt all reachable over IPv6). SSH/management is
#      via the RC as a jump host over the private/IPv6 path. This drops 3 billed
#      public IPv4 addresses.

# 10c. ACME DNS-01 delegation zone.
#      coredns (the xdn plugin) is authoritative for the apex and only knows how
#      to answer A queries for service names -- it cannot serve the dynamic
#      _acme-challenge TXT that Let's Encrypt needs for a wildcard. So we host
#      ONLY _acme-challenge.<base_domain> in Route53 and delegate to it from the
#      coredns zone file (see rc-userdata.tftpl). Caddy on each AR writes the
#      challenge TXT here via the AWS API; LE follows the coredns NS referral to
#      Route53 to read it. The apex stays on coredns (geo-routing untouched).
resource "aws_route53_zone" "acme" {
  name    = "_acme-challenge.${var.base_domain}"
  comment = "ACME DNS-01 challenge records for the *.${var.base_domain} wildcard (delegated from coredns)."
}

# 10d. Instance role so each AR's Caddy can write the challenge TXT into the zone
#      above using the EC2 instance profile (no static keys on the hosts).
data "aws_iam_policy_document" "ar_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ar_acme" {
  name               = "xdn-ar-acme-route53"
  assume_role_policy = data.aws_iam_policy_document.ar_assume.json
}

data "aws_iam_policy_document" "ar_acme" {
  # Zone discovery (libdns route53 finds the hosted zone by name).
  statement {
    actions   = ["route53:ListHostedZones", "route53:ListHostedZonesByName"]
    resources = ["*"]
  }
  # Poll until the change is INSYNC.
  statement {
    actions   = ["route53:GetChange"]
    resources = ["arn:aws:route53:::change/*"]
  }
  # Read/write the TXT, scoped to the delegated challenge zone only -- the ARs
  # cannot touch the geo-routing records (those live in coredns) or any other zone.
  statement {
    actions = [
      "route53:ChangeResourceRecordSets",
      "route53:ListResourceRecordSets",
      "route53:GetHostedZone",
    ]
    resources = ["arn:aws:route53:::hostedzone/${aws_route53_zone.acme.zone_id}"]
  }
  # Pull the Terraform-issued wildcard cert (PEM) from the cert store on boot.
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.tls.arn}/*"]
  }
}

resource "aws_iam_role_policy" "ar_acme" {
  name   = "xdn-ar-acme-route53"
  role   = aws_iam_role.ar_acme.id
  policy = data.aws_iam_policy_document.ar_acme.json
}

resource "aws_iam_instance_profile" "ar_acme" {
  name = "xdn-ar-acme-route53"
  role = aws_iam_role.ar_acme.name
}

# 10g. RC instance role: read-only pull of the Terraform-issued wildcard cert
#      from S3, so the control-plane HTTP API (HttpReconfigurator) can terminate
#      TLS for the browser dashboard. Unlike the ARs, the RC needs NO Route53
#      access (coredns is baked) -- only s3:GetObject on the cert store. Reuses the
#      ar_assume policy doc (same ec2.amazonaws.com principal).
resource "aws_iam_role" "rc_tls" {
  name               = "xdn-rc-tls-s3"
  assume_role_policy = data.aws_iam_policy_document.ar_assume.json
}

data "aws_iam_policy_document" "rc_tls" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.tls.arn}/*"]
  }
}

resource "aws_iam_role_policy" "rc_tls" {
  name   = "xdn-rc-tls-s3"
  role   = aws_iam_role.rc_tls.id
  policy = data.aws_iam_policy_document.rc_tls.json
}

resource "aws_iam_instance_profile" "rc_tls" {
  name = "xdn-rc-tls-s3"
  role = aws_iam_role.rc_tls.name
}

# 10e. Wildcard certificate, issued ONCE by Terraform via ACME DNS-01 and kept
#      in state, so AR replacements reuse it instead of re-issuing (which would
#      burn the Let's Encrypt rate limit). Terraform renews only on apply when
#      within min_days_remaining of expiry. The DNS-01 TXT is written into the
#      delegated Route53 zone (10c) using Terraform's own AWS creds; LE validates
#      it through the coredns delegation, exactly like before.
resource "tls_private_key" "acme_account" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "acme_registration" "reg" {
  account_key_pem = tls_private_key.acme_account.private_key_pem
  email_address   = var.acme_email
}

resource "acme_certificate" "wildcard" {
  account_key_pem    = acme_registration.reg.account_key_pem
  common_name        = "*.${var.base_domain}"
  min_days_remaining = 30

  dns_challenge {
    provider = "route53"
    # lego's route53 solver reads AWS creds from Terraform's environment; pin the
    # region. It writes the TXT into the _acme-challenge.<base_domain> zone (10c).
    config = {
      AWS_REGION = "us-east-1"
      # Pin the hosted zone id so lego writes the challenge TXT DIRECTLY (and waits
      # on route53 GetChange -> INSYNC) instead of discovering the zone via a public
      # SOA walk. On a cold apply that walk runs from the apply host's resolver,
      # which can SERVFAIL until the xdnapp.com delegation fully propagates and any
      # negative cache expires -- blocking issuance even though the zone already
      # exists (it's created in this same apply). Pinning removes that DNS dependency.
      AWS_HOSTED_ZONE_ID = aws_route53_zone.acme.zone_id
    }
  }

  # The post-present propagation pre-check resolves the TXT before asking the CA to
  # validate; use public recursive resolvers for it, since the apply host's default
  # resolver may SERVFAIL on the just-delegated zone.
  recursive_nameservers = ["8.8.8.8:53", "1.1.1.1:53"]

  # Validation resolves _acme-challenge through coredns -> Route53, so the RC
  # (coredns) must be reachable. On a fully cold apply this may need a re-apply
  # if coredns isn't serving yet; on AR-only applies the cert is reused from state.
  depends_on = [
    aws_route53_zone.acme,
    aws_route53domains_registered_domain.xdnapp,
    aws_eip_association.rc,
  ]
}

# 10f. Durable cert store. Terraform writes the issued PEM here; ARs pull it on
#      boot (and hourly, to pick up renewals). This is what makes the cert
#      survive instance replacement without re-issuing.
resource "aws_s3_bucket" "tls" {
  bucket_prefix = "xdn-tls-"
  force_destroy = true
  tags          = { Name = "xdn-tls-store" }
}

resource "aws_s3_bucket_public_access_block" "tls" {
  bucket                  = aws_s3_bucket.tls.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tls" {
  bucket = aws_s3_bucket.tls.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# fullchain = leaf + issuer chain (what Netty's SslContextBuilder wants).
resource "aws_s3_object" "fullchain" {
  bucket       = aws_s3_bucket.tls.id
  key          = "wildcard/fullchain.pem"
  content      = "${acme_certificate.wildcard.certificate_pem}${acme_certificate.wildcard.issuer_pem}"
  content_type = "application/x-pem-file"
  etag         = md5("${acme_certificate.wildcard.certificate_pem}${acme_certificate.wildcard.issuer_pem}")
}

resource "aws_s3_object" "privkey" {
  bucket       = aws_s3_bucket.tls.id
  key          = "wildcard/privkey.pem"
  content      = acme_certificate.wildcard.private_key_pem
  content_type = "application/x-pem-file"
  etag         = md5(acme_certificate.wildcard.private_key_pem)
}

# 11. Delegate the xdnapp.com zone to coredns on the RC.
#     coredns is authoritative for the whole zone and self-serves its own
#     ns1/ns2 glue (see xdn-dns/plugin/xdn/xdn.go). Both nameservers point to
#     the single RC EIP (no DNS redundancy, acceptable for a research cluster).
#     NOTE: the Route53 Domains API only operates in us-east-1 (matches our provider).
resource "aws_route53domains_registered_domain" "xdnapp" {
  domain_name = "xdnapp.com"

  name_server {
    name     = "ns1.xdnapp.com"
    glue_ips = [aws_eip.rc.public_ip]
  }

  name_server {
    name     = "ns2.xdnapp.com"
    glue_ips = [aws_eip.rc.public_ip]
  }
}

# 12. Outputs
output "rc_elastic_ip" {
  value       = aws_eip.rc.public_ip
  description = "RC address for everything (SSH, XDN_CONTROL_PLANE :3300, and the xdnapp.com authoritative NS). The instance's auto-assigned public IP is released once this EIP attaches, so always use this."
}

output "ar_ipv6_addresses" {
  value       = local.ar_ipv6s
  description = "Global IPv6 of the ActiveReplica nodes (what coredns returns as AAAA for <service>.xdnapp.com). The ARs have no public IPv4."
}

output "cluster_node_addresses" {
  value = merge(
    { "0" = "${aws_eip.rc.public_ip} (RC, IPv4 mgmt) / [${local.rc_ipv6}] (consensus)" },
    { for i in range(var.ar_count) : tostring(i + 1) => "[${local.ar_ipv6s[i]}] (AR, IPv6-only)" },
  )
  description = "Numeric node id -> advertised consensus address (IPv6). The RC also keeps an IPv4 EIP for management/DNS."
}

output "acme_delegation_ns" {
  value       = aws_route53_zone.acme.name_servers
  description = "Route53 nameservers for the delegated _acme-challenge zone. coredns refers ACME resolvers here; verify with: dig +short NS _acme-challenge.<base_domain> @<rc_elastic_ip>"
}
