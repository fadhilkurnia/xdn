terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
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

  # Rule B4: RC control-plane API (xdn-cli contacts cp.xdnapp.com:3300)
  ingress {
    description      = "XDN control-plane API on the RC"
    from_port        = 3300
    to_port          = 3300
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Rule B5: intra-cluster traffic over PUBLIC IPs. Because gigapaxos.properties
  #          advertises the nodes' EIPs, consensus (e.g. :2000, :3000) flows
  #          node->node via public IPs. Allow all ports ONLY from the cluster's
  #          own EIPs -- this keeps :2000/:3000 off the open internet (they are
  #          NOT in any 0.0.0.0/0 rule) while letting members reach each other.
  ingress {
    description = "Intra-cluster consensus over public IPs (RC + AR EIPs only)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = concat(["${aws_eip.rc.public_ip}/32"], [for e in aws_eip.ar : "${e.public_ip}/32"])
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
  description = "AMI for the Reconfigurator (RC) + coredns node (includes docker-ce-cli for CREATE-time image validation)."
  type        = string
  default     = "ami-0af90e71c86a6ac6b"
}

variable "ar_ami" {
  description = "AMI for the ActiveReplica (AR) edge nodes."
  type        = string
  default     = "ami-02037dbf544575eef"
}

variable "ar_count" {
  description = "Number of ActiveReplica nodes (uses subnets 2..N)."
  type        = number
  default     = 3
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

  # Clear HTTP port the XDN frontend binds on each AR when PORT_80=false:
  # the AR's gigapaxos listen port (2000, from active.N below) + HTTP_PORT_OFFSET
  # (300). Caddy reverse-proxies to 127.0.0.1 on this port.
  ar_http_upstream_port = 2300

  # gigapaxos cluster config shared by every node. Numeric node ids: RC=0, ARs=1..N.
  # Nodes advertise their PUBLIC Elastic IPs so that coredns returns
  # client-reachable addresses for <service>.xdnapp.com (and geo-routing works on
  # real geolocated IPs). Consequence: consensus traffic flows node->node over
  # public IPs; ports 2000/3000 are therefore restricted to the cluster's own
  # EIPs in the security group (Rule B5) and never exposed to the open internet.
  gigapaxos_properties = join("\n", concat([
    "APPLICATION=edu.umass.cs.xdn.XdnGigapaxosApp",
    "REPLICA_COORDINATOR_CLASS=edu.umass.cs.xdn.XdnReplicaCoordinator",
    "INITIAL_STATE_VALIDATOR_CLASS=edu.umass.cs.xdn.XdnServiceInitialStateValidator",
    "GIGAPAXOS_DATA_DIR=/tmp/gigapaxos",
    "NIO_MAX_PAYLOAD_SIZE=134217728",
    # AR data-plane HTTP frontend (the RC ignores these flags; the frontend is
    # gated off for reconfigurators in ActiveReplica.java). PORT_80 is FALSE so
    # the frontend binds its offset clear port (active 2000 + HTTP_PORT_OFFSET
    # 300 = 2300, on 0.0.0.0) instead of :80. That frees :80/:443 for Caddy,
    # which terminates TLS and reverse-proxies to 127.0.0.1:${local.ar_http_upstream_port}.
    "ENABLE_ACTIVE_REPLICA_HTTP=true",
    "ENABLE_ACTIVE_REPLICA_HTTP_PORT_80=false",
    # RC control-plane API on :3300, which xdn-cli + coredns depend on
    # (default true; set explicitly for clarity).
    "ENABLE_RECONFIGURATOR_HTTP=true",
    "reconfigurator.0=${aws_eip.rc.public_ip}:3000",
    ], [
    for i in range(var.ar_count) : "active.${i + 1}=${aws_eip.ar[i].public_ip}:2000"
  ]))
}

# 9a. Reconfigurator (control plane) + coredns nameserver. Single node, subnet[0].
resource "aws_instance" "rc" {
  ami                    = var.rc_ami
  instance_type          = "t3.large"
  subnet_id              = aws_subnet.ipv6_subnets[0].id
  private_ip             = local.rc_private_ip
  vpc_security_group_ids = [aws_security_group.allow_ssh_ipv6.id]
  key_name               = aws_key_pair.deployer.key_name

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
  })

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = { Name = "xdn-rc" }
}

# 9b. ActiveReplica edge nodes, one per remaining subnet (subnet[i+1]).
resource "aws_instance" "ar" {
  count                  = var.ar_count
  ami                    = var.ar_ami
  instance_type          = "t3.large"
  subnet_id              = aws_subnet.ipv6_subnets[count.index + 1].id
  private_ip             = local.ar_private_ips[count.index]
  vpc_security_group_ids = [aws_security_group.allow_ssh_ipv6.id]
  key_name               = aws_key_pair.deployer.key_name

  # Lets the baked Caddy answer the ACME DNS-01 challenge via Route53 (no keys).
  iam_instance_profile = aws_iam_instance_profile.ar_acme.name

  # Bootstrap: drop config + a unique numeric node id (1..N) so the baked
  # xdn-ar unit starts. The RC is node 0; ARs continue from 1.
  # Replace on config change so cloud-init re-runs with the new properties.
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/ar-userdata.tftpl", {
    gigapaxos_properties = local.gigapaxos_properties
    node_id              = count.index + 1
    base_domain          = var.base_domain
    upstream_port        = local.ar_http_upstream_port
    acme_email           = var.acme_email
    acme_ca              = var.acme_ca
    aws_region           = "us-east-1"
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

# 10b. Stable public IPs for the ARs. Required now that gigapaxos.properties
#      advertises public addresses: coredns returns these to clients, so they
#      must be stable (ephemeral IPs change on stop/start and would break DNS).
#      Allocated standalone (no instance) so the AR user_data can reference them
#      without a dependency cycle.
resource "aws_eip" "ar" {
  count  = var.ar_count
  domain = "vpc"
  tags   = { Name = "xdn-ar-eip-${count.index}" }
}

resource "aws_eip_association" "ar" {
  count         = var.ar_count
  instance_id   = aws_instance.ar[count.index].id
  allocation_id = aws_eip.ar[count.index].id
}

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

output "ar_public_ips" {
  value       = aws_eip.ar[*].public_ip
  description = "Elastic IPs of the ActiveReplica nodes (also what coredns returns for service names)."
}

output "cluster_node_addresses" {
  value = merge(
    { "0" = "${aws_eip.rc.public_ip} (RC)" },
    { for i in range(var.ar_count) : tostring(i + 1) => "${aws_eip.ar[i].public_ip} (AR)" },
  )
  description = "Numeric node id -> PUBLIC address advertised in gigapaxos.properties (node 0 is the RC)."
}

output "acme_delegation_ns" {
  value       = aws_route53_zone.acme.name_servers
  description = "Route53 nameservers for the delegated _acme-challenge zone. coredns refers ACME resolvers here; verify with: dig +short NS _acme-challenge.<base_domain> @<rc_elastic_ip>"
}
