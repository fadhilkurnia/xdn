# ===========================================================================
# Key pairs (region-scoped), AR AMI copies (region-scoped), and the instances.
# IAM instance profiles are GLOBAL, so the ar_acme / rc_tls profiles defined in
# controlplane.tf are referenced by every AR/RC regardless of region.
# ===========================================================================

# Key pairs: one per region (EC2 key pairs are region-scoped). All import the
# same local public key, so the same ~/.ssh/xdn-aws-key private key works
# everywhere. ignore_changes avoids churn (the AWS API doesn't return key material).
resource "aws_key_pair" "use1" {
  key_name   = "xdn-aws-key"
  public_key = file("~/.ssh/xdn-aws-key.pub")
  lifecycle { ignore_changes = [public_key] }
}

resource "aws_key_pair" "use2" {
  provider   = aws.use2
  key_name   = "xdn-aws-key"
  public_key = file("~/.ssh/xdn-aws-key.pub")
  lifecycle { ignore_changes = [public_key] }
}

resource "aws_key_pair" "usw2" {
  provider   = aws.usw2
  key_name   = "xdn-aws-key"
  public_key = file("~/.ssh/xdn-aws-key.pub")
  lifecycle { ignore_changes = [public_key] }
}

# AR AMI copies into the non-source regions (AMIs are region-scoped). The RC and
# the us-east-1 ARs use the source AMIs directly.
resource "aws_ami_copy" "ar_use2" {
  provider          = aws.use2
  name              = "xdn-ar-arm64-mr-us-east-2"
  description       = "xdn-ar arm64 (multi-region copy from us-east-1)"
  source_ami_id     = var.ar_ami
  source_ami_region = "us-east-1"
  tags              = { Name = "xdn-ar-arm64-mr-us-east-2" }
}

resource "aws_ami_copy" "ar_usw2" {
  provider          = aws.usw2
  name              = "xdn-ar-arm64-mr-us-west-2"
  description       = "xdn-ar arm64 (multi-region copy from us-east-1)"
  source_ami_id     = var.ar_ami
  source_ami_region = "us-east-1"
  tags              = { Name = "xdn-ar-arm64-mr-us-west-2" }
}

# ---- Reconfigurator (control plane) + coredns, in us-east-1 ---------------
resource "aws_instance" "rc" {
  ami                    = var.rc_ami
  instance_type          = var.rc_instance_type
  subnet_id              = aws_subnet.rc.id
  ipv6_addresses         = [local.rc_ipv6]
  vpc_security_group_ids = [aws_security_group.use1.id]
  key_name               = aws_key_pair.use1.key_name
  iam_instance_profile   = aws_iam_instance_profile.rc_tls.name

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/../rc-userdata.tftpl", {
    gigapaxos_properties = local.gigapaxos_properties
    rc_eip               = data.aws_eip.rc.public_ip
    base_domain          = var.base_domain
    acme_ns              = aws_route53_zone.acme.name_servers
    aws_region           = local.control_region
    tls_bucket           = local.persist_tls_bucket
    fullchain_key        = "wildcard/fullchain.pem"
    privkey_key          = "wildcard/privkey.pem"
    candidate_geo_props  = local.candidate_geo_props
    # Per-replica `edge` sub-zone: the node->IPv6 Corefile block + the
    # _acme-challenge.edge.<domain> delegation NS (for the *.edge.<domain> SAN).
    edge_node_props = local.edge_node_props
    acme_edge_ns    = aws_route53_zone.acme_edge.name_servers
  })

  root_block_device {
    volume_size = 16
    volume_type = "gp3"
  }
  tags = { Name = "xdn-rc" }
}

# ---- ActiveReplicas ------------------------------------------------------
# One instance resource per region (provider is static per resource). Each uses
# the region's subnet/SG/key/AMI; the shared gigapaxos.properties + per-node id
# come from locals. ipv6_addresses pins the consensus-advertised global IPv6.

resource "aws_instance" "ar_use1" {
  for_each               = toset(local.ar_ids_use1)
  ami                    = var.ar_ami
  instance_type          = var.ar_instance_type
  subnet_id              = aws_subnet.ar_use1[each.key].id
  ipv6_addresses         = [local.ar_ipv6s[each.key]]
  vpc_security_group_ids = [aws_security_group.use1.id]
  key_name               = aws_key_pair.use1.key_name
  iam_instance_profile   = aws_iam_instance_profile.ar_acme.name

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

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/../ar-userdata.tftpl", {
    gigapaxos_properties = local.gigapaxos_properties
    node_id              = each.key
    aws_region           = local.control_region
    tls_bucket           = local.persist_tls_bucket
    fullchain_key        = "wildcard/fullchain.pem"
    privkey_key          = "wildcard/privkey.pem"
  })

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
  tags = { Name = "xdn-ar-${each.key}" }
}

resource "aws_instance" "ar_use2" {
  provider               = aws.use2
  for_each               = toset(local.ar_ids_use2)
  ami                    = aws_ami_copy.ar_use2.id
  instance_type          = var.ar_instance_type
  subnet_id              = aws_subnet.ar_use2[each.key].id
  ipv6_addresses         = [local.ar_ipv6s[each.key]]
  vpc_security_group_ids = [aws_security_group.use2.id]
  key_name               = aws_key_pair.use2.key_name
  iam_instance_profile   = aws_iam_instance_profile.ar_acme.name

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

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/../ar-userdata.tftpl", {
    gigapaxos_properties = local.gigapaxos_properties
    node_id              = each.key
    aws_region           = local.control_region
    tls_bucket           = local.persist_tls_bucket
    fullchain_key        = "wildcard/fullchain.pem"
    privkey_key          = "wildcard/privkey.pem"
  })

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
  tags = { Name = "xdn-ar-${each.key}" }
}

resource "aws_instance" "ar_usw2" {
  provider               = aws.usw2
  for_each               = toset(local.ar_ids_usw2)
  ami                    = aws_ami_copy.ar_usw2.id
  instance_type          = var.ar_instance_type
  subnet_id              = aws_subnet.ar_usw2[each.key].id
  ipv6_addresses         = [local.ar_ipv6s[each.key]]
  vpc_security_group_ids = [aws_security_group.usw2.id]
  key_name               = aws_key_pair.usw2.key_name
  iam_instance_profile   = aws_iam_instance_profile.ar_acme.name

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

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/../ar-userdata.tftpl", {
    gigapaxos_properties = local.gigapaxos_properties
    node_id              = each.key
    aws_region           = local.control_region
    tls_bucket           = local.persist_tls_bucket
    fullchain_key        = "wildcard/fullchain.pem"
    privkey_key          = "wildcard/privkey.pem"
  })

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
  tags = { Name = "xdn-ar-${each.key}" }
}
