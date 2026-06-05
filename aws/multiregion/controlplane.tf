# ===========================================================================
# Control plane (us-east-1 / global): RC elastic IP, ACME DNS-01 delegation zone,
# IAM instance profiles, the wildcard cert, its S3 store, and the domain
# delegation. Identical to the single-region config -- the RC is single-homed.
# ===========================================================================

# Stable public IP for the RC (a nameserver must not change IP on stop/start).
resource "aws_eip" "rc" {
  domain = "vpc"
  tags   = { Name = "xdn-rc-eip" }
}

resource "aws_eip_association" "rc" {
  instance_id   = aws_instance.rc.id
  allocation_id = aws_eip.rc.id
}

# ACME DNS-01 delegation zone: coredns is authoritative for the apex but can't
# serve the dynamic _acme-challenge TXT, so that single subzone is hosted in
# Route53 and delegated from coredns (see ../rc-userdata.tftpl).
resource "aws_route53_zone" "acme" {
  name    = "_acme-challenge.${var.base_domain}"
  comment = "ACME DNS-01 challenge records for the *.${var.base_domain} wildcard (delegated from coredns)."
}

# Same delegation pattern for the per-replica `edge` sub-zone, so the
# *.edge.${var.base_domain} wildcard SAN can pass DNS-01 at
# _acme-challenge.edge.${var.base_domain}. coredns forwards this subzone to here.
resource "aws_route53_zone" "acme_edge" {
  name    = "_acme-challenge.edge.${var.base_domain}"
  comment = "ACME DNS-01 for the *.edge.${var.base_domain} wildcard (per-replica names; delegated from coredns)."
}

# Instance role assumed by EC2.
data "aws_iam_policy_document" "ar_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

# AR role: write the ACME challenge TXT into the delegated zone + pull the cert.
resource "aws_iam_role" "ar_acme" {
  name               = "xdn-mr-ar-acme-route53"
  assume_role_policy = data.aws_iam_policy_document.ar_assume.json
}

data "aws_iam_policy_document" "ar_acme" {
  statement {
    actions   = ["route53:ListHostedZones", "route53:ListHostedZonesByName"]
    resources = ["*"]
  }
  statement {
    actions   = ["route53:GetChange"]
    resources = ["arn:aws:route53:::change/*"]
  }
  statement {
    actions = [
      "route53:ChangeResourceRecordSets",
      "route53:ListResourceRecordSets",
      "route53:GetHostedZone",
    ]
    resources = ["arn:aws:route53:::hostedzone/${aws_route53_zone.acme.zone_id}"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${local.persist_tls_bucket_arn}/*"]
  }
}

resource "aws_iam_role_policy" "ar_acme" {
  name   = "xdn-mr-ar-acme-route53"
  role   = aws_iam_role.ar_acme.id
  policy = data.aws_iam_policy_document.ar_acme.json
}

resource "aws_iam_instance_profile" "ar_acme" {
  name = "xdn-mr-ar-acme-route53"
  role = aws_iam_role.ar_acme.name
}

# RC role: read-only pull of the wildcard cert from S3 (for control-plane TLS).
resource "aws_iam_role" "rc_tls" {
  name               = "xdn-mr-rc-tls-s3"
  assume_role_policy = data.aws_iam_policy_document.ar_assume.json
}

data "aws_iam_policy_document" "rc_tls" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${local.persist_tls_bucket_arn}/*"]
  }
}

resource "aws_iam_role_policy" "rc_tls" {
  name   = "xdn-mr-rc-tls-s3"
  role   = aws_iam_role.rc_tls.id
  policy = data.aws_iam_policy_document.rc_tls.json
}

resource "aws_iam_instance_profile" "rc_tls" {
  name = "xdn-mr-rc-tls-s3"
  role = aws_iam_role.rc_tls.name
}

# Wildcard cert, issued ONCE (var.issue_cert) via ACME DNS-01 and kept in state.
# Default false: nodes pull the persisted cert from local.persist_tls_bucket.
resource "tls_private_key" "acme_account" {
  count     = var.issue_cert ? 1 : 0
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "acme_registration" "reg" {
  count           = var.issue_cert ? 1 : 0
  account_key_pem = tls_private_key.acme_account[0].private_key_pem
  email_address   = var.acme_email
}

resource "acme_certificate" "wildcard" {
  count           = var.issue_cert ? 1 : 0
  account_key_pem = acme_registration.reg[0].account_key_pem
  common_name     = "*.${var.base_domain}"
  # Per-replica wildcard for the reserved `edge` sub-zone (<nodeid>.edge.<domain>)
  # so a browser can reach a chosen replica over a cert-valid HTTPS name.
  subject_alternative_names = ["*.edge.${var.base_domain}"]
  min_days_remaining        = 30

  dns_challenge {
    provider = "route53"
    config = {
      AWS_REGION = "us-east-1"
      # No AWS_HOSTED_ZONE_ID pin: there are now two challenge names in two
      # delegated zones (_acme-challenge.<domain> and _acme-challenge.edge.<domain>),
      # so lego must discover the right zone per challenge. Issue AFTER the cluster
      # is up (coredns + both delegations serving) so discovery resolves via the
      # recursive_nameservers below.
    }
  }

  recursive_nameservers = ["8.8.8.8:53", "1.1.1.1:53"]

  # Keep lego's propagation pre-check ENABLED (it waits until the TXT is visible
  # before asking LE to validate, so LE never races propagation and burns a
  # failed-validation). The earlier flakiness was the Route53 SOA's 24h negative
  # cache, now lowered to 60s; with the un-pinned two-zone setup the pre-check is
  # the safe path since a propagation timeout costs no LE quota. Give it room.
  cert_timeout = 600

  depends_on = [
    aws_route53_zone.acme,
    aws_route53_zone.acme_edge,
    aws_route53domains_registered_domain.xdnapp,
    aws_eip_association.rc,
  ]
}

# Durable cert store (only written when issuing; otherwise the persistent bucket
# created out-of-band by ../bin/persist-cert.sh is used).
resource "aws_s3_bucket" "tls" {
  count         = var.issue_cert ? 1 : 0
  bucket_prefix = "xdn-tls-"
  force_destroy = true
  tags          = { Name = "xdn-tls-store" }
}

resource "aws_s3_bucket_public_access_block" "tls" {
  count                   = var.issue_cert ? 1 : 0
  bucket                  = aws_s3_bucket.tls[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tls" {
  count  = var.issue_cert ? 1 : 0
  bucket = aws_s3_bucket.tls[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "fullchain" {
  count        = var.issue_cert ? 1 : 0
  bucket       = aws_s3_bucket.tls[0].id
  key          = "wildcard/fullchain.pem"
  content      = "${acme_certificate.wildcard[0].certificate_pem}${acme_certificate.wildcard[0].issuer_pem}"
  content_type = "application/x-pem-file"
  etag         = md5("${acme_certificate.wildcard[0].certificate_pem}${acme_certificate.wildcard[0].issuer_pem}")
}

resource "aws_s3_object" "privkey" {
  count        = var.issue_cert ? 1 : 0
  bucket       = aws_s3_bucket.tls[0].id
  key          = "wildcard/privkey.pem"
  content      = acme_certificate.wildcard[0].private_key_pem
  content_type = "application/x-pem-file"
  etag         = md5(acme_certificate.wildcard[0].private_key_pem)
}

# Delegate xdnapp.com to coredns on the RC (ns1/ns2 glue -> the RC EIP).
# The Route53 Domains API only operates in us-east-1 (the default provider).
resource "aws_route53domains_registered_domain" "xdnapp" {
  domain_name = var.base_domain

  name_server {
    name     = "ns1.${var.base_domain}"
    glue_ips = [aws_eip.rc.public_ip]
  }

  name_server {
    name     = "ns2.${var.base_domain}"
    glue_ips = [aws_eip.rc.public_ip]
  }
}
