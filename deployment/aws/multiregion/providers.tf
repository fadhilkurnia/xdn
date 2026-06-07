# Multi-region XDN deployment.
#
# Unlike ../useast1/main.tf (single-region: all nodes in one us-east-1 VPC), this config
# physically spreads the ActiveReplicas across AWS regions. The control plane (RC
# + coredns + EIP + ACME/cert/domain) stays in us-east-1; each AR lives in a VPC
# in its own region and joins consensus over its GLOBAL IPv6 (the gigapaxos wire
# format is IPv6-capable and IPv6 is globally routable, so no VPC peering is
# needed -- the per-region security groups just allow the cluster's IPv6 ranges).
#
# Terraform cannot select a provider dynamically, so each region is wired
# explicitly via a provider alias; topology (which AR in which region) is data in
# locals.replicas.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Obtains the wildcard cert ONCE via ACME DNS-01 and keeps it in TF state.
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

# Default provider = the control region (us-east-1). The Route53 Domains API and
# the RC/cert/DNS resources all run here.
provider "aws" {
  region = "us-east-1"
}

# AR regions. Add a provider alias here (and a matching region block in
# network.tf / compute.tf) to host ARs in a new region.
provider "aws" {
  alias  = "use2"
  region = "us-east-2"
}

provider "aws" {
  alias  = "usw2"
  region = "us-west-2"
}

provider "acme" {
  server_url = var.acme_ca
}
