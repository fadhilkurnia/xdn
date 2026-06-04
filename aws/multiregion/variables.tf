# AMIs are REGION-SCOPED. rc_ami / ar_ami are the us-east-1 images (built from
# fork/main by ../create_rc_ami.sh / ../create_ar_ami.sh); the AR image is
# aws_ami_copy'd into the other regions at apply time (see compute.tf).
variable "rc_ami" {
  description = "us-east-1 AMI for the Reconfigurator (RC) + coredns node. ARM64/Graviton (for t4g.* rc_instance_type), built from fork/main by `ARCH=arm64 ./create_rc_ami.sh`."
  type        = string
  default     = "ami-03b0ad52656bf6840" # arm64, main: RESTful control API + GeoIP demand
}

variable "ar_ami" {
  description = "us-east-1 AMI for the ActiveReplica (AR) edge nodes; copied into the other AR regions. ARM64/Graviton (for t4g.* ar_instance_type), built from fork/main by `ARCH=arm64 ./create_ar_ami.sh`. Requires multi-arch service images."
  type        = string
  default     = "ami-0872fc74ccf98b371" # arm64, main: RESTful control API + GeoIP demand
}

variable "rc_instance_type" {
  description = "EC2 instance type for the Reconfigurator (control plane). Graviton t4g.nano (0.5GB; the userdata pins -Xmx256m + a 2GB swapfile). Bump for heavy load."
  type        = string
  default     = "t4g.nano"
}

variable "ar_instance_type" {
  description = "EC2 instance type for the ActiveReplica edge nodes. Graviton (needs the arm64 ar_ami + multi-arch service images). t4g.small (2GB) for light services; bump for heavy DB apps."
  type        = string
  default     = "t4g.small"
}

variable "ar_use_spot" {
  description = "Run ActiveReplicas as Spot instances (persistent, stop-on-interruption). ~60-70% cheaper; a reclaim is tolerated by the quorum."
  type        = bool
  default     = false
}

variable "base_domain" {
  description = "Apex domain served by coredns; the wildcard *.<base_domain> cert makes every <service>.<base_domain> reachable over HTTPS."
  type        = string
  default     = "xdnapp.com"
}

variable "acme_email" {
  description = "Contact email for the Let's Encrypt ACME account. Empty registers anonymously."
  type        = string
  default     = ""
}

variable "acme_ca" {
  description = "ACME CA directory endpoint. Defaults to Let's Encrypt PRODUCTION; use the staging endpoint while testing."
  type        = string
  default     = "https://acme-v02.api.letsencrypt.org/directory"
}

variable "issue_cert" {
  description = <<-EOT
    Whether THIS apply should issue the wildcard cert via ACME. Default false: the
    cert is read from the PERSISTENT bucket (xdn-tls-persist-<account>) that
    survives `terraform destroy`, so redeploys reuse it and never touch the Let's
    Encrypt rate limit. Set true ONLY to (re)issue, then run ../bin/persist-cert.sh.
  EOT
  type        = bool
  default     = false
}
