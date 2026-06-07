output "rc_elastic_ip" {
  value       = data.aws_eip.rc.public_ip
  description = "RC address for everything (SSH, XDN_CONTROL_PLANE :3300, and the xdnapp.com authoritative NS)."
}

output "ar_ipv6_addresses" {
  value       = local.ar_ipv6s
  description = "AR node id -> advertised global IPv6 (what coredns returns as AAAA for <service>.xdnapp.com). The ARs have no public IPv4."
}

output "cluster_node_addresses" {
  value = merge(
    { "cp0" = "${data.aws_eip.rc.public_ip} (RC us-east-1, IPv4 mgmt) / [${local.rc_ipv6}] (consensus)" },
    { for id, r in local.replicas : id => "[${local.ar_ipv6s[id]}] (AR ${r.region}, IPv6-only)" },
  )
  description = "Node id -> advertised consensus address. ARs are spread across regions; the RC stays in us-east-1."
}

output "regions" {
  value       = distinct([for id, r in local.replicas : r.region])
  description = "Regions hosting ActiveReplicas (RC is always in us-east-1)."
}

output "acme_delegation_ns" {
  value       = aws_route53_zone.acme.name_servers
  description = "Route53 nameservers for the delegated _acme-challenge zone. Verify: dig +short NS _acme-challenge.<base_domain> @<rc_elastic_ip>"
}
