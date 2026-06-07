# ===========================================================================
# Per-region networking. Each region gets a dual-stack VPC (auto-assigned IPv6
# /56), an internet gateway, one /64 subnet per node, a default route table, and
# a security group. The control region (us-east-1) additionally hosts the RC.
# ===========================================================================

# ---- us-east-1 (control region: RC + its ARs) -----------------------------
resource "aws_vpc" "use1" {
  cidr_block                       = "10.0.0.0/16"
  assign_generated_ipv6_cidr_block = true
  tags                             = { Name = "xdn-mr-vpc-us-east-1" }
}

resource "aws_internet_gateway" "use1" {
  vpc_id = aws_vpc.use1.id
  tags   = { Name = "xdn-mr-igw-us-east-1" }
}

resource "aws_route_table" "use1" {
  vpc_id = aws_vpc.use1.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.use1.id
  }
  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.use1.id
  }
  tags = { Name = "xdn-mr-rt-us-east-1" }
}

# RC subnet (slot 0 in the us-east-1 VPC).
resource "aws_subnet" "rc" {
  vpc_id                          = aws_vpc.use1.id
  availability_zone               = "us-east-1a"
  cidr_block                      = cidrsubnet(aws_vpc.use1.cidr_block, 8, 0)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.use1.ipv6_cidr_block, 8, 0)
  map_public_ip_on_launch         = true
  assign_ipv6_address_on_creation = true
  tags                            = { Name = "xdn-mr-subnet-rc" }
}

# us-east-1 AR subnets (slots 1..N).
resource "aws_subnet" "ar_use1" {
  for_each                        = toset(local.ar_ids_use1)
  vpc_id                          = aws_vpc.use1.id
  availability_zone               = local.replicas[each.key].az
  cidr_block                      = cidrsubnet(aws_vpc.use1.cidr_block, 8, index(local.ar_ids_use1, each.key) + 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.use1.ipv6_cidr_block, 8, index(local.ar_ids_use1, each.key) + 1)
  map_public_ip_on_launch         = false
  assign_ipv6_address_on_creation = true
  tags                            = { Name = "xdn-mr-subnet-${each.key}" }
}

resource "aws_route_table_association" "rc" {
  subnet_id      = aws_subnet.rc.id
  route_table_id = aws_route_table.use1.id
}

resource "aws_route_table_association" "ar_use1" {
  for_each       = aws_subnet.ar_use1
  subnet_id      = each.value.id
  route_table_id = aws_route_table.use1.id
}

resource "aws_security_group" "use1" {
  name        = "xdn-mr-us-east-1"
  description = "XDN multi-region: data plane + management (internet) and consensus (cluster IPv6)"
  vpc_id      = aws_vpc.use1.id

  dynamic "ingress" {
    for_each = local.sg_ingress
    content {
      description      = ingress.value.desc
      from_port        = ingress.value.from
      to_port          = ingress.value.to
      protocol         = ingress.value.proto
      cidr_blocks      = ingress.value.v4
      ipv6_cidr_blocks = ingress.value.v6
    }
  }
  ingress {
    description = "All traffic between members of this SG"
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
  tags = { Name = "xdn-mr-us-east-1" }
}

# ---- us-east-2 ------------------------------------------------------------
resource "aws_vpc" "use2" {
  provider                         = aws.use2
  cidr_block                       = "10.1.0.0/16"
  assign_generated_ipv6_cidr_block = true
  tags                             = { Name = "xdn-mr-vpc-us-east-2" }
}

resource "aws_internet_gateway" "use2" {
  provider = aws.use2
  vpc_id   = aws_vpc.use2.id
  tags     = { Name = "xdn-mr-igw-us-east-2" }
}

resource "aws_route_table" "use2" {
  provider = aws.use2
  vpc_id   = aws_vpc.use2.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.use2.id
  }
  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.use2.id
  }
  tags = { Name = "xdn-mr-rt-us-east-2" }
}

resource "aws_subnet" "ar_use2" {
  provider                        = aws.use2
  for_each                        = toset(local.ar_ids_use2)
  vpc_id                          = aws_vpc.use2.id
  availability_zone               = local.replicas[each.key].az
  cidr_block                      = cidrsubnet(aws_vpc.use2.cidr_block, 8, index(local.ar_ids_use2, each.key))
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.use2.ipv6_cidr_block, 8, index(local.ar_ids_use2, each.key))
  map_public_ip_on_launch         = false
  assign_ipv6_address_on_creation = true
  tags                            = { Name = "xdn-mr-subnet-${each.key}" }
}

resource "aws_route_table_association" "ar_use2" {
  provider       = aws.use2
  for_each       = aws_subnet.ar_use2
  subnet_id      = each.value.id
  route_table_id = aws_route_table.use2.id
}

resource "aws_security_group" "use2" {
  provider    = aws.use2
  name        = "xdn-mr-us-east-2"
  description = "XDN multi-region: data plane + management (internet) and consensus (cluster IPv6)"
  vpc_id      = aws_vpc.use2.id

  dynamic "ingress" {
    for_each = local.sg_ingress
    content {
      description      = ingress.value.desc
      from_port        = ingress.value.from
      to_port          = ingress.value.to
      protocol         = ingress.value.proto
      cidr_blocks      = ingress.value.v4
      ipv6_cidr_blocks = ingress.value.v6
    }
  }
  ingress {
    description = "All traffic between members of this SG"
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
  tags = { Name = "xdn-mr-us-east-2" }
}

# ---- us-west-2 ------------------------------------------------------------
resource "aws_vpc" "usw2" {
  provider                         = aws.usw2
  cidr_block                       = "10.2.0.0/16"
  assign_generated_ipv6_cidr_block = true
  tags                             = { Name = "xdn-mr-vpc-us-west-2" }
}

resource "aws_internet_gateway" "usw2" {
  provider = aws.usw2
  vpc_id   = aws_vpc.usw2.id
  tags     = { Name = "xdn-mr-igw-us-west-2" }
}

resource "aws_route_table" "usw2" {
  provider = aws.usw2
  vpc_id   = aws_vpc.usw2.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.usw2.id
  }
  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.usw2.id
  }
  tags = { Name = "xdn-mr-rt-us-west-2" }
}

resource "aws_subnet" "ar_usw2" {
  provider                        = aws.usw2
  for_each                        = toset(local.ar_ids_usw2)
  vpc_id                          = aws_vpc.usw2.id
  availability_zone               = local.replicas[each.key].az
  cidr_block                      = cidrsubnet(aws_vpc.usw2.cidr_block, 8, index(local.ar_ids_usw2, each.key))
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.usw2.ipv6_cidr_block, 8, index(local.ar_ids_usw2, each.key))
  map_public_ip_on_launch         = false
  assign_ipv6_address_on_creation = true
  tags                            = { Name = "xdn-mr-subnet-${each.key}" }
}

resource "aws_route_table_association" "ar_usw2" {
  provider       = aws.usw2
  for_each       = aws_subnet.ar_usw2
  subnet_id      = each.value.id
  route_table_id = aws_route_table.usw2.id
}

resource "aws_security_group" "usw2" {
  provider    = aws.usw2
  name        = "xdn-mr-us-west-2"
  description = "XDN multi-region: data plane + management (internet) and consensus (cluster IPv6)"
  vpc_id      = aws_vpc.usw2.id

  dynamic "ingress" {
    for_each = local.sg_ingress
    content {
      description      = ingress.value.desc
      from_port        = ingress.value.from
      to_port          = ingress.value.to
      protocol         = ingress.value.proto
      cidr_blocks      = ingress.value.v4
      ipv6_cidr_blocks = ingress.value.v6
    }
  }
  ingress {
    description = "All traffic between members of this SG"
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
  tags = { Name = "xdn-mr-us-west-2" }
}
