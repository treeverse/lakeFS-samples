terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ─── Data sources ────────────────────────────────────────────────────────────

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  my_ip = "${chomp(data.http.my_ip.response_body)}/32"
}

# ─── Security Groups ─────────────────────────────────────────────────────────

resource "aws_security_group" "ec2" {
  name        = "lakefs-ontap-demo-sg"
  description = "lakeFS demo EC2 security group"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_ip]
    description = "SSH from my IP"
  }

  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = [local.my_ip]
    description = "lakeFS UI"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "lakefs-ontap-demo-sg" }
}

resource "aws_security_group" "fsx" {
  name        = "lakefs-ontap-fsx-sg"
  description = "FSx for ONTAP security group"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = [aws_security_group.ec2.id]
    description     = "All traffic from EC2"
  }

  # PoC only: exposes ONTAP S3 publicly over plaintext HTTP for pre-signed URL
  # support. For non-demo use, scope cidr_blocks to a known IP and front with TLS.
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "ONTAP S3 HTTP via NLB for pre-signed URLs"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "lakefs-ontap-fsx-sg" }
}

# ─── FSx for NetApp ONTAP ─────────────────────────────────────────────────────

resource "aws_fsx_ontap_file_system" "demo" {
  storage_capacity     = 1024
  subnet_ids           = [var.subnet_id]
  preferred_subnet_id  = var.subnet_id
  deployment_type      = "SINGLE_AZ_2"
  throughput_capacity  = 384
  fsx_admin_password   = var.fsxadmin_password
  security_group_ids   = [aws_security_group.fsx.id]

  tags = { Name = "lakefs-ontap-demo" }
}

resource "aws_fsx_ontap_storage_virtual_machine" "demo" {
  file_system_id             = aws_fsx_ontap_file_system.demo.id
  name                       = "fsx"
  root_volume_security_style = "UNIX"
  svm_admin_password         = var.fsxadmin_password

  tags = { Name = "lakefs-ontap-demo-svm" }
}

resource "aws_fsx_ontap_volume" "demo" {
  name                       = "vol1"
  junction_path              = "/vol1"
  size_in_megabytes          = 1024000
  storage_efficiency_enabled = true
  storage_virtual_machine_id = aws_fsx_ontap_storage_virtual_machine.demo.id

  tags = { Name = "lakefs-ontap-demo-vol" }
}

# ─── EC2 Instance ────────────────────────────────────────────────────────────

resource "aws_instance" "lakefs" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = "t3.small"
  subnet_id                   = var.subnet_id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  key_name                    = var.key_pair_name

  root_block_device {
    volume_size = 20
  }

  tags = { Name = "lakefs-ontap-demo" }
}

# ─── NLB — exposes ONTAP S3 publicly for pre-signed URL support ──────────────

resource "aws_lb" "ontap_s3" {
  name               = "lakefs-ontap-s3"
  internal           = false
  load_balancer_type = "network"
  subnets            = [var.subnet_id]

  tags = { Name = "lakefs-ontap-s3-nlb" }
}

resource "aws_lb_target_group" "ontap_s3" {
  name        = "lakefs-ontap-s3-tg"
  port        = 80
  protocol    = "TCP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    protocol = "TCP"
    port     = 80
  }
}

resource "aws_lb_target_group_attachment" "ontap_s3" {
  target_group_arn = aws_lb_target_group.ontap_s3.arn
  target_id        = tolist(aws_fsx_ontap_storage_virtual_machine.demo.endpoints[0].management[0].ip_addresses)[0]
  port             = 80
}

resource "aws_lb_listener" "ontap_s3" {
  load_balancer_arn = aws_lb.ontap_s3.arn
  port              = 80
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ontap_s3.arn
  }
}

# ─── Elastic IP (so IP never changes between stops/starts) ───────────────────

resource "aws_eip" "lakefs" {
  instance = aws_instance.lakefs.id
  domain   = "vpc"
  tags     = { Name = "lakefs-ontap-demo-eip" }
}
