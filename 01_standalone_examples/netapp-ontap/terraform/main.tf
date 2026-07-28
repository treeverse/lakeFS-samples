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
  name        = "${var.name_prefix}-sg"
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

  tags = { Name = "${var.name_prefix}-sg" }
}

resource "aws_security_group" "fsx" {
  name        = "${var.name_prefix}-fsx-sg"
  description = "FSx for ONTAP security group"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = [aws_security_group.ec2.id]
    description     = "All traffic from EC2"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name_prefix}-fsx-sg" }
}

# ─── FSx for NetApp ONTAP ─────────────────────────────────────────────────────

resource "aws_fsx_ontap_file_system" "demo" {
  storage_capacity    = 1024
  subnet_ids          = [var.subnet_id]
  preferred_subnet_id = var.subnet_id
  deployment_type     = "SINGLE_AZ_2"
  throughput_capacity = 384
  fsx_admin_password  = var.fsxadmin_password
  security_group_ids  = [aws_security_group.fsx.id]

  tags = { Name = var.name_prefix }
}

resource "aws_fsx_ontap_storage_virtual_machine" "demo" {
  file_system_id             = aws_fsx_ontap_file_system.demo.id
  name                       = "fsx"
  root_volume_security_style = "UNIX"
  svm_admin_password         = var.fsxadmin_password

  tags = { Name = "${var.name_prefix}-svm" }
}

resource "aws_fsx_ontap_volume" "demo" {
  name                       = "vol1"
  junction_path              = "/vol1"
  size_in_megabytes          = 1024000
  storage_efficiency_enabled = true
  storage_virtual_machine_id = aws_fsx_ontap_storage_virtual_machine.demo.id

  tags = { Name = "${var.name_prefix}-vol" }
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

  tags = { Name = var.name_prefix }
}

# ─── Elastic IP (so IP never changes between stops/starts) ───────────────────

resource "aws_eip" "lakefs" {
  instance = aws_instance.lakefs.id
  domain   = "vpc"
  tags     = { Name = "${var.name_prefix}-eip" }
}
