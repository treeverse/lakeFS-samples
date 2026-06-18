variable "aws_region" {
  description = "AWS region to deploy into"
  default     = "us-east-1"
}

# ── Account-specific — no defaults; supply via terraform.tfvars ───────────────

variable "vpc_id" {
  description = "ID of the VPC to deploy into (e.g. vpc-0123456789abcdef0)"
  type        = string
}

variable "subnet_id" {
  description = "ID of a public subnet in the VPC for the EC2 instance and FSx"
  type        = string
}

variable "key_pair_name" {
  description = "Name of an EC2 key pair that already exists in this account/region"
  type        = string
}

# ── Secrets — no defaults; supply via terraform.tfvars (gitignored) ───────────

variable "fsxadmin_password" {
  description = "Password for the ONTAP fsxadmin and vsadmin users (min 8 chars, mixed case + number/special)"
  type        = string
  sensitive   = true
}
