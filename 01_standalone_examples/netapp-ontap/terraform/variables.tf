variable "aws_region" {
  description = "AWS region to deploy into"
  default     = "us-east-1"
}

# Prefixes every resource name. Load balancer and target group names are unique
# per region, so change this if you want more than one of these environments in
# the same account/region, or if a previous teardown left resources behind.
variable "name_prefix" {
  description = "Prefix for all created resource names"
  type        = string
  default     = "lakefs-ontap-demo"

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9-]{0,20}$", var.name_prefix))
    error_message = "name_prefix must start with a letter, contain only letters, digits and hyphens, and be at most 21 characters (load balancer names are capped at 32)."
  }
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
