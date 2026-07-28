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

# ── ONTAP S3 exposure ─────────────────────────────────────────────────────────
# The ONTAP S3 endpoint is served over plaintext HTTP (port 80). It must be
# reachable from wherever pre-signed URLs are resolved — for the `everest mount`
# step in SETUP_GUIDE.md that is the EC2 host, so the default below (your current
# public IP) is sufficient. Widen this only if you know you need to, and never
# to 0.0.0.0/0 outside a throwaway demo: S3 traffic and pre-signed URL
# signatures would travel unencrypted across the public internet.
variable "ontap_s3_allowed_cidrs" {
  description = "CIDRs allowed to reach the ONTAP S3 endpoint over HTTP. Defaults to your current public IP."
  type        = list(string)
  default     = null
}

# ── Secrets — no defaults; supply via terraform.tfvars (gitignored) ───────────

variable "fsxadmin_password" {
  description = "Password for the ONTAP fsxadmin and vsadmin users (min 8 chars, mixed case + number/special)"
  type        = string
  sensitive   = true
}
