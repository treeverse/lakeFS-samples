output "ec2_public_ip" {
  description = "EC2 public IP (stable — Elastic IP)"
  value       = aws_eip.lakefs.public_ip
}

output "lakefs_ui" {
  description = "lakeFS UI URL"
  value       = "http://${aws_eip.lakefs.public_ip}:8000"
}

output "fsx_management_ip" {
  description = "FSx filesystem management IP (for fsxadmin SSH)"
  value       = aws_fsx_ontap_file_system.demo.endpoints[0].management[0].ip_addresses
}

output "svm_management_ip" {
  description = "SVM management IP (ONTAP S3 endpoint for lakeFS)"
  value       = aws_fsx_ontap_storage_virtual_machine.demo.endpoints[0].management[0].ip_addresses
}

output "ssh_command" {
  description = "SSH command to connect to EC2"
  value       = "ssh -i ~/.ssh/lakefs-ontap-demo.pem ubuntu@${aws_eip.lakefs.public_ip}"
}

output "ontap_s3_endpoint" {
  description = "Public ONTAP S3 endpoint via NLB (use as pre_signed_endpoint in lakeFS config)"
  value       = "http://${aws_lb.ontap_s3.dns_name}"
}
