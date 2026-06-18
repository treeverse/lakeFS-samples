#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# setup-demo.sh — Run this on EC2 after Terraform provisions the infrastructure
# Usage: bash setup-demo.sh <SVM_MANAGEMENT_IP> <ONTAP_ACCESS_KEY> <ONTAP_SECRET_KEY>
# ─────────────────────────────────────────────────────────────────────────────

set -e

SVM_IP="${1:?Usage: $0 <SVM_IP> <ONTAP_ACCESS_KEY> <ONTAP_SECRET_KEY>}"
ONTAP_ACCESS_KEY="${2:?missing ONTAP_ACCESS_KEY}"
ONTAP_SECRET_KEY="${3:?missing ONTAP_SECRET_KEY}"

# Random encryption secret for lakeFS auth (generated fresh per run).
AUTH_ENCRYPT_SECRET_KEY="$(openssl rand -hex 20)"

echo "==> Installing dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq docker.io python3-pip

sudo systemctl start docker
sudo usermod -aG docker ubuntu

echo "==> Starting Postgres..."
sudo docker run -d --name postgres \
  -e POSTGRES_USER=lakefs \
  -e POSTGRES_PASSWORD=lakefs \
  -e POSTGRES_DB=lakefs \
  -p 5432:5432 \
  --restart unless-stopped \
  postgres:15-alpine

echo "==> Installing lakeFS..."
LAKEFS_VERSION=$(curl -s https://api.github.com/repos/treeverse/lakeFS/releases/latest \
  | grep '"tag_name"' | cut -d'"' -f4 | sed 's/v//')
curl -sL "https://github.com/treeverse/lakeFS/releases/download/v${LAKEFS_VERSION}/lakeFS_${LAKEFS_VERSION}_Linux_x86_64.tar.gz" | tar xz
sudo mv lakefs /usr/local/bin/

echo "==> Writing lakeFS config..."
cat > ~/lakefs.yaml << EOF
database:
  type: postgres
  postgres:
    connection_string: "postgresql://lakefs:lakefs@localhost:5432/lakefs?sslmode=disable"

auth:
  encrypt:
    secret_key: "${AUTH_ENCRYPT_SECRET_KEY}"

installation:
  user_name: admin
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

blockstore:
  type: s3
  s3:
    endpoint: "http://${SVM_IP}"
    force_path_style: true
    region: "us-east-1"
    credentials:
      access_key_id: "${ONTAP_ACCESS_KEY}"
      secret_access_key: "${ONTAP_SECRET_KEY}"

logging:
  level: "INFO"
  format: "text"
EOF

echo "==> Installing Python demo dependencies..."
pip3 install -q requests

echo "==> Starting lakeFS..."
nohup lakefs run --config ~/lakefs.yaml > ~/lakefs.log 2>&1 &
sleep 3

echo ""
echo "✓ Setup complete!"
echo "  lakeFS UI: http://$(curl -s checkip.amazonaws.com):8000"
echo "  Complete setup at: http://$(curl -s checkip.amazonaws.com):8000/setup"
echo ""
echo "  lakeFS admin credentials (set during /setup):"
echo "  Save the Access Key ID and Secret Key shown in the browser."
