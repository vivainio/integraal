#!/usr/bin/env bash
# Local end-to-end run against roto (https://github.com/vivainio/roto).
# Prereq: `uv tool install roto-aws` and `roto-server --port 5070` running.
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_ENDPOINT_URL=http://localhost:5070 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
export SCRIPT_BUCKET=scripts DATA_BUCKET=data DATA_PREFIX=tenants/ PORT=8080

uvx --from awscli aws s3 mb s3://scripts 2>/dev/null || true
uvx --from awscli aws s3 mb s3://data 2>/dev/null || true
uvx --from awscli aws s3 cp examples/acme/hello/latest.luau s3://scripts/acme/hello/latest.luau
uvx --from awscli aws s3 cp examples/acme/config.json s3://scripts/acme/config.json

echo "try: curl -XPOST localhost:8080/v1/acme/hello -d '{\"x\":1}' -H 'content-type: application/json'"
exec cargo run -p integraal-server
