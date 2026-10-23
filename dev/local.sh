#!/usr/bin/env bash
# Local end-to-end run against roto (https://github.com/vivainio/roto).
# Prereq: `uv tool install roto-aws` and `roto-server --port 5070` running.
set -euo pipefail
cd "$(dirname "$0")/.."
# Only the AWS SDK variables are environment; everything else is in dev/local.json.
export AWS_ENDPOINT_URL=http://localhost:5070 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
DB_TABLE=integraal

uvx --from awscli aws s3 mb s3://scripts
uvx --from awscli aws s3 mb s3://data-in
uvx --from awscli aws s3 mb s3://data-out
uvx --from awscli aws s3 mb s3://data-temp
aws() { uvx --from awscli aws "$@"; }
aws dynamodb create-table --table-name "$DB_TABLE" --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE >/dev/null
aws dynamodb update-time-to-live --table-name "$DB_TABLE" \
  --time-to-live-specification Enabled=true,AttributeName=ttl >/dev/null
for s in hello feed router; do
  uvx --from awscli aws s3 cp examples/acme/$s/latest.luau s3://scripts/acme/$s/latest.luau
done
uvx --from awscli aws s3 cp examples/acme/config.json s3://scripts/acme/config.json

echo "routed: curl -XPOST localhost:8080/v1/acme -d '{\"kind\":\"x\"}' -H 'content-type: application/json'"
echo "try: curl -XPOST localhost:8080/v1/acme/hello -d '{\"x\":1}' -H 'content-type: application/json'"
exec cargo run -p integraal-server -- --config dev/local.json
