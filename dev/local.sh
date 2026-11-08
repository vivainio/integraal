#!/usr/bin/env bash
# Local end-to-end run against roto (https://github.com/vivainio/roto).
# Prereq: `uv tool install roto-aws` and `roto-server --port 5070` running.
# For dev/alb.sh also pass `--lua-services <roto>/services --alb-port 8888` to roto-server.
set -euo pipefail
cd "$(dirname "$0")/.."
# Only the AWS SDK variables (and optional OTEL_*, see docs/observability.md) are environment; everything else is in dev/local.json.
# AWS_DEFAULT_REGION is for the awscli v1 that uvx installs, which ignores AWS_REGION.
export AWS_ENDPOINT_URL=http://localhost:5070 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
DB_TABLE=integraal

uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" s3 mb s3://scripts
uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" s3 mb s3://data-in
uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" s3 mb s3://data-out
uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" s3 mb s3://data-temp
uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" s3 mb s3://flow-logs
aws() { uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" "$@"; }
aws dynamodb create-table --table-name "$DB_TABLE" --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE >/dev/null
aws sqs create-queue --queue-name integraal-jobs.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true >/dev/null
aws dynamodb update-time-to-live --table-name "$DB_TABLE" \
  --time-to-live-specification Enabled=true,AttributeName=ttl >/dev/null
for s in hello feed router; do
  uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" s3 cp examples/acme/$s/latest.luau s3://scripts/acme/$s/latest.luau
done
uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" s3 cp dev/acme-local.json s3://scripts/acme/config.json

echo "routed: curl -XPOST localhost:8080/v1/acme -d '{\"kind\":\"x\"}' -H 'content-type: application/json'"
echo "try: curl -XPOST localhost:8080/v1/acme/hello -d '{\"x\":1}' -H 'content-type: application/json'"
# Seed only after this server is healthy. Keep it as our child so startup failures and signals
# terminate the launcher rather than leaving a background server behind.
cargo run -p integraal-server -- --config dev/local.json &
server_pid=$!
cleanup() { kill "$server_pid" 2>/dev/null || true; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for ((attempt = 0; attempt < 600; attempt++)); do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid"
    exit 1
  fi
  if curl -fsS --max-time 1 http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS --max-time 1 http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
  echo 'Integraal did not become ready.' >&2
  exit 1
fi
dev/seed-flows.sh
wait "$server_pid"
