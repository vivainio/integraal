#!/usr/bin/env bash
# Populate the running local Integraal instance with real successful and failing flows.
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_ENDPOINT_URL=http://localhost:5070 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
aws() { uvx --from awscli aws --endpoint-url "$AWS_ENDPOINT_URL" "$@"; }
aws s3 cp dev/flow-scenarios.luau s3://scripts/acme/order-import/latest.luau >/dev/null
aws s3 cp dev/flow-scenarios.luau s3://scripts/acme/customer-sync/latest.luau >/dev/null
aws s3 cp dev/order-pipeline.luau s3://scripts/acme/order-pipeline/v1.luau >/dev/null
aws s3 cp dev/order-pipeline.luau s3://scripts/acme/order-pipeline/latest.luau >/dev/null
aws s3 cp dev/user-greeting.luau s3://scripts/acme/user-greeting/latest.luau >/dev/null
aws s3 cp dev/batch-dispatch.luau s3://scripts/acme/batch-dispatch/latest.luau >/dev/null
for worker in prepare-item reserve-item notify-item; do
  aws s3 cp dev/batch-worker.luau "s3://scripts/acme/$worker/latest.luau" >/dev/null
done
aws s3 cp dev/flow-feed.xml s3://data-in/tenants/acme/incoming/feeds/catalog.xml >/dev/null

run_flow() {
  local path="$1" body="$2" expected="$3" status
  status=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 15 \
    -X POST "http://127.0.0.1:8080/v1/acme${path:+/$path}" \
    -H 'content-type: application/json' -d "$body")
  if [[ "$expected" == ok && "$status" != 200 ]] || \
     [[ "$expected" == error && ( "$status" -lt 400 || "$status" -ge 600 ) ]]; then
    echo "Unexpected HTTP $status for local $path flow (expected $expected)" >&2
    return 1
  fi
}
run_flow hello '{"name":"Aino"}' ok
run_flow feed '{"key":"incoming/feeds/catalog.xml"}' ok
run_flow order-import '{"action":"process"}' ok
run_flow customer-sync '{"action":"sync"}' ok
run_flow order-import '{"action":"invalid-order"}' error
run_flow customer-sync '{"action":"missing-customer"}' error
run_flow '' '{"key":"incoming/feeds/catalog.xml"}' ok
run_flow '' '{"message":"routed greeting"}' ok
workflow_id="local-order-$(date +%s)-$RANDOM"
run_flow batch-dispatch "{\"batch_id\":\"$workflow_id\"}" ok
run_flow order-pipeline "{\"id\":\"$workflow_id\"}" ok
# A minute tick dispatches durable handoffs; the normal worker consumes the resulting message.
aws sqs send-message --queue-url http://localhost:5070/123456789012/integraal-jobs.fifo --message-group-id local-tick --message-deduplication-id "seed-$workflow_id" \
  --message-body '{"type":"system.tick","every":"minute","at":"local-seed"}' >/dev/null
echo 'Created local flows, including two failures, two routed calls, queued tasks, and a durable order workflow.'
