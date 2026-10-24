#!/usr/bin/env bash
# Puts an application load balancer in front of the local server and sends a request through it.
# Prereq: dev/local.sh running, against `roto-server --port 5070 --lua-services <roto>/services --alb-port 8888`.
set -euo pipefail
export AWS_ENDPOINT_URL=http://localhost:5070 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
elb() { uvx --from awscli aws elbv2 "$@"; }

LB=$(elb create-load-balancer --name integraal --subnets subnet-1 subnet-2 \
  --query 'LoadBalancers[0].LoadBalancerArn' --output text)
DNS=$(elb describe-load-balancers --load-balancer-arns "$LB" --query 'LoadBalancers[0].DNSName' --output text)
# Same health check as the production target group: GET /healthz.
TG=$(elb create-target-group --name integraal-http --protocol HTTP --port 8080 --vpc-id vpc-1 --target-type ip \
  --health-check-path /healthz --health-check-interval-seconds 5 --health-check-timeout-seconds 2 \
  --healthy-threshold-count 2 --query 'TargetGroups[0].TargetGroupArn' --output text)
elb register-targets --target-group-arn "$TG" --targets Id=127.0.0.1,Port=8080
LISTENER=$(elb create-listener --load-balancer-arn "$LB" --protocol HTTP --port 80 \
  --default-actions "Type=forward,TargetGroupArn=$TG" --query 'Listeners[0].ListenerArn' --output text)

echo "waiting for the target to become healthy..."
until [ "$(elb describe-target-health --target-group-arn "$TG" \
  --query 'TargetHealthDescriptions[0].TargetHealth.State' --output text)" = healthy ]; do sleep 1; done

echo "via ALB: curl -XPOST http://$DNS:8888/v1/acme/hello"
curl -fsS -XPOST "http://$DNS:8888/v1/acme/hello" -d '{"x":1}' -H 'content-type: application/json'
echo
echo "listener: $LISTENER"
