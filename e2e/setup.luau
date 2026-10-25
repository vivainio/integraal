--!strict
-- Provisions every AWS resource the mega integration test needs; run by `roto-server --setup`.
-- Vars: FIXTURES (dir of the `mega` tenant's scripts), MEGALITH (base URL of the mock target, with a trailing slash).

local DLQ = roto.sqs.queue("jobs-dlq")
-- `jobs` is the queue the server polls; S3 notifications land here too.
local jobs = roto.sqs.queue("jobs", {
	visibility_timeout = 2,
	attributes = {
		RedrivePolicy = `\{"deadLetterTargetArn":"{DLQ.arn}","maxReceiveCount":"3"}`,
	},
})
local intake = roto.sqs.queue("intake")
roto.sqs.queue("jobs-small")
roto.sqs.queue("jobs-large")

roto.s3.bucket("scripts")
roto.s3.bucket("data-out")
roto.s3.bucket("data-temp")
roto.s3.bucket("data-in", {
	notify = { { queue = jobs, events = { "s3:ObjectCreated:*" } } },
})
-- Drops go to the intake queue, to be placed by size before any worker sees them.
roto.s3.bucket("data-drops", {
	notify = { { queue = intake, events = { "s3:ObjectCreated:*" } } },
})
roto.s3.bucket("batch", {
	notify = { { queue = jobs, events = { "s3:ObjectCreated:*" }, suffix = "done.json" } },
})

roto.dynamodb.table("integraal", { hash = "pk", range = "sk" })

-- The Lambda entrypoints run as the real `bootstrap` binaries, warm, behind roto's Runtime API.
local function bootstrap(name: string, bin: string)
	return roto.lambda.fn(name, {
		timeout = 10,
		environment = { INTEGRAAL_KEY_1 = roto.vars.KEY },
		executor = { runtime = { `{roto.vars.BIN_DIR}/{bin}`, "--config", roto.vars.LAMBDA_CFG } },
	})
end
bootstrap("integraal", "lambda") -- direct invoke
bootstrap("webhook", "webhook") -- behind --http-routes: POST /roto-http/cb/{tenant}/{token}
roto.lambda.event_source(intake, bootstrap("ingress", "ingress"), { batch_size = 10, report_batch_item_failures = true })

roto.s3.link_dir("scripts", "mega/", roto.vars.FIXTURES)

-- The tenant's config is built here because the allowlist needs megalith's (random) port.
roto.s3.put(
	"scripts",
	"mega/config.json",
	`\{"router":"router",`
		.. `"vars":\{"megalith":"{roto.vars.MEGALITH}"},`
		.. `"buckets":\{"In":"readwrite","Out":"readwrite","Temp":"readwrite","Drops":"readwrite"},`
		.. `"queue":true,"batch":true,"schedule":["hour"],`
		.. `"placement":\{"class":"small","classes":["large"],"script":"place"},`
		.. `"limits":\{"timeout_ms":20000},`
		.. `"http":\{"allow":["$\{megalith}"]}}`,
	{ content_type = "application/json" }
)

-- Every tenant, for the operator ticks.
roto.s3.put("scripts", "tenants.json", `["mega"]`, { content_type = "application/json" })
