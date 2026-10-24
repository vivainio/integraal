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
roto.sqs.queue("intake")
roto.sqs.queue("jobs-small")
roto.sqs.queue("jobs-large")

roto.s3.bucket("scripts")
roto.s3.bucket("data-out")
roto.s3.bucket("data-temp")
roto.s3.bucket("data-in", {
	notify = { { queue = jobs, events = { "s3:ObjectCreated:*" } } },
})
roto.s3.bucket("batch", {
	notify = { { queue = jobs, events = { "s3:ObjectCreated:*" }, suffix = "done.json" } },
})

roto.dynamodb.table("integraal", { hash = "pk", range = "sk" })

roto.s3.link_dir("scripts", "mega/", roto.vars.FIXTURES)

-- The tenant's config is built here because the allowlist needs megalith's (random) port.
roto.s3.put(
	"scripts",
	"mega/config.json",
	`\{"router":"router",`
		.. `"vars":\{"megalith":"{roto.vars.MEGALITH}"},`
		.. `"buckets":\{"In":"readwrite","Out":"readwrite","Temp":"readwrite"},`
		.. `"queue":true,"batch":true,`
		.. `"limits":\{"timeout_ms":20000},`
		.. `"http":\{"allow":["$\{megalith}"]}}`,
	{ content_type = "application/json" }
)
