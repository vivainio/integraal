--!strict
-- Provisions every AWS resource the mega integration test needs; run by `roto-server --setup`.
-- Fixture scripts for the `mega` tenant are uploaded from roto.vars.FIXTURES.

local DLQ = roto.sqs.queue("jobs-dlq")
local jobs = roto.sqs.queue("jobs", {
	visibility_timeout = 2,
	attributes = {
		RedrivePolicy = `\{"deadLetterTargetArn":"{DLQ.arn}","maxReceiveCount":"3"}`,
	},
})
roto.sqs.queue("intake")
roto.sqs.queue("jobs-small")
roto.sqs.queue("jobs-large")
local events = roto.sqs.queue("s3-events")

roto.s3.bucket("scripts")
roto.s3.bucket("data-out")
roto.s3.bucket("data-temp")
roto.s3.bucket("data-in", {
	notify = { { queue = events, events = { "s3:ObjectCreated:*" } } },
})
roto.s3.bucket("batch", {
	notify = { { queue = events, events = { "s3:ObjectCreated:*" }, suffix = "done.json" } },
})

roto.dynamodb.table("integraal", { hash = "pk", range = "sk" })

roto.s3.link_dir("scripts", "mega/", roto.vars.FIXTURES)
