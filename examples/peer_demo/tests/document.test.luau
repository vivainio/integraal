--!strict
local ctx: Context = {
    tenant = "peer_demo", script = "upload", trigger = "invoke", runtime = 1, attempt = 1,
}

test.case("uploaded document is acknowledged by the peer", function()
    local upload = test.load("upload/latest.luau")
    local result = upload.handle(ctx, { body = '<invoice id="42"/>', reference = "INV-42" })

    local document = test.peer.receive(result.job, "document")
    test.equal(document.body, '<invoice id="42"/>')
    test.equal(document.request.queries[1].query, "accept_document")

    local event = test.peer.ack(document, { results = { receipt = { { accepted = true } } } })
    local routed = native.batch.route(event)
    assert(routed ~= nil and routed.script == "received")
    local continuation = test.load(routed.script .. "/latest.luau")
    local queue_ctx: Context = {
        tenant = ctx.tenant, script = routed.script, trigger = "queue", runtime = 1, attempt = 1,
    }
    local completed = continuation.on_queue(queue_ctx, routed.event)
    test.equal(completed.reference, "INV-42")
    test.equal(completed.receipts, { { accepted = true } })
end)

test.case("peer can reject an uploaded document", function()
    local result = test.load("upload/latest.luau").handle(ctx, { body = "invalid", reference = "BAD" })
    local document = test.peer.receive(result.job, "document")
    local event = test.peer.ack(document, { ok = false, error = "invalid invoice" })
    local routed = native.batch.route(event)
    assert(routed ~= nil and routed.script ~= nil)
    local queue_ctx: Context = {
        tenant = ctx.tenant, script = routed.script, trigger = "queue", runtime = 1, attempt = 1,
    }
    local completed = test.load(routed.script .. "/latest.luau").on_queue(queue_ctx, routed.event)
    test.equal(completed, { accepted = false, error = "invalid invoice" })
end)
