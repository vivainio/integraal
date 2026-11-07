--!strict
-- Typed declarations for optional continuation guards.
local function handle(_ctx: Context, event: { key: string })
    local p = require("@native/patterns@1")
    local caps: RouteCapabilities = { stores = { Out = { read = { event.key } } } }
    ig.queue.send({ script = "send-report", event = event, capabilities = caps })
    ig.queue.handoff("report", { script = "send-report", event = event, capabilities = caps })
    ig.timer.after("report", 1, { script = "send-report", event = event, capabilities = caps })
    p.retry.schedule("report", { script = "send-report", event = event, capabilities = caps }, { attempt = 1 })
    p.callback.create({ script = "send-report", state = event, capabilities = caps })
    p.barrier.create({ id = "report", members = { "a" }, continue_with = "send-report", state = event, capabilities = caps })
    p.sequence.start("report", { script = "send-report", version = "v1", input = event, capabilities = caps })
    local job = p.batch.new()
    job:continue_with("send-report", nil, caps)
    return true
end
return { handle = handle }
