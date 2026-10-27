--!strict
--!runtime 1
-- Deploy under operations.operator_tenant and invoke directly through Lambda.
local ops = require("ops")

local function handle(_ctx: Context, event: any): any
    if event.action == "pause-fleet" then
        return ops.fleet.pause(event.reason)
    elseif event.action == "resume-fleet" then
        return ops.fleet.resume(event.reason)
    elseif event.action == "pause-tenant" then
        return ops.tenant.pause(event.tenant, event.reason)
    elseif event.action == "resume-tenant" then
        return ops.tenant.resume(event.tenant, event.reason)
    elseif event.action == "inspect-flow" then
        return ops.flow.inspect(event.tenant, event.trace_id)
    elseif event.action == "inspect-dlq" then
        return ops.dlq.inspect(event.queue)
    elseif event.action == "replay-dlq" then
        return ops.dlq.replay(event.queue, event.message_id, event.reason)
    elseif event.action == "status" then
        return ops.fleet.status()
    end
    error("unknown operator action")
end

return { handle = handle }
