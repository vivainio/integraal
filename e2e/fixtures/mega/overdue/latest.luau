--!nonstrict
--!runtime 1
-- A barrier past its deadline; records what `status` says, since the barrier may have finished meanwhile.
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	local status = igp.barrier.status(event.barrier)
	ig.store.Out.put("overdue/" .. event.barrier, ig.json.encode({ type = event.type, complete = status.complete }))
end

return { on_queue = handle, handle = handle }
