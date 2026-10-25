--!nonstrict
--!runtime 1
-- A barrier past its deadline; records what `status` says, since the barrier may have finished meanwhile.
local function handle(_ctx, event)
	local status = native.barrier.status(event.barrier)
	native.bucket.Out.put("overdue/" .. event.barrier, native.json.encode({ type = event.type, complete = status.complete }))
end

return { on_queue = handle, handle = handle }
