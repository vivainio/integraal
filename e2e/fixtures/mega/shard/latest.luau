--!nonstrict
--!runtime 1
-- One shard of a scatter: writes its part, counts itself once, and the last one sends the gather.
local function handle(_ctx, event)
	local flow, i = event.flow, event.i
	native.store.Out.put("shards/" .. flow .. "/" .. i, "part-" .. i)
	if not native.db.put_if_absent("flow-seen", flow .. "#" .. i, true) then
		return { counted = false }
	end
	local done = native.db.add("flow-done", flow)
	local total = native.db.get("flow", flow).total
	if done == total and native.db.put_if_absent("flow-gathered", flow, true) then
		native.queue.send({ script = "gather", event = { flow = flow } })
	end
	return { counted = true, done = done }
end

return { on_queue = handle, handle = handle }
