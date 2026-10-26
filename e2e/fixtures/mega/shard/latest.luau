--!nonstrict
--!runtime 1
-- One shard of a scatter: writes its part, counts itself once, and the last one sends the gather.
local function handle(_ctx, event)
	local flow, i = event.flow, event.i
	ig.store.Out.put("shards/" .. flow .. "/" .. i, "part-" .. i)
	if not ig.db.put_if_absent("flow-seen", flow .. "#" .. i, true) then
		return { counted = false }
	end
	local done = ig.db.add("flow-done", flow)
	local total = ig.db.get("flow", flow).total
	if done == total and ig.db.put_if_absent("flow-gathered", flow, true) then
		ig.queue.send({ script = "gather", event = { flow = flow } })
	end
	return { counted = true, done = done }
end

return { on_queue = handle, handle = handle }
