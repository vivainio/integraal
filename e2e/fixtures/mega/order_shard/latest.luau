--!nonstrict
--!runtime 1
-- One shard: enriches its order from the CRM (vault credential, serialized by a lock), writes the part,
-- and the shard that completes the count sends the gather.
local function handle(_ctx, event)
	local flow = event.flow
	local got, tier = native.lock.with("crm", { scope = "db", wait_ms = 20000 }, function()
		local r = native.http.request({
			url = native.config.megalith .. "crm/" .. event.id,
			headers = { authorization = "Bearer {{vault:crm}}" },
			vault = { "header" },
		})
		return native.json.decode(r.body).tier
	end)
	assert(got, "crm lock not acquired")
	native.store.Out.put("orders/" .. flow .. "/" .. event.id, native.json.encode({ id = event.id, qty = event.qty, tier = tier }))
	if not native.db.put_if_absent("flow-seen", flow .. "#" .. event.id, true) then
		return { counted = false }
	end
	local done = native.db.add("flow-done", flow)
	if done == native.db.get("flow", flow).total and native.db.put_if_absent("flow-gathered", flow, true) then
		native.queue.send({ script = "order_gather", event = { flow = flow } })
	end
	return { counted = true }
end

return { on_queue = handle, handle = handle }
