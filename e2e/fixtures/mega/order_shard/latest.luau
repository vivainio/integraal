--!nonstrict
--!runtime 1
-- One shard: enriches its order from the CRM (vault credential, serialized by a lock), writes the part,
-- and the shard that completes the count sends the gather.
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	local flow = event.flow
	local got, tier = igp.lock.with("crm", { scope = "db", wait_ms = 20000 }, function()
		local r = ig.http.request({
			url = ig.config.megalith .. "crm/" .. event.id,
			headers = { authorization = "Bearer {{vault:crm}}" },
			vault = { "header" },
		})
		return ig.json.decode(r.body).tier
	end)
	assert(got, "crm lock not acquired")
	ig.store.Out.put("orders/" .. flow .. "/" .. event.id, ig.json.encode({ id = event.id, qty = event.qty, tier = tier }))
	if not ig.db.put_if_absent("flow-seen", flow .. "#" .. event.id, true) then
		return { counted = false }
	end
	local done = ig.db.add("flow-done", flow)
	if done == ig.db.get("flow", flow).total and ig.db.put_if_absent("flow-gathered", flow, true) then
		ig.queue.send({ script = "order_gather", event = { flow = flow } })
	end
	return { counted = true }
end

return { on_queue = handle, handle = handle }
