--!nonstrict
--!runtime 1
-- Queue worker. `event.op` picks the behaviour; `event.id` names its marker objects.
local function handle(ctx, event)
	if event.op == "fail" then
		-- Recorded before failing so the test can see the delivery count of each attempt.
		native.store.Out.put("attempts/" .. event.id, tostring(ctx.attempt))
		error("boom")
	elseif event.op == "idem" then
		-- Run-once despite redelivery: only the first delivery claims the key.
		if native.db.put_if_absent("idem", event.id, true) then
			native.store.Out.put("idem/" .. event.id, tostring(native.db.add("idem-runs", event.id)))
		end
		return { claimed = false }
	elseif event.op == "scatter" then
		native.db.put("flow", event.flow, { total = event.n })
		local msgs = {}
		for i = 1, event.n do
			table.insert(msgs, { script = "shard", event = { flow = event.flow, i = i } })
		end
		return { sent = native.queue.send(msgs) }
	end
	native.store.Out.put("marks/" .. event.id, native.json.encode({ trigger = ctx.trigger, attempt = ctx.attempt }))
	return { ok = true }
end

return { on_queue = handle, handle = handle }
