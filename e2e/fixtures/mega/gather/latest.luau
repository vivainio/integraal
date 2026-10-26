--!nonstrict
--!runtime 1
-- Joins the shards of a scatter. Counts its own runs so the test can prove it ran exactly once.
local function handle(_ctx, event)
	local parts = ig.store.Out.list("shards/" .. event.flow .. "/")
	table.sort(parts)
	local runs = ig.db.add("gather-runs", event.flow)
	ig.store.Out.put("gathered/" .. event.flow, ig.json.encode({ parts = #parts, runs = runs }))
	return { parts = #parts }
end

return { on_queue = handle, handle = handle }
