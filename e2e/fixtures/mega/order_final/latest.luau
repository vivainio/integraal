--!nonstrict
--!runtime 1
-- The approval system called back: writes the flow's final report.
local function handle(_ctx, event)
	local state = event.state
	ig.store.Out.put("reports/" .. state.flow, ig.json.encode({
		flow = state.flow,
		orders = state.count,
		total = state.total,
		approved = ig.json.decode(event.body).approved,
		gather_runs = ig.db.get("flow-gathered", state.flow) ~= nil,
	}))
end

return { on_queue = handle, handle = handle }
