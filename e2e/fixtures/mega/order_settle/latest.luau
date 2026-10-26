--!nonstrict
--!runtime 1
-- Pricing is back: totals it, then asks the approval system to call back before the flow is finalized.
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	local job = igp.batch.hydrate(event)
	if not job then
		return nil
	end
	local total = 0
	for row in job:rows("pricing") do
		total += row.price
	end
	local callback = igp.callback.create({
		script = "order_final",
		state = { flow = job.state.flow, count = job.state.count, total = total },
	})
	ig.http.request({
		method = "POST",
		url = ig.config.megalith .. "approval",
		body = ig.json.encode({ flow = job.state.flow, total = total, callback = callback }),
		basic_auth = { username = "svc", password = "{{vault:crm}}" },
		vault = { "basic_auth" },
	})
	return { delete = job:keys() }
end

return { on_queue = handle, handle = handle }
