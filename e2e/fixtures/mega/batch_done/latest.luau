--!nonstrict
--!runtime 1
-- Continuation: sees every result of the job at once, reports it, cleans the job up.
local igp = require("@native/patterns@1")
local function handle(ctx, event)
	local job = igp.batch.hydrate(event, { allow_failure = true })
	if not job then
		return nil
	end
	local report = { ok = job.ok, error = job.error, state = job.state, runs = ig.db.add("batch-runs", job.id) }
	if job.ok then
		local customers, orders = {}, {}
		for row in job:rows("customers") do
			table.insert(customers, row.name)
		end
		for row in job:rows("orders") do
			table.insert(orders, row.id)
		end
		report.customers, report.orders = customers, orders
	end
	ig.store.Out.put("batch/" .. job.id, ig.json.encode(report))
	return { delete = job:keys() }
end

return { on_queue = handle, handle = handle }
