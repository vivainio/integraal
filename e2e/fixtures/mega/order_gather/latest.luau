--!nonstrict
--!runtime 1
-- Joins the shards and hands the orders to the pricing system as a batch job.
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	local flow = event.flow
	local rows = {}
	for _, key in ipairs(ig.store.Out.list("orders/" .. flow .. "/")) do
		table.insert(rows, ig.json.decode(ig.store.Out.get(key)))
	end
	table.sort(rows, function(a, b) return a.id < b.id end)
	local job = igp.batch.new({ id = flow })
	job:input_rows("orders", rows)
	job:query("pricing", "price_orders", { input = "orders" })
	job:state({ flow = flow, count = #rows })
	job:continue_with("order_settle")
	return { job = job:submit() }
end

return { on_queue = handle, handle = handle }
