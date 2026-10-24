--!nonstrict
--!runtime 1
-- Submits a batch job with two independent queries (the megalith side answers them later).
local function handle(_ctx, event)
	local job = native.batch.new({ id = event.id })
	job:query("customers", "customers_by_id", { ids = event.ids })
	job:query("orders", "open_orders")
	job:state({ ref = event.ref })
	job:continue_with("batch_done")
	return { job = job:submit() }
end

return { handle = handle }
