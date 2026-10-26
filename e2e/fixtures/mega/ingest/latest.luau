--!nonstrict
--!runtime 1
-- Order file drop: streams a zip of XML orders into a scratch SQL table, saves it, and scatters one shard per order.
local function handle(_ctx, event)
	local flow = string.match(event.key, "incoming/orders/(.+)%.zip$")
	local db = ig.sql.open()
	db:exec("create table o (id text primary key, qty integer)")
	for entry in ig.zip.entries(ig.store.In.open(event.key)) do
		if not entry.is_dir then
			for ev in ig.xml.events(entry:open(), { trim = true }) do
				if ev.type == "start" and ev.tag == "order" then
					db:exec("insert into o (id, qty) values (?, ?)", { ev.attrs.id, tonumber(ev.attrs.qty) })
				end
			end
		end
	end
	local orders = db:query("select id, qty from o order by id")
	local sink = ig.store.Temp.create("flows/" .. flow .. ".sqlite")
	db:save(sink)
	sink:close()

	ig.db.put("flow", flow, { total = #orders })
	local msgs = {}
	for _, o in ipairs(orders) do
		table.insert(msgs, { script = "order_shard", event = { flow = flow, id = o.id, qty = o.qty } })
	end
	ig.queue.send(msgs)
	return { orders = #orders, delete = true }
end

return { on_queue = handle, handle = handle }
