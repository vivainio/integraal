--!nonstrict
--!runtime 1
-- Exercises every native.db call and reports what it saw.
local function handle(_ctx, event)
	local k = event.k
	native.db.put(k, "a", { v = 1 })
	native.db.put(k, "b", { v = 2 })
	local out = {}
	out.get = native.db.get(k, "a")
	out.first = native.db.put_if_absent(k, "claim", "me")
	out.second = native.db.put_if_absent(k, "claim", "you")
	out.add1 = native.db.add(k, "n", 5)
	out.add2 = native.db.add(k, "n", -2)
	local rows = native.db.query(k, { sk_prefix = "a" })
	out.prefix_rows = #rows
	out.all_rows = #native.db.query(k)
	native.db.delete(k, "a")
	out.deleted = native.db.get(k, "a") == nil
	return out
end

return { handle = handle }
