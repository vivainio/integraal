--!nonstrict
--!runtime 1
-- Exercises every ig.db call and reports what it saw.
local function handle(_ctx, event)
	local k = event.k
	ig.db.put(k, "a", { v = 1 })
	ig.db.put(k, "b", { v = 2 })
	local out = {}
	out.get = ig.db.get(k, "a")
	out.first = ig.db.put_if_absent(k, "claim", "me")
	out.second = ig.db.put_if_absent(k, "claim", "you")
	out.add1 = ig.db.add(k, "n", 5)
	out.add2 = ig.db.add(k, "n", -2)
	local rows = ig.db.query(k, { sk_prefix = "a" })
	out.prefix_rows = #rows
	out.all_rows = #ig.db.query(k)
	ig.db.delete(k, "a")
	out.deleted = ig.db.get(k, "a") == nil
	return out
end

return { handle = handle }
