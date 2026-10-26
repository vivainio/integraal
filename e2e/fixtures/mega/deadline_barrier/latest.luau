--!nonstrict
--!runtime 1
-- A one-member barrier with a short deadline. `op` is "create" or "ack".
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	if event.op == "create" then
		return igp.barrier.create({ id = event.id, members = { "a" }, deadline_s = event.deadline_s })
	end
	return igp.barrier.ack(event.id, "a", { ok = true })
end

return { on_invoke = handle, handle = handle }
