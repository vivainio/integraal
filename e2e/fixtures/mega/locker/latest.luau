--!nonstrict
--!runtime 1
-- Holds a lock while megalith answers slowly; reports when it held it.
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	local got, window = igp.lock.with("e2e-lock", { scope = event.scope, wait_ms = 15000 }, function()
		local t0 = ig.time.now_ms()
		ig.http.get(ig.config.megalith .. "hold")
		return { t0 = t0, t1 = ig.time.now_ms() }
	end)
	return { got = got, window = window }
end

return { handle = handle }
