--!nonstrict
--!runtime 1
-- Holds a lock while megalith answers slowly; reports when it held it.
local function handle(_ctx, event)
	local got, window = native.lock.with("e2e-lock", { scope = event.scope, wait_ms = 15000 }, function()
		local t0 = native.time.now_ms()
		native.http.get(native.config.megalith .. "hold")
		return { t0 = t0, t1 = native.time.now_ms() }
	end)
	return { got = got, window = window }
end

return { handle = handle }
