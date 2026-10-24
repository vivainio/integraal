--!nonstrict
--!runtime 1
-- A nested deadline around a slow call must fire before the call finishes.
local function handle(_ctx, event)
	local t0 = native.time.now_ms()
	local ok, err = pcall(native.deadline.with, event.ms, function()
		native.http.get(native.config.megalith .. "hold")
	end)
	return { ok = ok, err = tostring(err), elapsed = native.time.now_ms() - t0 }
end

return { handle = handle }
