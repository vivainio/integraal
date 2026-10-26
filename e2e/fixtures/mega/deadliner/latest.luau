--!nonstrict
--!runtime 1
-- A nested deadline around a slow call must fire before the call finishes.
local function handle(_ctx, event)
	local t0 = ig.time.now_ms()
	local ok, err = pcall(ig.deadline.with, event.ms, function()
		ig.http.get(ig.config.megalith .. "hold")
	end)
	return { ok = ok, err = tostring(err), elapsed = ig.time.now_ms() - t0 }
end

return { handle = handle }
