--!strict
--!runtime 1
-- Serves invoke, http and queue triggers from one handler.
local function handle(ctx: Context, event: any)
	local n = (tonumber(native.data.out.get("counter")) or 0) + 1
	native.data.out.put("counter", tostring(n))
	native.log("info", "hello from " .. ctx.tenant .. " via " .. ctx.trigger)
	return { trigger = ctx.trigger, count = n, event = event }
end

return { handle = handle }
