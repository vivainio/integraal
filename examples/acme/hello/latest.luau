--!runtime 1
-- Serves invoke, http and queue triggers from one handler.
return {
	handle = function(ctx, event)
		local n = (tonumber(native.data.get("counter")) or 0) + 1
		native.data.put("counter", tostring(n))
		native.log("info", "hello from " .. ctx.tenant .. " via " .. ctx.trigger)
		return { trigger = ctx.trigger, count = n, event = event }
	end,
}
