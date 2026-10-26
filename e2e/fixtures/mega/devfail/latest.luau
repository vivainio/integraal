--!nonstrict
--!runtime 1
-- Logs a line, then fails two calls deep, so a flow log has a line and a failure with frames.
local function inner(event)
	error("bad input " .. tostring(event.what))
end

local function outer(event)
	inner(event)
end

local function handle(_ctx, event)
	ig.log("info", "about to fail: " .. tostring(event.what))
	outer(event)
end

return { handle = handle }
