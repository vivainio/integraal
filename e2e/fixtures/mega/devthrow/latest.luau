--!nonstrict
--!runtime 1
-- Throws a table that holds a value which must never reach a log.
local function handle(_ctx, event)
	error({ code = "E_NOPE", secret = event.secret })
end

return { handle = handle }
