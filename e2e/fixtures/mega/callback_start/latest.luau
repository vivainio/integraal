--!nonstrict
--!runtime 1
-- Hands out a single-use callback URL for a third party to call back.
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	return { url = igp.callback.create({ script = "callback_done", state = { id = event.id }, ttl_s = event.ttl_s }) }
end

return { handle = handle }
