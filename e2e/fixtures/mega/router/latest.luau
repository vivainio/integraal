--!nonstrict
--!runtime 1
-- Routes everything that arrives without a script name.
local igp = require("@native/patterns@1")
local function on_route(_ctx, event)
	if event.type == "schedule" then
		return "ticker"
	end
	if event.type == "barrier.overdue" then
		return "overdue"
	end
	local batch = igp.batch.route(event)
	if batch then
		return batch.script and { script = batch.script, event = batch.event } or "hello"
	end
	local key = event.key
	if key and string.sub(key, 1, 15) == "incoming/feeds/" then
		return "feed"
	end
	if key and string.sub(key, 1, 15) == "incoming/flaky/" then
		return "flaky"
	end
	if key and string.sub(key, 1, 16) == "incoming/orders/" then
		return "ingest"
	end
	if event.route then
		return event.route
	end
	return "hello"
end

return { on_route = on_route }
