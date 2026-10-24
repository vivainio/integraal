--!nonstrict
--!runtime 1
-- Routes everything that arrives without a script name.
local function on_route(_ctx, event)
	local batch = native.batch.route(event)
	if batch then
		return batch.script and { script = batch.script, event = batch.event } or "hello"
	end
	local key = event.key
	if key and string.sub(key, 1, 15) == "incoming/feeds/" then
		return "feed"
	end
	if event.route then
		return event.route
	end
	return "hello"
end

return { on_route = on_route }
