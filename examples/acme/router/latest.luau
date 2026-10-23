--!strict
--!runtime 1
-- Router: picks a script for messages that don't name one.
local function on_route(_ctx: Context, event: { key: string? })
	local key = event.key
	if key and string.sub(key, 1, 15) == "incoming/feeds/" then
		return "feed"
	end
	return "hello"
end

return { on_route = on_route }
