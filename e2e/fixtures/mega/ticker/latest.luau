--!nonstrict
--!runtime 1
-- An operator tick (type = "schedule"); the marker is named by the tick's `at`.
local function handle(_ctx, event)
	ig.store.Out.put("ticks/" .. event.at, ig.json.encode({ type = event.type, every = event.every }))
end

return { on_queue = handle, handle = handle }
