--!nonstrict
--!runtime 1
-- Runs when the third party calls back, with the state saved at creation.
local function handle(_ctx, event)
	native.store.Out.put("callbacks/" .. event.state.id, native.json.encode({
		method = event.method,
		body = event.body,
		state = event.state,
	}))
end

return { on_queue = handle, handle = handle }
