--!nonstrict
--!runtime 1
-- Fails the first time it sees an object, so a record gets requeued, and succeeds the second time.
local function handle(_ctx, event)
	local runs = native.db.add("flaky-runs", event.key)
	if runs == 1 then
		error("first try fails")
	end
	native.store.Out.put("flaky/" .. event.key, tostring(runs))
	return { runs = runs }
end

return { on_queue = handle, handle = handle }
