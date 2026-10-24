--!nonstrict
--!runtime 1
-- Continuation for background HTTP: reports statuses and bodies.
local function handle(_ctx, event)
	local job = native.batch.hydrate(event, { allow_failure = true })
	if not job then
		return nil
	end
	native.bucket.Out.put("bhttp/" .. job.id, native.json.encode({
		ok = job.ok,
		state = job.state,
		a = { status = job.results.a.status, body = job:body("a") },
		b = { status = job.results.b.status, body = job:body("b") },
	}))
	return { delete = job:keys() }
end

return { on_queue = handle, handle = handle }
