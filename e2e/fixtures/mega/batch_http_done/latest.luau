--!nonstrict
--!runtime 1
-- Continuation for background HTTP: reports statuses and bodies.
local igp = require("@native/patterns@1")
local function handle(_ctx, event)
	local job = igp.batch.hydrate(event, { allow_failure = true })
	if not job then
		return nil
	end
	ig.store.Out.put("bhttp/" .. job.id, ig.json.encode({
		ok = job.ok,
		state = job.state,
		a = { status = job.results.a.status, body = job:body("a") },
		b = { status = job.results.b.status, body = job:body("b") },
	}))
	return { delete = job:keys() }
end

return { on_queue = handle, handle = handle }
