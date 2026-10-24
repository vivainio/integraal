--!nonstrict
--!runtime 1
-- Submits a batch job whose requests the server performs in the background.
local function handle(_ctx, event)
	local base = native.config.megalith
	local job = native.batch.new({ id = event.id })
	job:http("a", {
		method = "POST",
		url = base .. "api/a",
		body = "hello",
		basic_auth = { username = "svc", password = "s3cretpass" },
	})
	job:http("b", { url = base .. "api/b" })
	job:state({ ref = event.ref })
	job:continue_with("batch_http_done")
	return { job = job:submit() }
end

return { handle = handle }
