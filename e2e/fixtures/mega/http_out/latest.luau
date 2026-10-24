--!nonstrict
--!runtime 1
-- Makes the request described by the event and reports the outcome instead of raising.
local function handle(_ctx, event)
	local ok, r = pcall(native.http.request, event)
	if not ok then
		return { ok = false, err = tostring(r) }
	end
	local body = r.body
	if event.stream then
		body = "<stream>"
	end
	return { ok = true, status = r.status, body = body, headers = r.headers }
end

return { handle = handle }
