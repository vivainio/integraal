--!nonstrict
--!runtime 1
-- Waits on megalith's slow route, so the invocation is in flight for a while.
local function handle(_ctx, event)
	local r = native.http.request({ method = "GET", url = event.url })
	return { status = r.status }
end

return { handle = handle }
