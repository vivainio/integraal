--!strict
--!runtime 1
-- Counts <entry> elements in a (possibly huge) XML object, streaming.
local function handle(ctx: Context, event: { key: string })
	local stream = native.data["in"].open(event.key)
	if not stream then
		return { entries = 0, found = false }
	end

	local entries = 0
	for ev in native.xml.events(stream, { trim = true }) do
		if ev.type == "start" and ev.tag == "entry" then
			entries += 1
		end
	end
	return { entries = entries, found = true }
end

return { on_queue = handle, handle = handle }
