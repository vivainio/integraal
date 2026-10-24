--!nonstrict
--!runtime 1
-- File drop: counts <entry> elements of the dropped XML, records the count, then asks for the object to be deleted.
local function handle(_ctx, event)
	local stream = native.bucket.In.open(event.key)
	local entries = 0
	for ev in native.xml.events(stream, { trim = true }) do
		if ev.type == "start" and ev.tag == "entry" then
			entries += 1
		end
	end
	native.bucket.Out.put("feed-result/" .. event.key, tostring(entries))
	return { entries = entries, delete = true }
end

return { on_queue = handle, handle = handle }
