--!nonstrict
--!runtime 1
-- Streams a zip of XML files from In into an ndjson object and a summary zip in Out.
local function handle(_ctx, event)
	local src = native.store.In.open(event.key)
	local out = native.store.Out.create("transform/rows.ndjson")
	local n = 0
	for entry in native.zip.entries(src) do
		if not entry.is_dir then
			for ev in native.xml.events(entry:open(), { trim = true }) do
				if ev.type == "start" and ev.tag == "item" then
					n += 1
					out:write(native.json.encode({ file = entry.name, id = ev.attrs.id }) .. "\n")
				end
			end
		end
	end
	out:close()
	local zsink = native.store.Out.create("transform/summary.zip")
	local z = native.zip.writer(zsink)
	z:add("summary.json", native.json.encode({ items = n }))
	z:finish()
	zsink:close()
	return { items = n }
end

return { handle = handle }
