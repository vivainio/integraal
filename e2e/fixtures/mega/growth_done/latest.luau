--!nonstrict
--!runtime 1
local igp = require("@native/patterns@1")
local function handle(_ctx, ev)
    local b = igp.barrier.hydrate(ev)
    if not b then return end
    local count = 0
    for _member, _outcome in b:results() do count += 1 end
    ig.store.Out.put("growth/" .. b.id, ig.json.encode({ total = b.total, results = count }))
end
return { handle = handle }
