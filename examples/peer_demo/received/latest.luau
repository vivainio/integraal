--!strict
--!runtime 1
local igp = require("@native/patterns@1")
local function on_queue(_ctx: Context, event: any): any
    local job = igp.batch.hydrate(event, { allow_failure = true })
    if not job then return nil end
    if not job.ok then return { accepted = false, error = job.error } end
    local receipts = {}
    for row in job:rows("receipt") do table.insert(receipts, row) end
    return { reference = job.state.reference, receipts = receipts, delete = job:keys() }
end
return { on_queue = on_queue }
