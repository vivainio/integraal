--!strict
--!runtime 1
local function on_queue(_ctx: Context, event: { batch_id: string, item: string })
    ig.log("info", "processed item " .. event.item)
    return { processed = true, batch = event.batch_id, item = event.item }
end
return { on_queue = on_queue }
