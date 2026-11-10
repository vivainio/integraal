--!strict
--!runtime 1
-- Dispatch three real queue messages. The worker invocations retain this flow's trace.
local function handle(_ctx: Context, event: { batch_id: string })
    local jobs = {
        { script = "prepare-item", event = { batch_id = event.batch_id, item = "A" }, group = "prepare" },
        { script = "reserve-item", event = { batch_id = event.batch_id, item = "B" }, group = "reserve" },
        { script = "notify-item", event = { batch_id = event.batch_id, item = "C" }, group = "notify" },
    }
    local count = ig.queue.send(jobs)
    ig.log("info", "dispatched " .. tostring(count) .. " queued tasks")
    return { queued = count }
end
return {
    handle = handle,
    on_user_trigger = handle,
    user_trigger = {
        title = "Dispatch a batch",
        description = "Send three queued tasks and inspect their shared flow.",
        arguments = {
            { name = "batch_id", label = "Batch ID", type = "string", required = true },
        },
    },
}
