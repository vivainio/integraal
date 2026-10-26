--!strict
--!runtime 1
-- Deploy as both order-workflow/v1.luau and order-workflow/latest.luau.
local igp = require("@native/patterns@1")

local function workflow(seq: WorkflowSequence, input: { items: { string } })
    local barrier = seq:step("dispatch", function()
        local id = igp.barrier.create({
            id = seq:key("items"),
            members = input.items,
            continue_with = "order-workflow",
            state = { sequence = seq.id },
        })
        for _, item in input.items do
            ig.queue.handoff(seq:key("work/" .. item), {
                script = "order-workflow", version = "v1",
                event = { type = "item", barrier = id, item = item },
            })
        end
        return id
    end)

    -- No worker is held while waiting. Completion replays this function.
    local completion = seq:wait("items-ready")
    return seq:step("gather", function()
        local gathered = igp.barrier.hydrate(completion)
        assert(gathered, "barrier results are no longer available")
        assert(gathered.id == barrier, "unexpected barrier")
        local results = {}
        local each = gathered.results
        assert(each)
        for member, outcome in each(gathered) do
            results[member] = outcome
        end
        return results
    end)
end

-- Workers and event adapters can live alongside the workflow.
local function on_queue(_ctx: Context, event: any): any
    if event.type == "item" then
        -- Replace this calculation with idempotent partner work.
        return igp.barrier.ack(event.barrier, event.item, {
            ok = true, result = string.upper(event.item),
        })
    elseif event.type == "barrier.complete" then
        local gathered = igp.barrier.hydrate(event)
        assert(gathered, "barrier results are no longer available")
        return igp.sequence.signal(gathered.state.sequence, "items-ready", event)
    end
    error("unexpected event type: " .. tostring(event.type))
end

local function on_invoke(_ctx: Context, event: { id: string, items: { string } })
    return igp.sequence.start(event.id, {
        script = "order-workflow", version = "v1", input = { items = event.items },
    })
end

return { workflow = workflow, on_queue = on_queue, on_invoke = on_invoke }
