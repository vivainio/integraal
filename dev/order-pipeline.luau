--!strict
--!runtime 1
-- A local durable workflow. Each step is checkpointed by Integraal.
local igp = require("@native/patterns@1")

local function workflow(seq: WorkflowSequence, input: { order_id: string })
    local order = seq:step("Receive order", function()
        return { id = input.order_id, items = 3 }
    end)
    local validated = seq:step("Validate order", function()
        assert(order.items > 0, "order must contain items")
        return true
    end)
    local customer = seq:step("Look up customer", function()
        return { name = "Example customer", active = validated }
    end)
    local stock = seq:step("Reserve inventory", function()
        return { reserved = order.items }
    end)
    local payment = seq:step("Authorize payment", function()
        return { authorized = customer.active and stock.reserved > 0 }
    end)
    seq:step("Prepare shipment", function()
        assert(payment.authorized, "payment not authorized")
        return { ready = true }
    end)
    seq:step("Notify customer", function()
        return { notified = true }
    end)
    return seq:step("Complete order", function()
        return { id = order.id, status = "complete" }
    end)
end

local function handle(_ctx: Context, event: { id: string })
    return igp.sequence.start(event.id, {
        script = "order-pipeline", version = "v1", input = { order_id = event.id },
    })
end

return {
    workflow = workflow,
    handle = handle,
    on_user_trigger = handle,
    user_trigger = {
        title = "Run order workflow",
        description = "Start the durable order workflow. Use a unique run ID for each new run.",
        arguments = {
            { name = "id", label = "Unique run ID", type = "string", required = true },
        },
    },
}
