--!strict
local function on_route(ctx: Context, event: { key: string }): RouteDecision
    return {
        script = "parse-order",
        capabilities = {
            stores = {
                In = { read = { event.key } },
                Out = { write_prefixes = { "reports/" } },
            },
        },
    }
end
return { on_route = on_route }
