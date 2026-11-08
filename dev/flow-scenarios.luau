--!strict
--!runtime 1
-- Local development workloads: errors are intentional so flow inspection has failures to show.
local function handle(_ctx: Context, event: { action: string })
    if event.action == "missing-customer" then
        ig.log("warn", "customer lookup failed")
        error("Customer C-1042 was not found in the local example dataset")
    elseif event.action == "invalid-order" then
        ig.log("warn", "order validation failed")
        error("Order validation failed: delivery address is missing")
    end
    ig.log("info", "local processing completed")
    return { processed = true, action = event.action }
end

return { handle = handle }
