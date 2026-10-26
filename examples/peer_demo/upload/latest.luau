--!strict
--!runtime 1
local function handle(_ctx: Context, event: any)
    local job = native.batch.new()
    job:input("document", event.body, { ext = ".xml" })
    job:query("receipt", "accept_document", { input = "document" })
    job:state({ reference = event.reference })
    job:continue_with("received")
    return { job = job:submit() }
end
return { handle = handle }
