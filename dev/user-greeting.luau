--!strict
--!runtime 1
local function on_user_trigger(_ctx: Context, args: { name: string, enthusiastic: boolean })
    local greeting = "Hello, " .. args.name .. (if args.enthusiastic then "!" else ".")
    ig.log("info", "Created a greeting")
    return { greeting = greeting }
end
local user_trigger: UserTrigger = {
    title = "Create a greeting",
    description = "Run a short script and see its result immediately.",
    arguments = {
        { name = "name", label = "Name", type = "string", required = true, default = "Aino" },
        { name = "enthusiastic", label = "Add an exclamation mark", type = "boolean", default = true },
    },
}
return { user_trigger = user_trigger, on_user_trigger = on_user_trigger }
