--!strict
--!runtime 1
local function handle(ctx: Context, event: any)
	return { trigger = ctx.trigger, event = event }
end

return { handle = handle }
