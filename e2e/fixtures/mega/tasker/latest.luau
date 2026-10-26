--!nonstrict
--!runtime 1
-- Describes its own task, then optionally fails, so the task record has data from before the failure.
local function handle(_ctx, event)
	ig.task.title("tasker " .. tostring(event.what))
	ig.task.metric("rows", 10)
	ig.task.add("rows", 5)
	ig.task.attr("region", "eu")
	ig.task.detail({ skipped = { 1, 2 } })
	ig.log("info", "working")
	if event.fail then
		error("tasker failed " .. tostring(event.what))
	end
	return { ok = true }
end

return { handle = handle }
