--!nonstrict
--!runtime 1
-- Describes its own task, then optionally fails, so the task record has data from before the failure.
local function handle(_ctx, event)
	native.task.title("tasker " .. tostring(event.what))
	native.task.metric("rows", 10)
	native.task.add("rows", 5)
	native.task.attr("region", "eu")
	native.task.detail({ skipped = { 1, 2 } })
	native.log("info", "working")
	if event.fail then
		error("tasker failed " .. tostring(event.what))
	end
	return { ok = true }
end

return { handle = handle }
