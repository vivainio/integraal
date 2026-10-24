--!nonstrict
--!runtime 1
-- Placement: big work goes to the large worker class, everything else to the tenant's default.
local function on_place(_ctx, event)
	if event.big or (event.size and event.size > 1000) then
		return "large"
	end
	return nil
end

return { on_place = on_place }
