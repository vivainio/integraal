--!nonstrict
--!runtime 1
local igp = require("@native/patterns@1")
local function handle(_ctx, ev)
    if ev.op == "create" then
        return igp.barrier.create({ id = ev.id, sealed = false, members = ev.members, continue_with = "growth_done" })
    elseif ev.op == "add" then
        return igp.barrier.add(ev.id, ev.members)
    elseif ev.op == "ack" then
        return igp.barrier.ack(ev.id, ev.member)
    elseif ev.op == "seal" then
        return igp.barrier.seal(ev.id)
    end
    return igp.barrier.status(ev.id)
end
return { handle = handle }
