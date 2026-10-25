--!nonstrict
--!runtime 1
-- Scratch SQL that survives between invocations by being saved to a store.
local function handle(_ctx, event)
	if event.op == "write" then
		local db = native.sql.open()
		db:exec("create table t (id integer primary key, name text)")
		for _, name in ipairs(event.names) do
			db:exec("insert into t (name) values (?)", { name })
		end
		local sink = native.store.Temp.create("scratch.sqlite")
		local bytes = db:save(sink)
		sink:close()
		return { bytes = bytes }
	end
	local db = native.sql.load(native.store.Temp.open("scratch.sqlite"))
	return { rows = db:query("select id, name from t order by id") }
end

return { handle = handle }
