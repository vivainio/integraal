# SQL scratch databases

`native.sql` gives a script a private **SQLite database in memory** for working data: staging a feed,
deduplicating, joining, aggregating, keeping a run's bookkeeping. It lives only for the invocation, so
there is nothing to share or clean up. To hand work to a later invocation, save it to a bucket and load
it there.

```lua
local db = native.sql.open()
db:exec("create table seen (id text primary key, amount real)")
for ev in native.xml.events(native.gzip.open(native.bucket.In.open("feed.xml.gz"))) do
  if ev.type == "start" and ev.tag == "item" then
    db:exec("insert or ignore into seen values (?, ?)", { ev.attrs.id, tonumber(ev.attrs.amount) })
  end
end

-- keep it for later processing
local out = native.bucket.Temp.create("job-42.db")
db:save(out)
out:close()
```

A later invocation (a queue message, a callback) continues from the file:

```lua
local db = native.sql.load(native.bucket.Temp.open("job-42.db"))
for _, row in db:query("select id, amount from seen order by amount desc limit 10") do ... end
```

## API
| | |
| --- | --- |
| `native.sql.open()` | a new empty database |
| `native.sql.load(source)` | a database from a saved file; `source` is a string or a stream. The loaded database is writable. |
| `native.sql.null` | bind it as a parameter to store `NULL` |
| `db:exec(sql, params?)` | runs statements and returns the rows changed by the last one. Without `params` it may contain several statements (schema scripts); with them it must be one. |
| `db:query(sql, params?)` | all result rows as an array of `{column = value}` tables. `NULL` columns are absent. Fails past 100 000 rows: add `LIMIT` or page through (`where id > ? order by id limit 1000`). |
| `db:get(sql, params?)` | the first row, or `nil` |
| `db:last_insert_id()` | rowid of the last insert |
| `db:save(sink)` | streams the database file into a sink from `bucket.create`; `sink:close()` commits it. Returns the bytes written. |
| `db:dump()` | the database file as a string, capped at `max_object_mb` |
| `db:close()` | frees the database early |

Parameters are a positional array bound to `?`. Strings are stored as text (or as a blob when they are
not valid UTF-8), integral numbers as integers, other numbers as reals, booleans as 0/1. Luau numbers
are doubles, so integers above 2^53 lose precision.

## Limits and safety
- Size is capped by `max_sql_mb` ([Limits](limits.md)); inserts past it fail with "database or disk
  is full", and loading a larger file is refused.
- Statements are interrupted when the invocation deadline passes.
- The database is not a file: `ATTACH`, `DETACH` and all `PRAGMA` statements are denied, temporary
  tables stay in memory, and string/blob values are capped at `max_object_mb`.
- Loaded files are treated as untrusted input (untrusted-schema and defensive modes are on), but a
  saved file is a plain SQLite database, so tenants can also read it with any SQLite tool.
- Each invocation has its own databases; nothing is shared between invocations or tenants. To keep
  state, save it. Two invocations that load the same file and save it back do not merge: the last
  save wins, so give concurrent jobs separate files.
