# Barriers

`igp.barrier` waits for a known set of acknowledgements, then continues once. Use it when you
split work among queue workers or send documents to a partner, and need a continuation after
every item has an outcome. [Scatter and gather](patterns.md#scatter-and-gather) uses a barrier
to coordinate the gather; the barrier itself does not dispatch the work.

A barrier is a **work list**: a set of member names, usually object keys, and one outcome per
member. Nothing waits and nothing polls. Each ack is an ordinary event that reaches a script; the
script records it, and the call that resolves the last member sends the continuation.

```mermaid
sequenceDiagram
    participant S as sender script
    participant B as barrier (ig.db)
    participant P as partner
    participant A as ack script
    participant F as continuation
    S->>B: create(members = keys)
    S->>P: store.In.put(key) x N
    P-->>A: ack (S3 object, queue message, callback)
    A->>B: ack(id, key, outcome)
    Note over A,B: the last member resolved
    A->>F: queue message { barrier = id }
    F->>B: hydrate, read outcomes
```

## Script API

```lua
local igp = require("@native/patterns@1")
-- send
local id = igp.barrier.create({
  id = batch,                              -- optional; created again with the same id does nothing
  members = names,                         -- what the acks will name, e.g. "a.xml"
  continue_with = "finalize",              -- script that runs when every member is resolved
  state = { ref = ev.ref },                -- handed to the continuation
  deadline_s = 3600,                       -- optional; see Deadlines
  overdue_with = "chase",                  -- optional script for the overdue event; else the router picks
})
for _, name in names do ig.store.In.put(batch .. "/" .. name, docs[name]) end

-- ack: runs for every acknowledgement, whatever channel it came by
local r = igp.barrier.ack(batch, name, { ok = true, result = { id = "r-17" } })

-- finalize
function on_queue(ctx, ev)
  local b = igp.barrier.hydrate(ev)     -- nil if not complete, or expired / cleaned up
  if not b then return end
  for member, outcome in b:results() do
    if not outcome.ok then ig.log("warn", member .. ": " .. tostring(outcome.error)) end
  end
end
```

| Function | |
| --- | --- |
| `create(opts)` | registers the members and returns the id. `members` is required, at most 10 000, each at most 200 bytes, no duplicates. `id` is generated when absent. `continue_with` needs [`ig.queue`](queue.md) (`"queue": true`). `state` is any JSON, at most 32 KB. Calling it again with the id of a barrier that is already complete does nothing; with the id of one cut short by a crash it finishes the job |
| `ack(id, member, outcome?)` | records the outcome, retires the member and, if none are left, sends the continuation. Returns `{ duplicate, complete, dispatched }` |
| `resume(id)` | retries continuation dispatch without an ack payload. Returns `true` if this call sent; `false` if members are pending, dispatch is already marked sent, or no queue is configured. A live dispatch lease raises the same retryable error as `ack`. Unknown, expired or incompletely created barriers raise |
| `pending(id, limit?)` | the members still waiting, in name order (at most 100 per call) |
| `status(id)` | `{ id, total, ready, complete, dispatched, overdue, due_s }` |
| `hydrate(event)` | in the continuation: the barrier named by `event.barrier`, or `nil` |

`BarrierResult` from `hydrate`:

| | |
| --- | --- |
| `id`, `total`, `state` | the barrier id, the number of members, the value passed to `create` |
| `results()` | iterator of `member, outcome` over every member, in name order. Pages through the table 100 at a time, so it works for lists far larger than one value |
| `failures()` | array of `{ member, error, result }` for the members whose outcome is `ok = false` |

### Outcomes

An ack carries a small outcome: `{ ok = true | false, error = string?, result = any? }`, at most
8 KB (put anything larger in a store and send its key as the `result`). `ok` defaults to `true`.

A **failed ack still resolves its member**. Otherwise one rejected document would hold the whole
batch until its deadline. The barrier completes and the continuation decides what a failure means
(`b:failures()`).

The **first ack for a member wins**. A later one, a duplicate delivery or a partner's retry, is
reported as `duplicate = true` and changes nothing.

## Getting acks in

The barrier does not care how an ack arrives. Everything that can reach a script can ack: an S3
notification for an object the partner wrote, a message the partner put on the queue, a
[callback](callbacks.md). Route the event to one `ack` script and call `igp.barrier.ack` there.

The ack has to say **which barrier and which member**. The simplest convention is to put both in the
keys you control:

```
In/<batch>/<doc>.xml     what you send; the member is "<doc>.xml"
Acks/<batch>/<doc>.json  what the partner writes back
```

```lua
local igp = require("@native/patterns@1")
-- router
local function on_route(_ctx, event)
  if event.store == "Acks" then return "ack" end
  return "default"
end

-- script "ack"
function on_queue(ctx, ev)
  local batch, doc = string.match(ev.key, "^([^/]+)/(.+)%.json$")
  if not batch then return end
  local body = ig.json.decode(ig.store.Acks.get(ev.key) or "{}")
  igp.barrier.ack(batch, doc .. ".xml", { ok = body.status == "ok", error = body.error })
end
```

Two batches that both contain `invoice.xml` do not collide, because the barrier id is part of the
address. If the partner can echo back nothing but the document key, store a `member -> batch` row in
[`ig.db`](dynamodb.md) when you send and look it up in the ack script.

## Deadlines

With `deadline_s` the barrier also sets a [timer](schedules.md#timers). If members are still pending
when it comes due, the sweep sends the tenant

```json
{ "tenant": "acme", "script": "chase", "event": { "type": "barrier.overdue", "barrier": "b1" } }
```

(`script` only when `overdue_with` is set; otherwise the [router](router.md) picks one by
`event.type`). The script looks at `igp.barrier.pending(id)` and `status(id)` and decides: ask the
partner again, alert someone, or ack the missing members itself.

- **The barrier stays open.** Overdue only reports. A late ack still completes the barrier and sends
  the continuation.
- **At least once, and possibly late.** It needs the minute [tick](schedules.md#operator-ticks). The
  ack that sends the continuation cancels the timer, but if that cancel is lost the script still runs
  once for a finished barrier, so check `status(id).complete` first.
- **Needs a queue** (`"queue": true` and a deployment queue). Without one, `deadline_s` only feeds
  `status().overdue`.
- `deadline_s` is at most six days.
- The continuation event is `{ type = "barrier.complete", barrier = id }`.

## Guarantees

- **No lost ack.** `ack` is a sequence of idempotent steps (record the outcome if absent, delete the
  member's pending row, look for any pending row left) and there is no counter. A process that dies
  half way is repaired by the redelivered ack, which repeats the steps. There is no window in which
  the barrier can end up one short.
- **A continuation per completion, at least once.** The call that finds nothing pending claims the
  dispatch with a 60 second lease, sends the message, then marks it sent. Two calls racing for the
  last member send once. A call that dies after sending but before marking leaves a lease that
  lapses, and a later ack (a redelivery) sends again, so write the continuation to be safe to repeat.
- **A busy claim is an error.** While another call holds the lease, `ack` raises
  `barrier completion is being dispatched by another call; retry`. Let it fail: the queue redelivers
  and the next attempt sees the send finished.
- **Unknown members are refused.** Acking a name that is not on the list raises, so a stray event
  cannot complete a barrier.
- **Tenant-private.** The rows live in a partition that `ig.db` cannot address, so scripts
  cannot read or forge them except through `igp.barrier`.

## Crash recovery

Recovery depends on retrying the interrupted operation. Run dispatch and acknowledgement scripts
from a durable retry source, and let errors propagate so failed deliveries are retried.

- **Create before dispatch.** Retry `create` with the same id, members and options after a crash,
  then resend work using stable member ids. Creating the barrier and sending work are separate
  operations; a partially successful send can produce duplicate work on retry.
- **Save results before acking.** A worker that crashes after saving its result must repeat the
  write safely and call `ack` again. The barrier cannot make a store write or external side effect
  atomic with its acknowledgement. The first recorded outcome wins.
- **Retry interrupted acks.** A repeated `ack` finishes retiring the member and, when appropriate,
  dispatching the continuation, even if the outcome was already recorded. Before retiring a member,
  `ack` persists a recovery entry. If the worker stops after this point, the minute sweep
  finishes the acknowledgement and continuation dispatch without requiring ack redelivery. A crash
  before indexing still requires ack redelivery; the member remains pending.
- **Recover dispatch automatically.** Queue-send failures, exhausted invocation budgets, and crashes
  can leave indexed work behind. The sweep retries it after any live dispatch lease expires.
  `igp.barrier.resume(id)` remains available for explicit recovery; it leaves pending members and
  outcomes unchanged and does not resend a continuation already marked sent. Failed resumes of
  complete barriers also register recovery work. Legacy interruptions from before recovery indexing
  was introduced need an ack replay or explicit `resume` to enroll them; there is no history scan.
- **Make continuation effects safe to repeat.** Sending the continuation and marking it sent are
  separate operations, and the queue can redeliver it too. Use stable output keys or a destination
  idempotency key. Claiming with `put_if_absent` before performing an effect can lose that effect
  if the process crashes after the claim. Use a recoverable lease or a transactional mechanism
  when a destination cannot make the effect idempotent.

A [deadline](#deadlines) helps detect missing members, including work that was never dispatched.
Its handler must decide how to retry or resolve them; it does not automatically repair dispatch
or complete the barrier.

## Recovery sweep

The shared `system.tick` minute message drives the sweep on Fargate or via a direct runtime Lambda
invocation. There is no polling loop per worker. Automatic recovery needs the deployment database,
queue, and minute tick. A failed sweep is logged and the next minute can retry it.

The index contains one temporary entry per in-flight acknowledgement or explicit complete-barrier
resume. Successful operations delete their entries. Each ack registers **before** deleting its
pending member: indexing only after detecting completion could lose dispatch if the process stops
between the last member deletion and index registration. Separate member entries prevent concurrent
acks from clearing each other's recovery work. Recorded outcomes remain first-write-wins.

- An idle sweep performs two reads: its cursor and an empty pending-index query, with no writes.
- Each tick examines at most 100 index entries, not all barriers or historical receipts. A durable
  cursor advances past failed or leased entries and wraps at the end; a backlog may take several ticks.
- Recovery claims prevent overlapping sweepers from working the same entry at once. Dispatch also
  uses the existing per-barrier lease shared with foreground acks and resumes.
- Each recovery attempt persists its next eligible time before working. Repeated attempts back off
  from 60 seconds to at most one hour. Failed attempts are logged and counted in the tick report.
  An entry in backoff is still read when visited, but is not dispatched again until eligible.
- A crash after saving the sent receipt leaves only cleanup work. A crash after sending but before
  saving that receipt can send again. Continuation effects must remain safe to repeat.
- Index entries have no TTL until removed. Recovery claim rows have an expiry. Expired or removed
  legacy barrier headers cause their stale index entries to be removed without sending.

## Cost and limits

Every backend call is one `max_data_ops` ([limits](limits.md)).

| Call | Data ops |
| --- | --- |
| `create` with N members | N + 3 |
| `ack` (queue enabled) | 7 while members remain; 10 when it sends; 6 for a duplicate while pending, 8 if already sent |
| `resume` (queue enabled) | 2 while pending; 7 when it sends; 6 when already sent (plus 1 for deadline cancellation) |
| `results()` | 1 per 100 members |
| `pending`, `hydrate` | 2 |
| `status` | 3 |

`create` is one invocation, so the work list must fit in the tenant's `max_data_ops`
(default 1000, ceiling 10 000). Raise the tenant's limit for large batches.

Without a queue, `ack` uses 5 data ops (4 for a duplicate); there is no recovery index or send.
The index adds two database operations per queue-enabled ack or complete-barrier resume. Recovery
sweeps use their own bounded per-entry budget, independent of the interrupted invocation.

A barrier with `deadline_s` adds 1 data op to `create` and 1 to the ack that sends the continuation
(it cancels the deadline timer).

Barrier state, outcomes and dispatch receipts have no DynamoDB TTL. They persist until explicitly
removed by an operator; a completed barrier id cannot be reused while its records remain. Dispatch
leases still expire after 60 seconds. Existing barriers retain their stored expiry; `ack` and
`resume` refuse expired legacy barriers, and `hydrate` returns `nil` after their expiry.

## Deployment

Needs the `db` section of the [global config](deployment.md#configuration), and for `continue_with`
the tenant's `"queue": true` and a `queue.url`. Automatic recovery additionally needs the shared
[minute tick](schedules.md#operator-ticks) handled by Fargate or the runtime Lambda. The sweep uses
the same table actions as [`ig.db`](dynamodb.md), including `Query`.

Without a queue, a barrier still works as a plain work list: `ack` tells you when `complete`, and
you act on it yourself. More than one caller can see `complete = true`, so make the resulting
effects safe to repeat using the same recovery rules above.

## Not covered

- **A member that never acks, without a `deadline_s`.** Nothing runs; set one (see
  [Deadlines](#deadlines)).
- **Growing a barrier.** The members are fixed at `create`.
- **Recovering a lost notification.** If a partner's ack object exists but its event was lost,
  nothing acks the member. List the store and ack the missing keys by hand.
