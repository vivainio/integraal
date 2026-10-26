# Schedules and timers

Two things make scripts run at a given time, and both end as an ordinary [queue](triggers.md#sqs-fargate)
message that the tenant's [router](router.md) sends to a script:

| | Schedule | Timer |
| --- | --- | --- |
| Recurs | yes, every minute, hour or day | no, one-shot |
| Set by | the operator, in infrastructure | the engine, for a feature such as a [barrier deadline](barrier.md#deadlines) |
| Event | `type = "schedule"` | whatever its creator chose, such as `type = "barrier.overdue"` |

Neither is exact. Messages are delivered at least once, can be late, and arrive after a
per-tenant delay (below), so handlers must be safe to repeat and must not read the clock to find out
which firing they are.

## Event types

Events the engine makes carry a `type`, so a router can match on it instead of guessing from fields.
Names are `<source>.<what>`; anything you send yourself keeps its own fields.

| `type` | When | Other fields |
| --- | --- | --- |
| `schedule` | an operator tick | `every` (`minute`, `hour`, `day`), `at` |
| `barrier.complete` | the last member of a [barrier](barrier.md) resolved | `barrier` |
| `barrier.overdue` | a barrier is still open past its `deadline_s` | `barrier` |
| `s3.object` | an [S3 notification](triggers.md#s3-notifications) | `store`, `key`, `size`, `etag`, `event` |

```lua
local function on_route(_ctx, event)
  if event.type == "schedule" and event.every == "day" then return "daily-report" end
  if event.type == "barrier.overdue" then return "chase-partner" end
  return "default"
end
```

## Operator ticks

Three EventBridge Scheduler schedules send a tick to the queue (the one the server polls):

| Schedule | Input |
| --- | --- |
| `rate(1 minute)` | `{"type":"system.tick","every":"minute","at":"<aws.scheduler.scheduled-time>"}` |
| `rate(1 hour)` | `{"type":"system.tick","every":"hour","at":"<aws.scheduler.scheduled-time>"}` |
| `rate(1 day)` | `{"type":"system.tick","every":"day","at":"<aws.scheduler.scheduled-time>"}` |

The target is the queue itself (an SQS universal target), so the schedule's role needs only
`sqs:SendMessage` on it. Leave the flexible time window off. `at` is Scheduler's context attribute for
the time the schedule was due; the engine passes it on untouched.

A tick belongs to no tenant. The server that receives it:

1. on the **minute** tick, sends up to 100 due [timers](#timers), recovers up to 100 pending
   [durable handoffs](queue.md#durable-handoff), and examines up to 100 interrupted
   [barrier acknowledgements/dispatches](barrier.md#recovery-sweep);
2. reads `tenants.json` and sends one message to each tenant that asked for this kind of tick:

```json
{ "tenant": "acme", "event": { "type": "schedule", "every": "hour", "at": "2026-10-25T14:00:00Z" } }
```

There is no `script`, so the tenant's router chooses one. Use `at`, not the clock, as the key of
whatever the handler does ("the report for the day in `at`"), so a repeated or late message does the
same thing.

### Which tenants

`tenants.json` sits in the script bucket, next to the tenants' directories (under `scripts.prefix`):

```json
["acme", "globex"]
```

It is the operator's list of every tenant. Names that are not valid are logged and skipped. Each
tenant then opts in to the ticks it wants in its [`config.json`](tenant-config.md):

```json
{ "router": "router", "schedule": ["hour", "day"] }
```

A tenant that is listed but has no `schedule` gets nothing, so a minute tick costs nothing for the
tenants that do not use it. A tenant whose config cannot be read is logged and skipped; the others
still get theirs. If `tenants.json` is missing, no tenant gets ticks (timers are still swept).

### Spreading the load

The messages of one tick are not sent at once. Each gets the queue delay `hash(tenant, kind) mod
window`, so a tenant always lands at the same point in the window, and a repeated tick repeats the
same delays. `at` is not changed by it.

```json
{ "schedule": { "spread_s": { "minute": 20, "hour": 300, "day": 900 } } }
```

Those are the defaults. A window must be shorter than the period and at most 900 seconds, the
queue's limit on a delay. The sending itself is one batch call per ten tenants from one task, which
holds for a few thousand tenants.

### Failures

The minute tick is acknowledged whatever happens, because the next one is a minute away. If an
hour or day tick fails (the queue is unreachable, `tenants.json` is malformed), the message is left on
the queue and retried, so tenants that already got their message get it again.

### Recurring work with another rhythm

A tenant that wants "every 15 minutes" or "06:00 Helsinki time" needs a schedule of its own. Create
one in EventBridge Scheduler with the queue as target and an ordinary envelope as input; the router
sees it like any other message:

```json
{ "tenant": "acme", "event": { "type": "schedule", "name": "morning-report", "at": "<aws.scheduler.scheduled-time>" } }
```

## Timers

A timer is a row that says "at time T, send this envelope to the queue". The engine sets them for
features such as a [barrier's deadline](barrier.md#deadlines). They live in one partition sorted by due
time, so the sweep finds what is due with a single ordered query and the table needs no extra index.

- **The minute tick runs the sweep.** It sends up to 100 due timers per tick, oldest first. Without
  the minute schedule no timer fires.
- **Several tasks may sweep at once.** Each timer is claimed with a 60 second lease before it is sent
  and checked once more after the claim, so concurrent sweeps send it once. A sweeper that dies
  after sending lets the lease lapse and the next sweep sends again, so a timer fires at least once.
- **A timer that cannot be sent stays.** The next minute tries again.
- **Timer TTL is 30 days after its due time.** This gives the sweep a month to recover after a
  missed firing, independently of when the timer was created. Delivery or cancellation removes it
  earlier. The sweep drops expired or malformed rows. Dispatch leases still expire after 60 seconds.
- **At most six days ahead.** This remains the scheduling horizon.
- Timers need the `db` section and a `queue` in the [deployment config](deployment.md#configuration).
  They run only on the Fargate server.
