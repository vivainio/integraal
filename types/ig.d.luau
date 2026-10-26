-- Type definitions for integraal core (`ig`) and imported patterns, version 1.
-- Load with:  dev/typecheck.sh   or point luau-lsp at this file (see docs/types.md).
-- Note: definition files can only expose `declare extern type`, not plain aliases.

declare extern type ByteStream with
    function close(self): ()
end

-- First argument of every handler.
declare extern type Context with
    tenant: string
    script: string
    trigger: "invoke" | "http" | "queue" | "route"
    runtime: number
    -- Delivery number of a queue message (1 = first); always 1 for other triggers.
    attempt: number
end

-- Fields are present depending on `type`:
--   start: tag, attrs, depth   end: tag, depth   text/cdata/comment: text, depth
declare extern type XmlEvent with
    type: "start" | "end" | "text" | "cdata" | "comment"
    tag: string?
    attrs: { [string]: string }?
    text: string?
    depth: number
end

declare extern type XmlNode with
    kind: (XmlNode) -> "document" | "element" | "text" | "comment" | "processing-instruction" | "attribute"
    name: (XmlNode) -> string
    local_name: (XmlNode) -> string
    namespace: (XmlNode) -> string
    text: (XmlNode) -> string
    attr: (XmlNode, name: string) -> string?
    attrs: (XmlNode) -> { [string]: string }
    children: (XmlNode, name: string?) -> { XmlNode }
    child: (XmlNode, name: string) -> XmlNode?
    root: (XmlNode) -> XmlNode?
    parent: (XmlNode) -> XmlNode?
    -- Node-sets come back as arrays in document order; other results as string, number or boolean.
    xpath: (XmlNode, expr: string, ns: { [string]: string }?) -> any
    first: (XmlNode, expr: string, ns: { [string]: string }?) -> any
    xml: (XmlNode) -> string
end

declare extern type XmlWriter with
    start: (XmlWriter, tag: string, attrs: { [string]: string | number | boolean }?) -> ()
    close: (XmlWriter) -> ()
    text: (XmlWriter, text: string) -> ()
    cdata: (XmlWriter, text: string) -> ()
    comment: (XmlWriter, text: string) -> ()
    element: (XmlWriter, tag: string, attrs: { [string]: string | number | boolean }?, text: (string | number)?) -> ()
    finish: (XmlWriter) -> string?
end

-- Streaming upload from `Store.create`; the object appears on `close`.
declare extern type Sink with
    write: (Sink, data: string) -> ()
    close: (Sink) -> number
    abort: (Sink) -> ()
end

declare extern type ZipWriter with
    add: (ZipWriter, name: string, content: (string | ByteStream)?) -> ()
    write: (ZipWriter, data: string) -> ()
    finish: (ZipWriter) -> string?
end

declare extern type SqlDatabase with
    exec: (SqlDatabase, sql: string, params: { any }?) -> number
    query: (SqlDatabase, sql: string, params: { any }?) -> { { [string]: any } }
    get: (SqlDatabase, sql: string, params: { any }?) -> { [string]: any }?
    last_insert_id: (SqlDatabase) -> number
    save: (SqlDatabase, sink: Sink) -> number
    dump: (SqlDatabase) -> string
    close: (SqlDatabase) -> ()
end

declare extern type GzipWriter with
    write: (GzipWriter, data: string) -> ()
    finish: (GzipWriter) -> string?
end

-- One entry of a zip archive; valid only until the iterator advances.
declare extern type ZipEntry with
    name: string
    size: number?
    is_dir: boolean
    open: (ZipEntry) -> ByteStream
    read: (ZipEntry) -> string
end

-- A `igp.batch` job being put together. Nothing the partner sees exists until `submit`.
declare extern type BatchJob with
    -- Adds a named query for the partner; returns the name, which `rows`/`open` take later.
    query: (BatchJob, name: string, query: string, params: any?) -> string
    -- A request the host performs itself after `submit`, however long it takes (Fargate only). The
    -- result is read with `results[name]` (status, headers) and `body(name)`.
    http: (BatchJob, name: string, opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, basic_auth: { username: string, password: string }?, vault: { string }? }) -> string
    -- Attaches a file (or one more part of the same input) to the request: a string or a stream.
    input: (BatchJob, name: string, content: string | ByteStream, opts: { ext: string? }?) -> string
    -- Attaches rows, as NDJSON cut into parts: an array or an iterator function.
    input_rows: (BatchJob, name: string, rows: { any } | () -> any?, opts: { ext: string? }?) -> string
    -- Saved for the continuation; the partner never sees it.
    state: (BatchJob, value: any) -> ()
    -- Script that `igp.batch.route` picks when the partner's done.json arrives.
    continue_with: (BatchJob, script: string) -> ()
    id: (BatchJob) -> string
    submit: (BatchJob) -> string
end

declare extern type BatchResult with
    id: string
    ok: boolean
    error: string?
    state: any
    results: { [string]: { rows: number?, parts: number, status: number?, headers: { [string]: string }? } }
    -- Body of a `job:http` response, as a string (capped at max_object_mb).
    body: (BatchResult, name: string) -> string
    parts: (BatchResult, name: string) -> number
    -- Stream of one part (1-based), gunzipped if needed, checked against the manifest at its end.
    open: (BatchResult, name: string, part: number?) -> ByteStream
    -- Rows of every part in order; raises if a part is missing, corrupt or the row count is off.
    rows: (BatchResult, name: string) -> () -> any?
    -- Every object of the job, for a queue handler's `delete =` result (at most 100).
    keys: (BatchResult) -> { { store: string, key: string } }
end

-- What an ack records for a member. `ok = false` still resolves the member.
type BarrierOutcome = { ok: boolean, error: string?, result: any }

declare extern type BarrierResult with
    id: string
    total: number
    state: any
    -- Every member and its recorded outcome, in member order.
    results: (BarrierResult) -> () -> (string, BarrierOutcome)?
    -- The members whose outcome is `ok = false`.
    failures: (BarrierResult) -> { { member: string, error: string?, result: any } }
end

declare extern type HttpResponse with
    status: number
    headers: { [string]: string }
    body: string
end

declare extern type HttpStreamResponse with
    status: number
    headers: { [string]: string }
    body: ByteStream
end

declare extern type DbRow with
    sk: string
    value: any
end

-- Object functions for one store, scoped to the tenant's prefix. Which stores exist, and whether
-- they are read, write or readwrite, comes from the tenant's config.json `stores` block.
-- Store names are operator-defined; the conventional ones are In, Out, Backup and Temp.
-- A name may also be a whitelisted host directory (see docs/filesystem.md); the functions are the same,
-- except presign_get and presign_put, which directories do not support.
declare extern type Store with
    get: (key: string) -> string?
    put: (key: string, body: string) -> ()
    delete: (key: string) -> ()
    list: (prefix: string?) -> { string }
    open: (key: string) -> ByteStream?
    create: (key: string) -> Sink
    presign_get: (key: string, ttl_s: number?) -> string
    presign_put: (key: string, ttl_s: number?) -> string
end

declare ig: {
    version: number,
    log: (level: "error" | "warn" | "info" | "debug", message: string) -> (),
    config: { [string]: any },
    task: {
        title: (text: string) -> (),
        metric: (name: string, value: number) -> (),
        add: (name: string, delta: number?) -> (),
        attr: (name: string, value: string) -> (),
        detail: (value: any) -> (),
    },
    json: {
        encode: (value: any) -> string,
        decode: (text: string) -> any,
    },
    time: {
        now_ms: () -> number,
    },
    store: { [string]: Store },
    db: {
        get: (pk: string, sk: string?) -> any,
        put: (pk: string, sk: string?, value: any) -> (),
        -- Writes only if the key holds no live item; returns whether it wrote.
        put_if_absent: (pk: string, sk: string?, value: any) -> boolean,
        -- Atomic counter: adds `delta` (default 1) and returns the new value. Missing or expired starts at 0.
        add: (pk: string, sk: string?, delta: number?) -> number,
        delete: (pk: string, sk: string?) -> (),
        query: (pk: string, opts: { sk_prefix: string?, limit: number? }?) -> { DbRow },
    },
    timer: {
        -- Unix seconds. Returns due_s; cancel needs the same id and due_s.
        at: (id: string, due_s: number, message: { script: string?, version: string?, event: any }) -> number,
        after: (id: string, delay_s: number, message: { script: string?, version: string?, event: any }) -> number,
        cancel: (id: string, due_s: number) -> (),
    }
    queue: {
        -- Persist a stable-id handoff for the minute sweep; false for an identical existing intent.
        handoff: (id: string, message: { script: string?, version: string?, event: any }) -> boolean,
        -- One message or an array; returns how many were sent.
        send: (messages: { script: string?, version: string?, event: any, delay_s: number? } | { { script: string?, version: string?, event: any, delay_s: number? } }) -> number,
    },
    crypto: {
        encrypt: (key: string, plaintext: string, aad: string?) -> string,
        decrypt: (key: string, ciphertext: string, aad: string?) -> string,
        is_current: (ciphertext: string) -> boolean,
        hmac_sha256: (key: string, data: string, key_id: number?) -> string,
        public_key: (key: string, key_id: number?) -> string,
        seal: (public_key: string, plaintext: string) -> string,
        open: (key: string, ciphertext: string) -> string,
        sha256: (data: string) -> string,
        random: (n: number) -> string,
        hex_encode: (data: string) -> string,
        hex_decode: (hex: string) -> string,
        base64_encode: (data: string) -> string,
        base64_decode: (text: string) -> string,
    },
    http: {
        request: ((opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, basic_auth: { username: string, password: string }?, vault: { string }?, stream: true }) -> HttpStreamResponse)
            & ((opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, basic_auth: { username: string, password: string }?, vault: { string }?, stream: false? }) -> HttpResponse),
        get: (url: string, headers: { [string]: string }?, basic_auth: { username: string, password: string }?, vault: { string }?) -> HttpResponse,
    },
    xml: {
        events: (source: string | ByteStream, opts: { trim: boolean? }?) -> () -> XmlEvent?,
        extract: (source: string | ByteStream, rules: string) -> () -> { [string]: any }?,
        parse: (source: string | ByteStream, opts: { trim: boolean? }?) -> XmlNode,
        writer: ((sink: Sink?, opts: { declaration: boolean?, indent: number? }?) -> XmlWriter)
            & ((opts: { declaration: boolean?, indent: number? }) -> XmlWriter),
    },
    sql: {
        open: () -> SqlDatabase,
        load: (source: string | ByteStream) -> SqlDatabase,
        null: any,
    },
    gzip: {
        open: (source: string | ByteStream) -> ByteStream,
        writer: (sink: Sink?) -> GzipWriter,
        compress: (data: string) -> string,
        decompress: (data: string) -> string,
    },
    ndjson: {
        rows: (source: string | ByteStream) -> () -> any?,
    },
    deadline: {
        with: <T...>(ms: number, fn: () -> T...) -> T...,
    },
    zip: {
        entries: (source: string | ByteStream) -> () -> ZipEntry?,
        writer: (sink: Sink?) -> ZipWriter,
    },
}

-- Exact decimals (mlua-decimal): the global D and the Decimal type.
-- An exact base-10 number (up to 28 digits).
-- Arithmetic (+ - * / // %) and ordering (< <= ==) work between two Decimals; an integer may stand in for either
-- operand. A Luau number with a fraction is an error in arithmetic (write D"0.1"), and `d < 5` and `d == 5`
-- do not work either (Luau never asks a userdata about a number): use d:lt(5), d:eq(5) or d:cmp(5).
declare extern type Decimal with
	function __add(self, other: Decimal | number): Decimal
	function __sub(self, other: Decimal | number): Decimal
	function __mul(self, other: Decimal | number): Decimal
	function __div(self, other: Decimal | number): Decimal
	function __idiv(self, other: Decimal | number): Decimal
	function __mod(self, other: Decimal | number): Decimal
	function __unm(self): Decimal
	function __eq(self, other: Decimal): boolean
	function __lt(self, other: Decimal): boolean
	function __le(self, other: Decimal): boolean
	-- -1, 0 or 1; the other operand is a Decimal or an integer
	function cmp(self, other: Decimal | number): number
	function eq(self, other: Decimal | number): boolean
	function lt(self, other: Decimal | number): boolean
	function le(self, other: Decimal | number): boolean
	-- digits may be negative (round to tens); mode is half_even (the default), half_up, half_down, down, up, floor or ceil
	function round(self, digits: number?, mode: string?): Decimal
	function floor(self): Decimal
	function ceil(self): Decimal
	function trunc(self): Decimal
	function abs(self): Decimal
	function sign(self): number
	-- the digits after the decimal point: D"1.50":scale() is 2
	function scale(self): number
	-- without trailing zeros: D"1.50":normalize() is 1.5
	function normalize(self): Decimal
	function is_integer(self): boolean
	-- a string that is the same for equal values, to key a table: D"1.50":key() == D"1.5":key()
	function key(self): string
	-- the explicit, lossy way out to a Luau number
	function to_number(self): number
end

-- D"12.50", D(3) or D(d): a Decimal from its text, an integer or a Decimal (a float with a fraction is refused).
-- It takes `any` on purpose: `local price = D(row.Price)` types a cell, a variable or an `evaluate` result, which the
-- checker cannot see into, so that `price < 5` and `price == 5` are reported before the script runs.
declare D: ((v: any) -> Decimal) & {
	max: (...(Decimal | number)) -> Decimal,
	min: (...(Decimal | number)) -> Decimal,
	sum: ({ Decimal | number }) -> Decimal,
	-- the explicit way to turn a float into a decimal: D.from_float(0.1) is exactly 0.1
	from_float: (f: number) -> Decimal,
}

-- Opinionated coordination patterns. Versioned independently from the core API.
declare extern type PatternsV1 with
    version: number
    retry: {
        schedule: (id: string, message: { script: string, version: string?, event: any }, opts: { attempt: number, max_attempts: number?, base_delay_s: number?, max_delay_s: number?, jitter: boolean?, retry_after_s: number? }) -> { state: "scheduled", attempt: number, due_s: number } | { state: "exhausted" },
    }
    claim: {
        -- Acquires expired/absent work, reports busy ownership, or returns its durable result.
        acquire: (id: string, opts: { lease_s: number? }?) -> { state: "acquired", token: string, expires_s: number } | { state: "busy", expires_s: number } | { state: "complete", result: any },
        -- True for a successful completion or an identical retry by the same owner.
        complete: (id: string, token: string, result: any) -> boolean,
        -- New expiry, or nil when ownership was lost or a concurrent transition won.
        renew: (id: string, token: string, opts: { lease_s: number? }?) -> number?,
    }
    callback: {
        create: (opts: { script: string?, state: any, ttl_s: number? }?) -> string,
    }
    batch: {
        new: (opts: { id: string? }?) -> BatchJob,
        -- nil when the job is not complete or was already cleaned up.
        hydrate: (event: { [string]: any }, opts: { allow_failure: boolean? }?) -> BatchResult?,
        -- For a router: nil unless `event` is a done.json from the batch bucket.
        route: (event: { [string]: any }) -> { job: string, script: string?, event: { [string]: any } }?,
    }
    barrier: {
        -- Registers members (at most 10 000); returns the id. Default sealed=true needs members.
        -- sealed=false opens a growable list, with 0–99 initial members (members may be omitted).
        create: (opts: { id: string?, members: { string }?, sealed: boolean?, continue_with: string?, state: any, deadline_s: number?, overdue_with: string? }) -> string,
        -- Open lists only: atomically adds 1–99 members, returning the count of new names.
        add: (id: string, members: { string }) -> number,
        -- Closes the list; true only if this call dispatched the continuation. Safe to repeat.
        seal: (id: string) -> boolean,
        -- Records a member's outcome (the first ack wins). `complete` once sealed and all resolved;
        -- `dispatched` when this call sent the continuation.
        ack: (id: string, member: string, outcome: { ok: boolean?, error: string?, result: any }?) -> { duplicate: boolean, complete: boolean, dispatched: boolean },
        -- Retries dispatch for a complete, sealed barrier; true only if this call sent. Active lease raises.
        resume: (id: string) -> boolean,
        -- Checks a page of pending members; nil leaves a member pending. Errors propagate.
        -- Default limit 10, maximum 100. Continue with next_after; nil ends this pass.
        reconcile: (id: string, check: (member: string) -> { ok: boolean?, error: string?, result: any }?, opts: { after: string?, limit: number? }?) -> { checked: number, acknowledged: number, next_after: string?, complete: boolean, dispatched: boolean },
        -- Up to `limit` (default and maximum 100) members still waiting, in name order.
        pending: (id: string, limit: number?) -> { string },
        status: (id: string) -> { id: string, total: number, ready: boolean, sealed: boolean, complete: boolean, dispatched: boolean, overdue: boolean, due_s: number? },
        -- In the continuation: nil if the barrier is not complete or has expired.
        hydrate: (event: { [string]: any }) -> BarrierResult?,
    }
    lock: {
        with: <T...>(name: string, opts: { scope: ("local" | "db")?, wait_ms: number?, lease_ms: number? }?, fn: () -> T...) -> (boolean, T...),
    }
end

-- Built-in imports are version-pinned; tenant/shared modules retain their own shapes.
declare require: ((spec: "@native/patterns@1") -> PatternsV1) & ((spec: string) -> any)
