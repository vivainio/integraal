-- Type definitions for the integraal `native` API, runtime version 1.
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

-- Streaming upload from `Bucket.create`; the object appears on `close`.
declare extern type Sink with
    write: (self, data: string) -> ()
    close: (self) -> number
    abort: (self) -> ()
end

declare extern type ZipWriter with
    add: (self, name: string, content: (string | ByteStream)?) -> ()
    write: (self, data: string) -> ()
    finish: (self) -> string?
end

declare extern type SqlDatabase with
    exec: (self, sql: string, params: { any }?) -> number
    query: (self, sql: string, params: { any }?) -> { { [string]: any } }
    get: (self, sql: string, params: { any }?) -> { [string]: any }?
    last_insert_id: (self) -> number
    save: (self, sink: Sink) -> number
    dump: (self) -> string
    close: (self) -> ()
end

declare extern type GzipWriter with
    write: (self, data: string) -> ()
    finish: (self) -> string?
end

-- One entry of a zip archive; valid only until the iterator advances.
declare extern type ZipEntry with
    name: string
    size: number?
    is_dir: boolean
    open: (self) -> ByteStream
    read: (self) -> string
end

-- A `native.batch` job being put together. Nothing the partner sees exists until `submit`.
declare extern type BatchJob with
    -- Adds a named query for the partner; returns the name, which `rows`/`open` take later.
    query: (self, name: string, query: string, params: any?) -> string
    -- A request the host performs itself after `submit`, however long it takes (Fargate only). The
    -- result is read with `results[name]` (status, headers) and `body(name)`.
    http: (self, name: string, opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, vault: { string }? }) -> string
    -- Attaches a file (or one more part of the same input) to the request: a string or a stream.
    input: (self, name: string, content: string | ByteStream, opts: { ext: string? }?) -> string
    -- Attaches rows, as NDJSON cut into parts: an array or an iterator function.
    input_rows: (self, name: string, rows: { any } | () -> any?, opts: { ext: string? }?) -> string
    -- Saved for the continuation; the partner never sees it.
    state: (self, value: any) -> ()
    -- Script that `native.batch.route` picks when the partner's done.json arrives.
    continue_with: (self, script: string) -> ()
    id: (self) -> string
    submit: (self) -> string
end

declare extern type BatchResult with
    id: string
    ok: boolean
    error: string?
    state: any
    results: { [string]: { rows: number?, parts: number, status: number?, headers: { [string]: string }? } }
    -- Body of a `job:http` response, as a string (capped at max_object_mb).
    body: (self, name: string) -> string
    parts: (self, name: string) -> number
    -- Stream of one part (1-based), gunzipped if needed, checked against the manifest at its end.
    open: (self, name: string, part: number?) -> ByteStream
    -- Rows of every part in order; raises if a part is missing, corrupt or the row count is off.
    rows: (self, name: string) -> () -> any?
    -- Every object of the job, for a queue handler's `delete =` result (at most 100).
    keys: (self) -> { { bucket: string, key: string } }
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

-- Object functions for one bucket, scoped to the tenant's prefix. Which buckets exist, and whether
-- they are read, write or readwrite, comes from the tenant's config.json `buckets` block.
-- Bucket names are operator-defined; the conventional ones are In, Out, Backup and Temp.
declare extern type Bucket with
    get: (key: string) -> string?
    put: (key: string, body: string) -> ()
    delete: (key: string) -> ()
    list: (prefix: string?) -> { string }
    open: (key: string) -> ByteStream?
    create: (key: string) -> Sink
    presign_get: (key: string, ttl_s: number?) -> string
    presign_put: (key: string, ttl_s: number?) -> string
end

declare native: {
    version: number,
    log: (level: "error" | "warn" | "info" | "debug", message: string) -> (),
    config: { [string]: any },
    json: {
        encode: (value: any) -> string,
        decode: (text: string) -> any,
    },
    time: {
        now_ms: () -> number,
    },
    bucket: { [string]: Bucket },
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
    queue: {
        -- One message or an array; returns how many were sent.
        send: (messages: { script: string?, version: string?, event: any, delay_s: number? } | { { script: string?, version: string?, event: any, delay_s: number? } }) -> number,
    },
    callback: {
        create: (opts: { script: string?, state: any, ttl_s: number? }?) -> string,
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
        request: ((opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, vault: { string }?, stream: true }) -> HttpStreamResponse)
            & ((opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, vault: { string }?, stream: false? }) -> HttpResponse),
        get: (url: string, headers: { [string]: string }?, vault: { string }?) -> HttpResponse,
    },
    xml: {
        events: (source: string | ByteStream, opts: { trim: boolean? }?) -> () -> XmlEvent?,
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
    batch: {
        new: (opts: { id: string? }?) -> BatchJob,
        -- nil when the job is not complete or was already cleaned up.
        hydrate: (event: { [string]: any }, opts: { allow_failure: boolean? }?) -> BatchResult?,
        -- For a router: nil unless `event` is a done.json from the batch bucket.
        route: (event: { [string]: any }) -> { job: string, script: string?, event: { [string]: any } }?,
    },
    ndjson: {
        rows: (source: string | ByteStream) -> () -> any?,
    },
    lock: {
        with: <T...>(name: string, opts: { scope: ("local" | "db")?, wait_ms: number?, lease_ms: number? }?, fn: () -> T...) -> (boolean, T...),
    },
    deadline: {
        with: <T...>(ms: number, fn: () -> T...) -> T...,
    },
    zip: {
        entries: (source: string | ByteStream) -> () -> ZipEntry?,
        writer: (sink: Sink?) -> ZipWriter,
    },
}
