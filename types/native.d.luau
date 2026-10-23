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
        delete: (pk: string, sk: string?) -> (),
        query: (pk: string, opts: { sk_prefix: string?, limit: number? }?) -> { DbRow },
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
        request: ((opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, stream: true }) -> HttpStreamResponse)
            & ((opts: { method: string?, url: string, headers: { [string]: string }?, body: string?, stream: false? }) -> HttpResponse),
        get: (url: string, headers: { [string]: string }?) -> HttpResponse,
    },
    xml: {
        events: (source: string | ByteStream, opts: { trim: boolean? }?) -> () -> XmlEvent?,
    },
}
