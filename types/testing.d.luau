-- Load alongside native.d.luau when typechecking user test suites.
declare extern type PeerDocument with
    job: string
    key: string
    body: string
    request: any
end

declare test: {
    peer: {
        receive: (job: string, input: string?, part: number?) -> PeerDocument,
        ack: (document: PeerDocument, options: { ok: boolean?, error: string?, results: { [string]: any }? }?) -> any,
    },
    case: (name: string, fn: () -> ()) -> (),
    equal: (actual: any, expected: any) -> (),
    throws: (fn: () -> (), contains: string?) -> (),
    load: (path: string) -> any,
    http: (method: string, url: string, response: any) -> (),
    http_calls: { { method: string, url: string, headers: { [string]: string }, body: string? } },
    mock: (path: string, implementation: any) -> { calls: { any } },
}
