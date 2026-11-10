export type Flow = { trace_id: string; tenant: string; script: string; routed?: boolean; trigger: string; start_ms: number }
export type Invocation = { span_id: string; script?: string; version?: string; trigger?: string; start_ms?: number; duration_ms?: number; outcome: string | null; error?: unknown; log_lines?: number; failed_http?: unknown[] }
export type Inspection = { invocations: Invocation[]; state: Record<string, unknown> }
export type FlowLog = { type: string; t?: number; start_ms?: number; level?: string; msg?: string; script?: string; span_id?: string; [key: string]: unknown }
export type Connection = { url: string; token: string }

async function request<T>(connection: Connection, path: string, signal?: AbortSignal, body?: unknown): Promise<T> {
  const response = await fetch(`${connection.url.replace(/\/$/, '')}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { Accept: 'application/json', ...(connection.token ? { Authorization: `Bearer ${connection.token}` } : {}), ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
    body: body === undefined ? undefined : JSON.stringify(body), signal,
  })
  if (!response.ok) {
    let message = `Request failed (${response.status})`
    try { const body = await response.json(); if (typeof body.error === 'string') message = body.error } catch { /* Non-JSON gateway error. */ }
    throw new Error(message)
  }
  return response.json() as Promise<T>
}

export async function listFlows(connection: Connection, day: string, signal?: AbortSignal): Promise<Flow[]> {
  const result = await request<{ flows: Flow[] }>(connection, `/flows?day=${encodeURIComponent(day)}`, signal)
  if (!Array.isArray(result.flows)) throw new Error('The API returned an invalid flow list.')
  return result.flows.sort((a, b) => b.start_ms - a.start_ms)
}
export function inspectFlow(connection: Connection, trace: string, signal?: AbortSignal): Promise<Inspection> {
  return request(connection, `/flows/${encodeURIComponent(trace)}`, signal)
}

export async function getFlowLogs(connection: Connection, trace: string, signal?: AbortSignal): Promise<FlowLog[]> {
  const result = await request<{ lines: FlowLog[] }>(connection, `/flows/${encodeURIComponent(trace)}/logs`, signal)
  if (!Array.isArray(result.lines)) throw new Error('The API returned invalid flow logs.')
  return result.lines
}

export type TriggerArgument = { name: string; label: string; type: 'string' | 'boolean' | 'number'; required: boolean; default?: string | boolean | number }
export type TriggerMetadata = { title: string; description: string; arguments: TriggerArgument[] }
export async function listScripts(connection: Connection, signal?: AbortSignal): Promise<string[]> {
  const result = await request<{ scripts: string[] }>(connection, '/scripts', signal)
  if (!Array.isArray(result.scripts)) throw new Error('The API returned an invalid script list.')
  return result.scripts
}
export async function triggerMetadata(connection: Connection, script: string, version: string, signal?: AbortSignal): Promise<TriggerMetadata> {
  const result = await request<{ result: TriggerMetadata }>(connection, `/scripts/${encodeURIComponent(script)}/trigger${version ? `?version=${encodeURIComponent(version)}` : ''}`, signal)
  if (!result.result || !Array.isArray(result.result.arguments)) throw new Error('The API returned invalid trigger metadata.')
  return result.result
}
export async function runTrigger(connection: Connection, script: string, version: string, args: Record<string, unknown>): Promise<unknown> {
  const result = await request<{ result: unknown }>(connection, `/scripts/${encodeURIComponent(script)}/trigger${version ? `?version=${encodeURIComponent(version)}` : ''}`, undefined, args)
  return result.result
}
