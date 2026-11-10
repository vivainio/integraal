import { render } from 'preact'
import { useEffect, useMemo, useState } from 'preact/hooks'
import type { ComponentChildren } from 'preact'
import { getFlowLogs, inspectFlow, listFlows } from './api'
import type { Connection, Flow, FlowLog, Inspection } from './api'
import { RunDialog } from './components/RunDialog'
import './style.css'

const today = () => new Date().toISOString().slice(0, 10)
const dateTime = (ms?: number) => typeof ms === 'number' ? new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'UTC' }).format(ms) : '—'
const scriptLabel = (script?: string) => script === '<routed>' ? 'Routed invocation' : script || 'Unnamed script'
const shortId = (id: string) => `${id.slice(0, 8)}…${id.slice(-4)}`
const prettyDay = (day: string) => new Date(`${day}T12:00:00Z`).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' })

function Icon({ name, size = 18 }: { name: string; size?: number }) {
  const paths: Record<string, ComponentChildren> = {
    grid: <><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></>,
    flow: <><circle cx="5" cy="5" r="2"/><circle cx="19" cy="5" r="2"/><circle cx="12" cy="19" r="2"/><path d="M7 5h10M5 7v4c0 4 3 5 7 6M19 7v4c0 4-3 5-7 6"/></>,
    book: <><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v17H6.5A2.5 2.5 0 0 1 4 17.5z"/><path d="M4 17.5A2.5 2.5 0 0 1 6.5 15H20M8 7h8M8 10h6"/></>,
    arrow: <><path d="M7 17 17 7M7 7h10v10"/></>,
    right: <path d="m9 18 6-6-6-6"/>,
    refresh: <><path d="M20 7v5h-5M4 17v-5h5"/><path d="M5 9a7 7 0 0 1 12-2l3 5M4 12l3 5a7 7 0 0 0 12-2"/></>,
    search: <><circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 4 4"/></>,
    sliders: <><path d="M4 7h16M4 17h16"/><circle cx="9" cy="7" r="2" fill="currentColor" stroke="none"/><circle cx="15" cy="17" r="2" fill="currentColor" stroke="none"/></>,
    clock: <><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></>,
    copy: <><rect x="8" y="8" width="11" height="11" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2"/></>,
    close: <path d="M6 6l12 12M18 6 6 18"/>,
    check: <path d="m5 12 4 4L19 6"/>,
    plug: <><path d="M8 3v6M16 3v6M5 9h14v2a7 7 0 0 1-7 7v3M12 18a7 7 0 0 1-7-7V9"/></>,
    bolt: <path d="M13 2 4 14h7l-1 8 10-12h-7z"/>,
  }
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">{paths[name]}</svg>
}

function ConnectionDialog({ onClose, onConnect, initial }: { onClose: () => void; onConnect: (c: Connection) => void; initial: Connection | null }) {
  const [url, setUrl] = useState(initial?.url === '/__local' ? '' : initial?.url ?? '')
  const [token, setToken] = useState(initial?.token ?? '')
  const [error, setError] = useState('')
  return <div class="modal-backdrop" onClick={onClose}><section class="modal" role="dialog" aria-modal="true" aria-labelledby="connect-title" onClick={e => e.stopPropagation()}>
    <div class="modal-top"><span class="modal-icon"><Icon name="plug" size={20}/></span><button class="icon-button" onClick={onClose} aria-label="Close"><Icon name="close"/></button></div>
    <p class="eyebrow">YOUR ENVIRONMENT</p><h2 id="connect-title">Connect to Integraal</h2>
    <p class="modal-description">Point l8l at your control plane to explore flow history. Your token stays in this browser tab's memory.</p>
    {import.meta.env.DEV && <button class="local-button" onClick={() => onConnect({ url: '/__local', token: '' })}><Icon name="bolt" size={15}/> Use local Integraal <span>acme · 127.0.0.1</span></button>}
    <form onSubmit={e => { e.preventDefault(); try { const u = new URL(url); if (!(u.protocol === 'https:' || (u.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(u.hostname))) || u.username || u.password || u.search || u.hash) throw new Error(); if (!token.trim()) throw new Error(); onConnect({ url: u.toString().replace(/\/$/, ''), token: token.trim() }) } catch { setError('Enter an HTTPS API URL (or local HTTP URL) and an access token.') } }}>
      <label>Control plane URL<input type="url" value={url} onInput={e => setUrl(e.currentTarget.value)} placeholder="https://your-api.execute-api.region.amazonaws.com" required /></label>
      <label>Access token<input type="password" value={token} onInput={e => setToken(e.currentTarget.value)} placeholder="Paste your bearer token" autoComplete="off" required /></label>
      {error && <p class="form-error" role="alert">{error}</p>}
      <button class="primary-button" type="submit">Connect to control plane <Icon name="right" size={16}/></button>
    </form><p class="modal-foot">The API must allow this site's origin through CORS.</p>
  </section></div>
}

function FlowDetail({ flow, inspection, loading, error, logs, logsLoading, logsError }: { flow: Flow | undefined; inspection: Inspection | null; loading: boolean; error: string; logs: FlowLog[]; logsLoading: boolean; logsError: string }) {
  const [copied, setCopied] = useState(false)
  useEffect(() => setCopied(false), [flow?.trace_id])
  if (!flow) return <aside class="detail"><div class="detail-head"><div><span class="section-kicker">INSPECT</span><h2>Flow details</h2></div></div><div class="detail-empty"><span class="empty-icon"><Icon name="flow" size={23}/></span><h3>Select a flow</h3><p>Choose a flow from the list to see its invocations and pending state.</p></div></aside>
  const queuedInvocations = inspection?.invocations?.filter(inv => inv.trigger === 'queue') ?? []
  const stateEntries = Object.entries(inspection?.state ?? {}).filter(([key]) => key !== 'covers')
  const scriptBySpan = new Map<string, string>()
  for (const entry of logs) {
    if (entry.type === 'header' && typeof entry.span_id === 'string') scriptBySpan.set(entry.span_id, scriptLabel(entry.script))
  }
  const describeLog = (entry: FlowLog) => {
    const script = typeof entry.span_id === 'string' ? scriptBySpan.get(entry.span_id) : undefined
    if (entry.type === 'header') return `Started ${scriptLabel(entry.script)}`
    if (entry.type === 'end') {
      const outcome = String(entry.outcome ?? 'unknown')
      const result = outcome === 'ok' ? 'Completed' : outcome === 'script_error' ? 'Failed' : `Finished (${outcome})`
      const duration = typeof entry.duration_ms === 'number' ? ` · ${entry.duration_ms} ms` : ''
      return `${result} ${script ?? 'invocation'}${duration}`
    }
    if (entry.msg) return entry.msg
    if (entry.type === 'route') return `${String(entry.router)} → ${entry.script}`
    if (entry.type === 'workflow_step') return `${String(entry.name)} · ${String(entry.status)}`
    if (entry.type === 'queue_dispatch') return `${Array.isArray(entry.targets) ? entry.targets.length : 0} queued message(s) sent`
    return entry.type
  }
  const workflowSteps = new Map<string, string>()
  for (const entry of logs) {
    if (entry.type !== 'workflow_step' || typeof entry.name !== 'string' || typeof entry.status !== 'string') continue
    const previous = workflowSteps.get(entry.name)
    if (entry.status === 'replayed' && (previous === 'completed' || previous === 'failed')) continue
    workflowSteps.set(entry.name, entry.status)
  }
  return <aside class="detail"><div class="detail-head"><div><span class="section-kicker">INSPECT</span><h2>Flow details</h2></div><span class="detail-count">LIVE</span></div>
    <div class="detail-content"><div class="detail-title"><span class="flow-icon"><Icon name="flow" size={17}/></span><div><h3>{scriptLabel(flow.script)}</h3><p>Started {dateTime(flow.start_ms)} UTC</p></div></div>
      <div class="info-block"><div class="info-label">TRACE ID <button class="copy-button" onClick={async () => { try { await navigator.clipboard.writeText(flow.trace_id); setCopied(true) } catch { setCopied(false) } }} aria-label="Copy trace ID"><Icon name={copied ? 'check' : 'copy'} size={14}/>{copied && <span>Copied</span>}</button></div><code class="trace-full">{flow.trace_id}</code></div>
      <div class="detail-rule"/><div class="detail-section-heading"><div><span class="section-kicker">EXECUTION</span><h3>Invocations</h3></div><span class="number-pill">{inspection?.invocations?.length ?? '—'}</span></div>
      {loading ? <p class="muted-message">Loading inspection…</p> : error ? <p class="error-message" role="alert">{error}</p> : inspection && Array.isArray(inspection.invocations) && inspection.invocations.length > 0 ? <div class="timeline">{inspection.invocations.map((inv, i) => <div class="timeline-item" key={`${inv.span_id}-${i}`}><span class={`timeline-dot ${inv.outcome === 'ok' ? 'success' : inv.outcome ? 'failure' : ''}`}><Icon name={inv.outcome === 'ok' ? 'check' : inv.outcome ? 'close' : 'clock'} size={12}/></span><div class="timeline-body"><div class="timeline-line"><strong>{scriptLabel(inv.script)}</strong><span class={`outcome ${inv.outcome === 'ok' ? 'ok' : inv.outcome ? 'failed' : ''}`}>{inv.outcome ?? 'No outcome'}</span></div><p>{inv.version || 'Unversioned'} <span>·</span> {inv.trigger || 'Unknown trigger'} <span>·</span> {typeof inv.duration_ms === 'number' ? `${inv.duration_ms} ms` : 'Duration unavailable'}</p>{inv.error != null && <pre class="error-detail">{typeof inv.error === 'string' ? inv.error : JSON.stringify(inv.error, null, 2)}</pre>}</div></div>)}</div> : <p class="muted-message">No invocation summaries found.</p>}
      {queuedInvocations.length > 0 && <><div class="detail-rule"/><div class="detail-section-heading"><div><span class="section-kicker">QUEUE</span><h3>Queued invocations</h3></div><span class="number-pill">{queuedInvocations.length}</span></div><p class="queue-explanation">Queue deliveries recorded in this flow. Individual parent links are not recorded.</p><div class="queued-cards">{queuedInvocations.map((inv, index) => <div class="queued-card" key={`${inv.span_id}-${index}`}><span class={`queued-node ${inv.outcome === 'ok' ? 'workflow-done' : inv.outcome ? 'workflow-failed' : ''}`}><Icon name={inv.outcome === 'ok' ? 'check' : inv.outcome ? 'close' : 'clock'} size={12}/></span><div><strong>{scriptLabel(inv.script)}</strong><small>{shortId(inv.span_id)} · {inv.outcome ?? 'No outcome'}</small></div></div>)}</div></>}
      {workflowSteps.size > 0 && <><div class="detail-rule"/><div class="detail-section-heading"><div><span class="section-kicker">WORKFLOW</span><h3>Step execution</h3></div><span class="number-pill">{workflowSteps.size}</span></div><div class="workflow-steps">{Array.from(workflowSteps, ([name, status], index) => <div class="workflow-step" key={name}><span class={`workflow-node ${status === 'completed' || status === 'replayed' ? 'workflow-done' : status === 'failed' ? 'workflow-failed' : ''}`}>{status === 'completed' || status === 'replayed' ? <Icon name="check" size={12}/> : status === 'failed' ? <Icon name="close" size={12}/> : index + 1}</span><div><strong>{name}</strong><small>{status === 'replayed' ? 'Previously completed' : status}</small></div></div>)}</div></>}
      <div class="detail-rule"/><div class="detail-section-heading"><div><span class="section-kicker">OBSERVABILITY</span><h3>Flow log</h3></div><span class="number-pill">{logs.length}</span></div>
      {logsLoading ? <p class="muted-message">Loading log entries…</p> : logsError ? <p class="error-message" role="alert">{logsError}</p> : logs.length ? <div class="log-entries">{logs.map((entry, index) => <details class="log-entry" key={index}><summary><span class="log-time">{dateTime(entry.t ?? entry.start_ms)}</span><span class={`log-level ${entry.level === 'warn' || entry.level === 'error' ? 'log-warning' : ''}`}>{entry.level || entry.type}</span><span class="log-message">{describeLog(entry)}</span></summary><pre>{JSON.stringify(entry, null, 2)}</pre></details>)}</div> : <p class="muted-message">No log entries found.</p>}
      <div class="detail-rule"/><div class="detail-section-heading"><div><span class="section-kicker">DURABLE STATE</span><h3>Pending items</h3></div></div>
      {inspection ? <div class="pending-list">{stateEntries.length ? stateEntries.map(([key, value]) => <div class="pending-row" key={key}><span>{key}</span><strong>{Array.isArray(value) ? value.length : value == null ? '—' : typeof value === 'object' ? JSON.stringify(value) : String(value)}</strong></div>) : <p class="muted-message">No pending state available.</p>}</div> : <p class="muted-message">{loading ? 'Loading…' : 'Select a flow to inspect its state.'}</p>}
    </div></aside>
}

function App() {
  const [connection, setConnection] = useState<Connection | null>(import.meta.env.DEV ? { url: '/__local', token: '' } : null)
  const [dialog, setDialog] = useState(false)
  const [runDialog, setRunDialog] = useState(false)
  const [day, setDay] = useState(today)
  const [query, setQuery] = useState('')
  const [flows, setFlows] = useState<Flow[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [inspection, setInspection] = useState<Inspection | null>(null)
  const [loading, setLoading] = useState(false)
  const [inspectLoading, setInspectLoading] = useState(false)
  const [error, setError] = useState('')
  const [inspectError, setInspectError] = useState('')
  const [logs, setLogs] = useState<FlowLog[]>([])
  const [logsLoading, setLogsLoading] = useState(false)
  const [logsError, setLogsError] = useState('')
  const [refresh, setRefresh] = useState(0)
  useEffect(() => {
    if (!connection || !day) return
    const controller = new AbortController(); setLoading(true); setError(''); setFlows([]); setSelected(null); setInspection(null)
    listFlows(connection, day, controller.signal).then(setFlows).catch(e => { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : 'Could not load flows.') }).finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [connection, day, refresh])
  useEffect(() => {
    setInspection(null); setInspectError('')
    if (!selected) { setInspectLoading(false); return }
    if (!connection) return
    const controller = new AbortController(); setInspectLoading(true)
    inspectFlow(connection, selected, controller.signal).then(setInspection).catch(e => { if (!controller.signal.aborted) setInspectError(e instanceof Error ? e.message : 'Could not inspect this flow.') }).finally(() => { if (!controller.signal.aborted) setInspectLoading(false) })
    return () => controller.abort()
  }, [connection, selected])
  useEffect(() => {
    setLogs([]); setLogsError('')
    if (!selected || !connection) { setLogsLoading(false); return }
    const controller = new AbortController(); setLogsLoading(true)
    getFlowLogs(connection, selected, controller.signal).then(setLogs).catch(e => { if (!controller.signal.aborted) setLogsError(e instanceof Error ? e.message : 'Could not load flow logs.') }).finally(() => { if (!controller.signal.aborted) setLogsLoading(false) })
    return () => controller.abort()
  }, [connection, selected])
  const visibleFlows = useMemo(() => { const q = query.trim().toLowerCase(); return flows.filter(f => `${f.script} ${f.trace_id} ${f.trigger}`.toLowerCase().includes(q)) }, [flows, query])
  const activeFlow = flows.find(f => f.trace_id === selected)
  return <div class="app-shell"><aside class="sidebar"><div class="brand"><span class="brand-mark"><span>l</span><span>8</span><span>l</span></span><div><strong>l8l</strong><small>by integraal</small></div></div><div class="workspace-label">WORKSPACE <span>01</span></div><div class="workspace"><span class="workspace-avatar">i</span><div><strong>Integraal</strong><small>Flow explorer</small></div><span class="workspace-chev">⌄</span></div><div class="nav-label">EXPLORE</div><nav aria-label="Main navigation"><a class="nav-link active" href="#flows"><Icon name="flow" size={17}/><span>Flows</span><span class="nav-active-dot"/></a><a class="nav-link" href="https://vivainio.github.io/integraal/" target="_blank" rel="noreferrer"><Icon name="book" size={17}/><span>Documentation</span><Icon name="arrow" size={14}/></a></nav><div class="sidebar-spacer"/><div class="sidebar-card"><span class="sidebar-card-icon"><Icon name="bolt" size={16}/></span><p>Built for the work<br/><strong>behind the scenes.</strong></p><span>l8l / reference app</span></div><div class="sidebar-footer"><span class="connection-dot"/><div><strong>{connection?.url === '/__local' ? 'Local Integraal' : 'Control plane'}</strong><small>{connection?.url === '/__local' ? 'Tenant: acme' : connection ? 'API configured' : 'Not connected'}</small></div><button class="icon-button" onClick={() => setDialog(true)} aria-label="Connection settings"><Icon name="sliders" size={17}/></button></div></aside>
    <div class="main-wrap"><header class="topbar"><div class="breadcrumb"><span>Workspace</span><Icon name="right" size={13}/><strong>Flows</strong></div><div class="top-actions"><span class="top-status"><span class="connection-dot"/>{connection?.url === '/__local' ? 'Local · acme' : connection ? 'API configured' : 'Not connected'}</span><button class="run-trigger-button" onClick={() => setRunDialog(true)}>▶ <span>Run script</span></button><button class="connect-button" onClick={() => setDialog(true)}><Icon name="plug" size={15}/>{connection ? 'Connection' : 'Connect'}</button></div></header>
      <main id="flows"><div class="page-intro"><div><div class="page-overline"><span class="tiny-spark">✳</span> OPERATIONS / FLOW HISTORY</div><h1>Every flow, in view<span>.</span></h1><p>A closer look at the work moving through Integraal.</p></div><div class="page-tag"><span class="tag-dot"/> FLOW EXPLORER <span> / 01</span></div></div>
        <div class="content-grid"><section class="flow-panel"><div class="panel-heading"><div><span class="section-kicker">ACTIVITY</span><div class="title-line"><h2>Flow history</h2><span class="number-pill">{flows.length}</span></div><p>Flows started on the selected UTC day.</p></div><button class="refresh-button" onClick={() => setRefresh(n => n + 1)} disabled={loading} aria-label="Refresh flows"><Icon name="refresh" size={16}/></button></div>
          <div class="toolbar"><div class="search-box"><Icon name="search" size={16}/><input aria-label="Filter flows" placeholder="Filter by script, trigger or trace" value={query} onInput={e => setQuery(e.currentTarget.value)}/>{query && <button onClick={() => setQuery('')} aria-label="Clear filter"><Icon name="close" size={14}/></button>}</div><label class="date-control"><Icon name="clock" size={15}/><input aria-label="UTC day" type="date" value={day} onInput={e => { setDay(e.currentTarget.value); setSelected(null) }}/><span>UTC</span></label></div>
          <div class="list-caption"><span>{day ? prettyDay(day) : 'SELECT A DATE'}</span><span>{visibleFlows.length} {visibleFlows.length === 1 ? 'FLOW' : 'FLOWS'}</span></div>
          <div class="table-wrap"><table><thead><tr><th>FLOW / TRACE ID</th><th>TRIGGER</th><th>STARTED <span>UTC</span></th><th><span class="sr-only">Inspect</span></th></tr></thead><tbody>{visibleFlows.map(flow => <tr key={flow.trace_id} class={flow.trace_id === selected ? 'selected' : ''} onClick={() => setSelected(flow.trace_id)} tabIndex={0} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setSelected(flow.trace_id) } }} aria-label={`Inspect ${scriptLabel(flow.script)}, ${flow.trace_id}`}><td><div class="flow-name"><span class="table-flow-icon"><Icon name="flow" size={15}/></span><div><strong>{scriptLabel(flow.script)}</strong><code>{shortId(flow.trace_id)}</code></div></div></td><td><span class="trigger"><span/> {flow.trigger || 'unknown'}</span>{flow.routed && <span class="routed-label">routed</span>}</td><td class="time-cell">{dateTime(flow.start_ms)}</td><td class="row-arrow"><Icon name="right" size={16}/></td></tr>)}</tbody></table></div>
          {loading && <div class="list-empty"><span class="loading-spinner"/> Loading flows…</div>}{!loading && error && <div class="list-empty error-message" role="alert"><strong>Could not load flows.</strong><span>{error}</span><button onClick={() => setDialog(true)}>Check connection</button></div>}{!loading && !error && visibleFlows.length === 0 && <div class="list-empty"><Icon name="flow" size={22}/><strong>{query ? 'No matching flows' : connection ? 'No flows for this day' : 'Connect to Integraal'}</strong><span>{query ? 'Try a different search.' : connection ? 'Choose another UTC day or check back later.' : 'Use Connection to configure the control plane.'}</span></div>}
          <div class="panel-footer"><span><span class="footer-spark">✳</span> A little more context, a lot more clarity.</span><span>SHOWING {visibleFlows.length} OF {flows.length}</span></div></section>
          <FlowDetail flow={activeFlow} inspection={inspection} loading={inspectLoading} error={inspectError} logs={logs} logsLoading={logsLoading} logsError={logsError}/></div><footer class="app-footer"><span>l8l <span>—</span> a reference app for Integraal</span><span>MADE FOR THE IN-BETWEEN <span>↗</span></span></footer>
      </main></div>{runDialog && <RunDialog connection={connection} onClose={() => setRunDialog(false)} onRun={() => setRefresh(n => n + 1)}/>} {dialog && <ConnectionDialog initial={connection} onClose={() => setDialog(false)} onConnect={c => { setConnection(c); setSelected(null); setDay(today()); setDialog(false) }}/>}</div>
}

render(<App />, document.getElementById('app')!)
