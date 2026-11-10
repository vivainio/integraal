import { useEffect, useState } from 'preact/hooks'
import { listScripts, runTrigger, triggerMetadata } from '../api'
import type { Connection, TriggerMetadata } from '../api'

type Props = { connection: Connection | null; onClose: () => void; onRun: () => void }

export function RunDialog({ connection, onClose, onRun }: Props) {
  const [scripts, setScripts] = useState<string[]>([])
  const [script, setScript] = useState('')
  const [version, setVersion] = useState('')
  const [metadata, setMetadata] = useState<TriggerMetadata | null>(null)
  const [values, setValues] = useState<Record<string, string | boolean>>({})
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<{ value: unknown } | null>(null)

  useEffect(() => {
    if (!connection) return
    const controller = new AbortController()
    listScripts(connection, controller.signal).then(setScripts).catch(e => { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : 'Could not list scripts.') })
    return () => controller.abort()
  }, [connection])
  useEffect(() => {
    setMetadata(null); setError(''); setResult(null); setValues({})
    if (!connection || !script) return
    const controller = new AbortController()
    setLoading(true)
    triggerMetadata(connection, script, version, controller.signal).then(meta => {
      setMetadata(meta)
      setValues(Object.fromEntries(meta.arguments.map(arg => [arg.name, arg.default === undefined ? (arg.type === 'boolean' ? false : '') : arg.type === 'boolean' ? Boolean(arg.default) : String(arg.default)])))
    }).catch(e => {
      if (!controller.signal.aborted) {
        const message = e instanceof Error ? e.message : 'Could not load trigger.'
        setError(message.includes('no on_user_trigger handler')
          ? `“${script}” does not have a user trigger. Add user_trigger metadata and an on_user_trigger handler to run it here.`
          : message)
      }
    }).finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [connection, script, version])

  async function submit(e: Event) {
    e.preventDefault()
    if (!connection || !metadata || running) return
    const args: Record<string, unknown> = {}
    for (const field of metadata.arguments) {
      const value = values[field.name]
      if (field.type === 'boolean') args[field.name] = value === true
      else if (value !== '' && value !== undefined) args[field.name] = field.type === 'number' ? Number(value) : value
    }
    setRunning(true); setError(''); setResult(null)
    try { setResult({ value: await runTrigger(connection, script, version, args) }); onRun() }
    catch (e) { setError(e instanceof Error ? e.message : 'Could not run script.') }
    finally { setRunning(false) }
  }

  return <div class="modal-backdrop" onClick={onClose}><section class="modal run-modal" role="dialog" aria-modal="true" aria-labelledby="run-title" onClick={e => e.stopPropagation()}>
    <div class="modal-top"><span class="modal-icon">▶</span><button class="icon-button" onClick={onClose} aria-label="Close">✕</button></div>
    <p class="eyebrow">INTERACTIVE TRIGGER</p><h2 id="run-title">Run a script</h2>
    <p class="modal-description">Choose a deployed script. Scripts with a user trigger provide their own form.</p>
    {!connection ? <p class="form-error">Connect to a control plane first.</p> : <>
      <label class="run-label">Script<select value={script} onChange={e => setScript(e.currentTarget.value)}><option value="">Select a script</option>{scripts.map(name => <option value={name} key={name}>{name}</option>)}</select></label>
      {scripts.length === 0 && <p class="modal-foot">No deployed scripts found.</p>}
      <label class="run-label">Version <span class="optional-label">optional · defaults to latest</span><input value={version} placeholder="latest" onInput={e => setVersion(e.currentTarget.value)} /></label>
      {loading && <p class="muted-message">Loading trigger form…</p>}
      {metadata && <form onSubmit={submit} class="trigger-form"><h3>{metadata.title}</h3>{metadata.description && <p class="modal-description">{metadata.description}</p>}
        {metadata.arguments.map(arg => arg.type === 'boolean' ? <label class="check-label" key={arg.name}><input type="checkbox" checked={values[arg.name] === true} onChange={e => setValues({ ...values, [arg.name]: e.currentTarget.checked })}/>{arg.label}</label> : <label key={arg.name}>{arg.label}{arg.required && <span class="required-mark"> *</span>}<input type={arg.type === 'number' ? 'number' : 'text'} step={arg.type === 'number' ? 'any' : undefined} required={arg.required} maxLength={arg.type === 'string' ? 4096 : undefined} value={String(values[arg.name] ?? '')} onInput={e => setValues({ ...values, [arg.name]: e.currentTarget.value })}/></label>)}
        <button class="primary-button" type="submit" disabled={running}>{running ? 'Running…' : 'Run script'}</button>
      </form>}
      {error && <p class="form-error" role="alert">{error}</p>}
      {result && <div class="run-result"><strong>Trigger returned</strong><pre>{JSON.stringify(result.value, null, 2)}</pre><p>Refresh flow history to inspect the run. Queued and durable work may still be running.</p></div>}
    </>}
  </section></div>
}
