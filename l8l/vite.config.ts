import { defineConfig, type Plugin } from 'vite'
import preact from '@preact/preset-vite'

// Development only: the Integraal query listener is unauthenticated and bound to loopback.
// Keep this adapter on loopback too; the production build contains no local proxy.
function localIntegraal(): Plugin {
  return {
    name: 'local-integraal',
    configureServer(server) {
      server.middlewares.use('/__local/scripts', async (req, res) => {
        const reply = (status: number, body: unknown) => {
          res.statusCode = status
          res.setHeader('content-type', 'application/json')
          res.setHeader('cache-control', 'no-store')
          res.end(JSON.stringify(body))
        }
        const origin = req.headers.origin
        if (origin && origin !== `http://${req.headers.host}`) return reply(403, { error: 'Cross-origin requests are not allowed' })
        const url = new URL(req.url ?? '/', 'http://localhost')
        const list = url.pathname === '/' || url.pathname === ''
        const match = /^\/([a-zA-Z0-9_.-]+)\/trigger$/.exec(url.pathname)
        if ((!list && !match) || (list && req.method !== 'GET') || (!list && req.method !== 'GET' && req.method !== 'POST')) return reply(404, { error: 'Not found' })
        const version = url.searchParams.get('version')
        if (version && !/^[a-zA-Z0-9_.-]+$/.test(version)) return reply(400, { error: 'Invalid version' })
        try {
          let body: string | undefined
          if (req.method === 'POST') {
            if (!req.headers['content-type']?.toLowerCase().startsWith('application/json')) return reply(415, { error: 'Expected JSON' })
            let size = 0
            const chunks: Buffer[] = []
            for await (const chunk of req) {
              const bytes = Buffer.from(chunk); size += bytes.length
              if (size > 65536) return reply(413, { error: 'Request body too large' })
              chunks.push(bytes)
            }
            body = Buffer.concat(chunks).toString('utf8')
          }
          const target = list ? '/scripts/acme' : `/triggers/acme/${match![1]}${version ? `?version=${encodeURIComponent(version)}` : ''}`
          const result = await fetch(`http://127.0.0.1:9091${target}`, {
            method: req.method, headers: body === undefined ? undefined : { 'content-type': 'application/json' }, body, signal: AbortSignal.timeout(30_000),
          })
          const data = await result.json()
          return reply(result.status, !result.ok || list || req.method === 'POST' ? data : { result: data })
        } catch (error) {
          console.error('Local Integraal trigger failed:', error)
          return reply(502, { error: 'Cannot reach local Integraal.' })
        }
      })
      server.middlewares.use('/__local/flows', async (req, res) => {
        const reply = (status: number, body: unknown) => {
          res.statusCode = status
          res.setHeader('content-type', 'application/json')
          res.setHeader('cache-control', 'no-store')
          res.end(JSON.stringify(body))
        }
        if (req.method !== 'GET') return reply(405, { error: 'Method not allowed' })
        const url = new URL(req.url ?? '/', 'http://localhost')
        const base = 'http://127.0.0.1:9091/flows/acme'
        try {
          async function get(path: string) {
            const result = await fetch(base + path, { signal: AbortSignal.timeout(10_000) })
            if (!result.ok) throw new Error(`Local Integraal returned HTTP ${result.status}`)
            return result.json()
          }
          if (url.pathname === '/' || url.pathname === '') {
            const day = url.searchParams.get('day') ?? ''
            if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return reply(400, { error: 'Invalid day' })
            return reply(200, { flows: await get(`?day=${encodeURIComponent(day)}`) })
          }
          const match = /^\/([a-fA-F0-9]{32})(\/logs)?$/.exec(url.pathname)
          if (!match) return reply(404, { error: 'Not found' })
          if (match[2]) return reply(200, { lines: await get(`/${match[1]}`) })
          const [invocations, state] = await Promise.all([get(`/${match[1]}/summary`), get(`/${match[1]}/state`)])
          return reply(200, { invocations, state })
        } catch (error) {
          console.error('Local Integraal request failed:', error)
          return reply(502, { error: 'Cannot reach local Integraal. Start the local server and check that flow logs are enabled.' })
        }
      })
    },
  }
}

export default defineConfig({ plugins: [preact(), localIntegraal()], server: { host: '127.0.0.1' } })
