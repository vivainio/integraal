import { defineConfig, type Plugin } from 'vite'
import preact from '@preact/preset-vite'

// Development only: the Integraal query listener is unauthenticated and bound to loopback.
// Keep this adapter on loopback too; the production build contains no local proxy.
function localIntegraal(): Plugin {
  return {
    name: 'local-integraal',
    configureServer(server) {
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
