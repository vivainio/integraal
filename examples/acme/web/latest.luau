--!strict
--!runtime 1
-- Read-only example. Configure { "web": { "script": "web" } } for this tenant.
type Response = { status: number?, headers: { [string]: string }?, body: string? }

local function json(status: number, value: any): Response
    return {
        status = status,
        headers = { ["content-type"] = "application/json; charset=utf-8" },
        body = ig.json.encode(value),
    }
end

local function on_http(ctx: Context, request: TenantWebRequest): Response
    if request.method ~= "GET" and request.method ~= "HEAD" then
        return { status = 405, headers = { Allow = "GET, HEAD" }, body = "Read-only app" }
    end
    if request.path == "/" then
        return {
            headers = { ["content-type"] = "text/html; charset=utf-8" },
            body = [[<!doctype html>
<html lang="en"><meta charset="utf-8"><title>Tenant data</title>
<style>body{font:16px system-ui;max-width:50rem;margin:3rem auto;padding:0 1rem}
input,button{font:inherit;padding:.5rem}pre{white-space:pre-wrap;background:#f4f4f4;padding:1rem}</style>
<h1>Tenant data</h1><p>Read a database partition. Results are limited to 25 records.</p>
<form id="query"><label>Partition <input name="pk" required></label> <button>Read</button></form>
<pre id="result">Enter a partition key.</pre>
<script>
const base = location.pathname.replace(/\/$/, '');
document.getElementById('query').addEventListener('submit', async event => {
  event.preventDefault();
  const output = document.getElementById('result');
  output.textContent = 'Loading…';
  try {
    const pk = new FormData(event.target).get('pk');
    const response = await fetch(base + '/records?pk=' + encodeURIComponent(pk));
    output.textContent = JSON.stringify(await response.json(), null, 2);
  } catch (error) { output.textContent = String(error); }
});
</script></html>]],
        }
    elseif request.path == "/records" then
        local pk = request.query.pk
        if not pk or pk == "" then
            return json(400, { error = "pk is required" })
        end
        if #pk > 200 or string.find(pk, "%c") then
            return json(400, { error = "invalid partition key" })
        end
        return json(200, { tenant = ctx.tenant, rows = ig.db.query(pk, { limit = 25 }) })
    end
    return json(404, { error = "not found" })
end

return { on_http = on_http }
