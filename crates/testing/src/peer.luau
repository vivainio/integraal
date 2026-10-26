-- The peer reads requests and inputs, then commits its answer with done.json last.
local get, put = __peer_get, __peer_put
local json = native.json
local function digest(body)
    return native.crypto.hex_encode(native.crypto.sha256(body))
end
local function request(job)
    local body = get(job, "request.json")
    assert(body ~= nil, "peer request not found for job " .. job)
    local value = json.decode(body)
    assert(value.v == 1 and value.job == job and type(value.queries) == "table",
        "invalid peer batch request")
    return value, body
end
local received = setmetatable({}, { __mode = "k" })
test.peer = {}
function test.peer.receive(job, input, part)
    local req, body = request(job)
    local key = "request.json"
    if input ~= nil then
        local spec = req.inputs and req.inputs[input]
        assert(spec ~= nil, "peer input not found: " .. input)
        part = part or 1
        assert(type(part) == "number" and part >= 1 and part % 1 == 0, "invalid peer input part")
        local info = spec.parts[part]
        assert(info ~= nil, "peer input part not found")
        key = info.key
        body = get(job, key)
        assert(body ~= nil, "peer input object not found: " .. key)
        if info.bytes then assert(#body == info.bytes, "peer input size mismatch") end
        if info.sha256 then assert(digest(body) == info.sha256, "peer input sha256 mismatch") end
    else
        assert(part == nil, "peer input name required with part")
    end
    local document = { job = job, key = key, body = body, request = req }
    received[document] = job
    return document
end
function test.peer.ack(document, options)
    local job = received[document]
    assert(job ~= nil, "peer.ack expects a document returned by peer.receive")
    assert(get(job, "done.json") == nil, "peer job already acknowledged")
    -- Reload the actual request so edits to document.request cannot change the protocol.
    local req = request(job)
    options = options or {}
    assert(options.ok == nil or type(options.ok) == "boolean", "peer ack ok must be boolean")
    local done, files = { ok = options.ok ~= false }, {}
    if not done.ok then
        assert(type(options.error) == "string" and #options.error > 0, "failed peer ack needs an error")
        assert(options.results == nil, "failed peer ack cannot include results")
        done.error = options.error
    else
        assert(options.error == nil, "successful peer ack cannot include an error")
        local expected = {}
        done.results = {}
        for _, query in ipairs(req.queries) do
            local name = query.name
            expected[name] = true
            local value = options.results and options.results[name] or nil
            assert(options.results == nil or value ~= nil, "peer result missing: " .. name)
            value = value or {}
            assert(type(value) == "table", "peer result must be rows or { body = string }")
            local body, rows
            if value.body ~= nil then
                assert(type(value.body) == "string", "peer result body must be a string")
                body = value.body
            else
                local lines = {}
                local count = 0
                for k in pairs(value) do
                    count += 1
                    assert(type(k) == "number" and k >= 1 and k % 1 == 0 and k <= #value,
                        "peer result rows must be an array")
                end
                assert(count == #value, "peer result rows must be a dense array")
                for _, row in ipairs(value) do table.insert(lines, json.encode(row) .. "\n") end
                rows = #lines
                body = table.concat(lines)
            end
            local key = "peer-" .. name .. ".0000.data"
            table.insert(files, { key = key, body = body })
            done.results[name] = { rows = rows,
                parts = { { key = key, bytes = #body, sha256 = digest(body) } } }
        end
        if options.results then
            for name in pairs(options.results) do assert(expected[name], "unknown peer result: " .. tostring(name)) end
        end
    end
    -- Serialize and validate the entire response before writing any objects.
    local body = json.encode(done)
    for _, file in ipairs(files) do put(job, file.key, file.body) end
    put(job, "done.json", body)
    return { type = "s3.object", store = "$batch", key = "jobs/" .. job .. "/done.json",
        size = #body, event = "ObjectCreated:Put" }
end
