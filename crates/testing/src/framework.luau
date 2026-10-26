local cases = {}
local cache, loading = {}, {}
local function fresh()
    cache, loading = {}, {}
    __install_native()
    -- HTTP is always intercepted; a missing mock must never reach the network.
    local routes = {}
    test.http_calls = {}
    function test.http(method, url, response)
        table.insert(routes, {method = method, url = url, response = response})
    end
    native.http = {}
    function native.http.request(opts)
        local request = { method = opts.method or "GET", url = opts.url,
            headers = opts.headers or {}, body = opts.body }
        table.insert(test.http_calls, request)
        for _, route in ipairs(routes) do
            if route.method == request.method and route.url == request.url then
                if type(route.response) == "function" then return route.response(request) end
                return route.response
            end
        end
        error("unmocked HTTP request: " .. request.method .. " " .. tostring(request.url), 2)
    end
    function native.http.get(url, headers)
        return native.http.request({ url = url, headers = headers })
    end
end
function require(spec)
    if cache[spec] ~= nil then return cache[spec] end
    assert(not loading[spec], "circular require: " .. spec)
    loading[spec] = true
    local ok, value = pcall(function() return __load_module(spec)() end)
    loading[spec] = nil
    if not ok then error(value, 2) end
    if value == nil then value = true end
    cache[spec] = value
    return value
end
local function equal(a, b, seen)
    if type(a) ~= type(b) then return false end
    if type(a) ~= "table" then return a == b end
    seen = seen or {}
    if seen[a] == b then return true end
    seen[a] = b
    for k, v in pairs(a) do if not equal(v, b[k], seen) then return false end end
    for k in pairs(b) do if a[k] == nil then return false end end
    return true
end
test = {}
function test.case(name, fn)
    assert(type(name) == "string" and type(fn) == "function", "case needs a name and function")
    table.insert(cases, { name = name, fn = fn })
end
function test.equal(actual, expected)
    assert(equal(actual, expected), "expected " .. tostring(expected) .. ", got " .. tostring(actual))
end
function test.throws(fn, contains)
    local ok, err = pcall(fn)
    assert(not ok, "expected an error")
    if contains then assert(string.find(tostring(err), contains, 1, true), tostring(err)) end
end
function test.mock(path, implementation)
    assert(type(implementation) == "function", "mock needs a function")
    local parts = string.split(path, ".")
    assert(parts[1] == "native" and #parts > 1, "mock path must start with native.")
    local target = native
    for i = 2, #parts - 1 do
        local key = parts[i]
        assert(type(target[key]) == "table", "unknown mock path: " .. path)
        target = target[key]
    end
    local spy = { calls = {} }
    target[parts[#parts]] = function(...)
        table.insert(spy.calls, table.pack(...))
        return implementation(...)
    end
    return spy
end
function test.load(path) return __load_script(path)() end
function __run_case(index)
    cases[index].fn()
end
function __case_names()
    local names = {}
    for i, case in ipairs(cases) do names[i] = case.name end
    return names
end
fresh()
