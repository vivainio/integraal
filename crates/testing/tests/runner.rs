//! Exercise the real executable, including sandbox setup and process exit status.
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicUsize, Ordering},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "integraal-runner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let f = Self(root);
        f.write("tenant/config.json", "{}");
        f.write("deployment.json", r#"{"scripts":{"bucket":"unused"}}"#);
        fs::create_dir_all(f.0.join("tenant/tests")).unwrap();
        f
    }
    fn write(&self, path: &str, body: &str) {
        let path = self.0.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, body).unwrap();
    }
    fn command(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_integraal-test"));
        cmd.arg(self.0.join("tenant"))
            .arg(self.0.join("deployment.json"))
            .env("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
            .env("AWS_EC2_METADATA_DISABLED", "true");
        cmd
    }
    fn run(&self, code: i32, expected: &[&str]) -> Output {
        let out = self.command().output().unwrap();
        let text = format!(
            "{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(code), "{text}");
        for fragment in expected {
            assert!(text.contains(fragment), "missing {fragment:?}: {text}");
        }
        out
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn discovers_nested_suites_runs_each_case_and_continues_after_failure() {
    let f = Fixture::new();
    f.write(
        "tenant/tests/a.test.luau",
        r#"
        test.case("fails", function() error("intentional failure") end)
        test.case("after failure", function() end)
    "#,
    );
    f.write(
        "tenant/tests/nested/b.test.luau",
        "test.case('nested', function() end)",
    );
    f.write("tenant/tests/ignored.luau", "error('must not execute')");
    let out = f.run(
        1,
        &[
            "FAIL",
            "intentional failure",
            "PASS",
            "after failure",
            "nested",
            "2 passed, 1 failed",
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.find("after failure").unwrap() < stdout.find("nested").unwrap());
}

#[test]
fn fresh_vms_isolate_globals_mocks_routes_and_module_cache() {
    let f = Fixture::new();
    f.write("tenant/lib/state.luau", "return { value = 0 }");
    f.write(
        "tenant/tests/isolation.test.luau",
        r#"
        local state = require("@lib/state")
        assert(state.value == 0)
        local function clean()
            assert(leaked == nil and state.value == 0)
            test.equal(#test.http_calls, 0)
            test.throws(function() ig.http.get("https://example.test") end, "unmocked HTTP")
            assert(ig.time.now_ms() ~= -1)
        end
        test.case("mutates", function()
            clean()
            leaked = true
            state.value = 99
            test.mock("ig.time.now_ms", function() return -1 end)
            test.http("GET", "https://example.test", {status=200})
            assert(ig.http.get("https://example.test").status == 200)
        end)
        test.case("starts clean", clean)
    "#,
    );
    f.run(0, &["2 passed, 0 failed"]);
}

#[test]
fn loads_real_scripts_native_imports_and_cached_local_and_shared_modules() {
    let f = Fixture::new();
    f.write("tenant/lib/util.luau", "return { value = 42 }");
    f.write("tenant/@shared/value/1.0.0.luau", "return 17");
    f.write(
        "tenant/handler/latest.luau",
        r#"--!runtime 1
        local util = require("@lib/util")
        local shared = require("@shared/value@1.0.0")
        local patterns = require("@native/patterns@1")
        return { run = function()
            assert(type(patterns.batch.new) == "function")
            assert(util == require("@lib/util"))
            return ig.json.decode('{"value":42}').value + shared
        end }
    "#,
    );
    f.write(
        "tenant/tests/load.test.luau",
        r#"
        test.case("loads", function()
            test.equal(test.load("handler/latest.luau").run(), 59)
            test.throws(function() require("@shared/value") end, "needs a version")
            test.throws(function() require("unsupported") end, "expected @lib/")
            test.throws(function() test.load("missing.luau") end)
        end)
    "#,
    );
    f.run(0, &["1 passed, 0 failed"]);
}

#[test]
fn rejects_path_traversal_absolute_paths_symlinks_and_bad_runtime_versions() {
    let f = Fixture::new();
    f.write("outside.luau", "error('escaped root')");
    f.write("tenant/future.luau", "--!runtime 2\nreturn {}");
    f.write("tenant/malformed.luau", "--!runtime nope\nreturn {}");
    let outside = f.0.join("outside.luau");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, f.0.join("tenant/link.luau")).unwrap();
    f.write(
        "tenant/tests/paths.test.luau",
        &format!(
            r#"
        test.case("paths", function()
            test.throws(function() test.load("../outside.luau") end, "escapes root")
            test.throws(function() test.load({absolute}) end, "escapes root")
            test.throws(function() require("@lib/../../outside") end, "escapes root")
            test.throws(function() test.load("future.luau") end, "unsupported runtime version 2")
            test.throws(function() test.load("malformed.luau") end)
            {symlink}
        end)
    "#,
            absolute = serde_json::to_string(&outside.to_string_lossy()).unwrap(),
            symlink = if cfg!(unix) {
                r#"test.throws(function() test.load("link.luau") end, "escapes root")"#
            } else {
                ""
            }
        ),
    );
    // The module path must exist before canonicalization can reject its escape.
    fs::create_dir_all(f.0.join("tenant/lib")).unwrap();
    f.run(0, &["1 passed, 0 failed"]);
}

#[test]
fn setup_errors_and_empty_suites_exit_two() {
    let f = Fixture::new();
    f.run(2, &["no test cases found"]);
    f.write("tenant/tests/empty.test.luau", "local x = 1");
    f.run(2, &["no test cases found"]);
    f.write("tenant/tests/empty.test.luau", "this is invalid luau !!!");
    f.run(2, &["syntax error"]);
    f.write(
        "tenant/tests/empty.test.luau",
        "error('registration failed')",
    );
    f.run(2, &["registration failed"]);
    f.write("tenant/config.json", "invalid json");
    f.run(2, &["expected value"]);
}

#[test]
fn requires_arguments_and_explicit_local_endpoint() {
    let out = Command::new(env!("CARGO_BIN_EXE_integraal-test"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage:"));
    let f = Fixture::new();
    for endpoint in [
        None,
        Some("https://example.com"),
        Some("http://example.com:5070"),
    ] {
        let mut cmd = f.command();
        cmd.env_remove("AWS_ENDPOINT_URL");
        if let Some(endpoint) = endpoint {
            cmd.env("AWS_ENDPOINT_URL", endpoint);
        }
        let out = cmd.output().unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&out.stderr).contains("local roto endpoint"));
    }
}

#[test]
fn memory_exhaustion_fails_case_and_runner_continues() {
    let f = Fixture::new();
    f.write(
        "tenant/tests/memory.test.luau",
        r#"
        test.case("memory", function()
            local data = {}
            while true do table.insert(data, tostring(#data) .. string.rep("x", 1024 * 1024)) end
        end)
        test.case("survives", function() end)
    "#,
    );
    f.run(
        1,
        &[
            "memory",
            "not enough memory",
            "survives",
            "1 passed, 1 failed",
        ],
    );
}

#[test]
fn infinite_loop_times_out_and_runner_continues() {
    let f = Fixture::new();
    f.write(
        "tenant/tests/timeout.test.luau",
        r#"
        test.case("timeout", function() while true do end end)
        test.case("survives", function() end)
    "#,
    );
    f.run(1, &["test timeout", "survives", "1 passed, 1 failed"]);
}
