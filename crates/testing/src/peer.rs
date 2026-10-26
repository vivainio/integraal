//! Test-side access to the peer's portion of the tenant's batch bucket.
use integraal_wrappers::StoreScope;
use mlua::Lua;

fn key(job: &str, relative: &str) -> mlua::Result<String> {
    let token = !job.is_empty()
        && job.len() <= 64
        && job
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_');
    let part = !relative.is_empty()
        && relative.len() <= 256
        && !relative.starts_with('/')
        && !relative.chars().any(|c| c.is_control() || c == '\\')
        && relative
            .split('/')
            .all(|s| !s.is_empty() && s != "." && s != "..");
    if !token || !part {
        return Err(mlua::Error::runtime("invalid peer job or part key"));
    }
    Ok(format!("jobs/{job}/{relative}"))
}

pub fn install(lua: &Lua, scope: Option<StoreScope>) -> mlua::Result<()> {
    let read = scope.clone();
    lua.globals().set(
        "__peer_get",
        lua.create_function(move |lua, (job, relative): (String, String)| {
            let key = key(&job, &relative)?;
            let scope = read.as_ref().ok_or_else(|| {
                mlua::Error::runtime(
                    "test.peer requires batch configuration and tenant batch access",
                )
            })?;
            scope
                .get(&key)
                .map_err(mlua::Error::runtime)?
                .map(|body| lua.create_string(body))
                .transpose()
        })?,
    )?;
    lua.globals().set(
        "__peer_put",
        lua.create_function(
            move |_, (job, relative, body): (String, String, mlua::LuaString)| {
                let key = key(&job, &relative)?;
                let scope = scope.as_ref().ok_or_else(|| {
                    mlua::Error::runtime(
                        "test.peer requires batch configuration and tenant batch access",
                    )
                })?;
                scope
                    .put(&key, body.as_bytes().to_vec())
                    .map_err(mlua::Error::runtime)
            },
        )?,
    )?;
    lua.load(include_str!("peer.luau"))
        .set_name("=integraal-test-peer")
        .exec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use integraal_wrappers::{Deadline, HostContext, HostLimits};
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    };

    struct Fixture {
        lua: Lua,
        dir: PathBuf,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    fn fixture() -> Fixture {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "integraal-peer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let scope = StoreScope::new(
            Arc::new(integraal_core::debug::FsBackend::new(&dir)),
            "tenant/",
        );
        let host = HostContext {
            tenant: "tenant".into(),
            script: Arc::new(Mutex::new("test".into())),
            stores: Default::default(),
            batch: Some(scope.clone()),
            db: None,
            queue: None,
            callback_base_url: None,
            throttle: None,
            background_http: false,
            http: Default::default(),
            vault: None,
            master_keys: None,
            vars: Default::default(),
            flow_log: None,
            summary: None,
            limits: HostLimits {
                deadline: Deadline::new(Instant::now() + Duration::from_secs(10)),
                max_http_response_bytes: 5_000_000,
                max_object_bytes: 5_000_000,
                max_stream_bytes: 64_000_000,
                max_sql_bytes: 64_000_000,
                max_http_requests: 100,
                max_data_ops: 1000,
                max_http_timeout: Duration::from_secs(10),
            },
        };
        let lua = Lua::new();
        integraal_wrappers::install(&lua, 1, &host).unwrap();
        lua.load("function __install_native() end").exec().unwrap();
        lua.load(include_str!("framework.luau")).exec().unwrap();
        install(&lua, Some(scope)).unwrap();
        Fixture { lua, dir }
    }

    #[test]
    fn uploaded_document_ack_routes_and_hydrates_real_results() {
        let f = fixture();
        f.lua
            .load(
                r#"
            local job = native.batch.new({ id = "document" })
            job:input("invoice", "<invoice id='42'/>", { ext = ".xml" })
            job:query("receipt", "accept_invoice", { input = "invoice" })
            job:state({ invoice = "42" })
            job:continue_with("received")
            local id = job:submit()
            local document = test.peer.receive(id, "invoice")
            assert(document.body == "<invoice id='42'/>")
            assert(document.request.queries[1].params.input == "invoice")
            -- Acknowledgment follows the stored request, even if the inspected copy was edited.
            document.request.queries = {}
            local event = test.peer.ack(document, { results = { receipt = {{ accepted = true }} } })
            assert(event.type == "s3.object" and event.store == "$batch")
            local route = native.batch.route(event)
            assert(route.script == "received" and route.event.job == id)
            local result = native.batch.hydrate(route.event)
            assert(result.ok and result.state.invoice == "42")
            local rows = {}
            for row in result:rows("receipt") do table.insert(rows, row) end
            test.equal(rows, {{ accepted = true }})
            test.throws(function() test.peer.ack(document) end, "already acknowledged")
        "#,
            )
            .exec()
            .unwrap();
        let done: serde_json::Value = serde_json::from_slice(
            &std::fs::read(f.dir.join("tenant/jobs/document/done.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(done["results"]["receipt"]["rows"], 1);
        assert!(
            done["results"]["receipt"]["parts"][0]["sha256"]
                .as_str()
                .unwrap()
                .len()
                == 64
        );
    }

    #[test]
    fn default_ack_failure_and_raw_documents_follow_batch_contract() {
        fixture()
            .lua
            .load(
                r#"
            for _, id in ipairs({"empty", "failure", "raw"}) do
                local job = native.batch.new({id=id})
                job:query("receipt", "upload")
                job:continue_with("received")
                job:submit()
            end
            local empty = test.peer.receive("empty")
            assert(native.json.decode(empty.body).job == "empty")
            local event = test.peer.ack(empty)
            local result = native.batch.hydrate(event)
            for _ in result:rows("receipt") do error("expected empty rows") end
            event = test.peer.ack(test.peer.receive("failure"), {ok=false, error="rejected"})
            test.throws(function() native.batch.hydrate(event) end, "rejected")
            result = native.batch.hydrate(event, {allow_failure=true})
            assert(not result.ok and result.error == "rejected")
            event = test.peer.ack(test.peer.receive("raw"), {results={receipt={body="<receipt/>"}}})
            local stream = native.batch.hydrate(event):open("receipt")
            assert(native.xml.parse(stream):first("/receipt") ~= nil)
            stream:close()
        "#,
            )
            .exec()
            .unwrap();
    }

    #[test]
    fn invalid_ack_does_not_commit_and_input_integrity_is_checked() {
        let f = fixture();
        f.lua.load(r#"
            local job = native.batch.new({id="validation"})
            job:input("invoice", "original")
            job:query("receipt", "upload")
            job:submit()
            test.throws(function() test.peer.receive("absent") end, "not found")
            test.throws(function() test.peer.receive("validation", "absent") end, "input not found")
            test.throws(function() test.peer.receive("validation", "invoice", 2) end, "part not found")
            local document = test.peer.receive("validation", "invoice")
            test.throws(function() test.peer.ack(document, {results={}}) end, "result missing")
            test.throws(function() test.peer.ack(document, {results={receipt={}, extra={}}}) end, "unknown peer result")
            test.throws(function() test.peer.ack(document, {ok=false}) end, "needs an error")
            test.throws(function() test.peer.ack({job="validation"}) end, "expects a document")
        "#).exec().unwrap();
        let job_dir = f.dir.join("tenant/jobs/validation");
        assert!(!job_dir.join("done.json").exists());
        assert!(!job_dir.join("peer-receipt.0000.data").exists());
        std::fs::write(job_dir.join("inputs/invoice.0000.ndjson"), "tampered").unwrap();
        f.lua.load(r#"test.throws(function() test.peer.receive("validation", "invoice") end, "sha256 mismatch")"#).exec().unwrap();
    }

    #[test]
    fn peer_keys_cannot_escape_the_job_or_access_state() {
        for (job, part) in [
            ("../other", "request.json"),
            ("ok", "../../state/ok.json"),
            ("ok", "/request.json"),
            ("ok", "a\\b"),
            ("ok", "a//b"),
        ] {
            assert!(key(job, part).is_err());
        }
        assert_eq!(
            key("ok", "inputs/doc.0000.xml").unwrap(),
            "jobs/ok/inputs/doc.0000.xml"
        );
        let lua = Lua::new();
        lua.load("test = {}; native = {json = {}} ").exec().unwrap();
        install(&lua, None).unwrap();
        let error = lua.load(r#"test.peer.receive("job")"#).exec().unwrap_err();
        assert!(error.to_string().contains("requires batch configuration"));
    }
}
