//! Thin CLI for the server's loopback query API (`local_api`). Each command is one GET; the
//! response body is printed as it came. `INTEGRAAL_URL` overrides the default `http://127.0.0.1:9091`.
//!
//! integraal-client flows <tenant> [yyyy-mm-dd]
//! integraal-client flow <tenant> <trace id> [--since <ms>]
//! integraal-client summary <tenant> <trace id>
//! integraal-client state <tenant> <trace id>
//! integraal-client running

use std::process::ExitCode;

const USAGE: &str = "usage: integraal-client flows <tenant> [yyyy-mm-dd]\n       integraal-client flow <tenant> <trace id> [--since <ms>]\n       integraal-client summary <tenant> <trace id>\n       integraal-client state <tenant> <trace id>\n       integraal-client running";

fn path(args: &[String]) -> Option<String> {
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    Some(match a[..] {
        ["flows", t] => format!("/flows/{t}"),
        ["flows", t, day] => format!("/flows/{t}?day={day}"),
        ["flow", t, id] => format!("/flows/{t}/{id}"),
        ["flow", t, id, "--since", ms] => format!("/flows/{t}/{id}?since={ms}"),
        ["summary", t, id] => format!("/flows/{t}/{id}/summary"),
        ["state", t, id] => format!("/flows/{t}/{id}/state"),
        ["running"] => "/running".into(),
        _ => return None,
    })
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(path) = path(&args) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let base = std::env::var("INTEGRAAL_URL").unwrap_or_else(|_| "http://127.0.0.1:9091".into());
    match ureq::get(&format!("{}{path}", base.trim_end_matches('/'))).call() {
        Ok(r) => match r.into_string() {
            Ok(body) => {
                println!("{body}");
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        },
        Err(ureq::Error::Status(code, r)) => {
            eprintln!("{code}: {}", r.into_string().unwrap_or_default());
            ExitCode::FAILURE
        }
        Err(e) => fail(e),
    }
}

fn fail(e: impl std::fmt::Display) -> ExitCode {
    eprintln!("{e}");
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::path;

    fn p(s: &str) -> Option<String> {
        path(&s.split(' ').map(String::from).collect::<Vec<_>>())
    }

    #[test]
    fn commands_map_to_one_get() {
        assert_eq!(p("flows acme").unwrap(), "/flows/acme");
        assert_eq!(p("flows acme 2026-10-25").unwrap(), "/flows/acme?day=2026-10-25");
        assert_eq!(p("flow acme abc --since 5").unwrap(), "/flows/acme/abc?since=5");
        assert_eq!(p("summary acme abc").unwrap(), "/flows/acme/abc/summary");
        assert_eq!(p("state acme abc").unwrap(), "/flows/acme/abc/state");
        assert_eq!(p("running").unwrap(), "/running");
        assert!(p("flow acme").is_none());
    }
}
