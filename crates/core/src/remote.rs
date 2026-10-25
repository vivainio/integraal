//! FTP, FTPS and SFTP object stores, driven by the `curl` binary.
//!
//! Shelling out keeps these protocols (and the SSH stack SFTP needs) out of our binary; the host
//! must have `curl` installed, built with the protocols the config uses. Every operation is one
//! `curl` process: `open` streams its stdout, `create` streams into its stdin, and the object only
//! appears when the upload is renamed from its temporary name on `commit`.
//!
//! Credentials never reach the command line (where `ps` would show them): each process gets a
//! private `-K` config file that is removed when it exits.

use std::{
    io::{Read, Write},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use integraal_wrappers::{ObjectBackend, ObjectWriter};

/// Most bytes `get` will buffer; `open` has no such cap.
const GET_CAP: u64 = 64 * 1024 * 1024;
/// Marks uploads in flight, so `list` can hide them.
const TMP_MARK: &str = ".integraal-tmp-";
/// `curl` exit code for a missing remote file.
const CURL_REMOTE_FILE_NOT_FOUND: i32 = 78;
/// `curl` exit code for a failed `-Q` command.
const CURL_QUOTE_ERROR: i32 = 21;
const MAX_LIST_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Ftp,
    Ftps,
    Sftp,
}

impl Protocol {
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Ftp => "ftp",
            Self::Ftps => "ftps",
            Self::Sftp => "sftp",
        }
    }
}

/// How to reach one remote store. Built from a `buckets.stores` entry.
#[derive(Debug, Clone)]
pub struct RemoteConfig {
    pub protocol: Protocol,
    /// `scheme://host[:port]`, no path.
    pub origin: String,
    /// The directory all keys are relative to, as written in the config: starts and ends with `/`.
    pub dir: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub key_file: Option<String>,
    pub host_key_sha256: Option<String>,
    /// Plain `ftp` only: upgrade the connection with explicit TLS and refuse to continue without it.
    pub require_tls: bool,
    pub timeout: Duration,
    pub curl: String,
}

impl RemoteConfig {
    /// `scheme://host[:port]`, checking the host is a plain name or address.
    pub fn origin(protocol: Protocol, host: &str, port: Option<u16>) -> Result<String, String> {
        let ok = !host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']'));
        if !ok {
            return Err(format!("host {host:?} is not a host name or address"));
        }
        Ok(match port {
            Some(p) => format!("{}://{host}:{p}", protocol.scheme()),
            None => format!("{}://{host}", protocol.scheme()),
        })
    }

    /// Checks `path` names a directory: starts and ends with `/`, with no `.` or `..` segments
    /// and no control characters. It is used as written, not as a URL.
    pub fn check_dir(path: &str) -> Result<(), String> {
        if !path.starts_with('/')
            || !path.ends_with('/')
            || path.chars().any(char::is_control)
            || path.split('/').any(|c| c == "." || c == "..")
        {
            return Err(format!(
                "path {path:?} must start and end with '/' and not contain '.' or '..'"
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct CurlBackend {
    cfg: RemoteConfig,
}

impl CurlBackend {
    pub fn new(cfg: RemoteConfig) -> Self {
        Self { cfg }
    }

    /// Fails unless `curl` runs and lists the protocol (SFTP needs a build with libssh).
    pub fn check_curl(curl: &str, protocol: Protocol) -> Result<(), String> {
        let out = Command::new(curl)
            .arg("-V")
            .output()
            .map_err(|e| format!("cannot run {curl:?}: {e} (remote stores need curl installed)"))?;
        let text = String::from_utf8_lossy(&out.stdout).to_lowercase();
        let protocols = text
            .lines()
            .find(|l| l.starts_with("protocols:"))
            .unwrap_or_default();
        if protocols.split_whitespace().any(|p| p == protocol.scheme()) {
            Ok(())
        } else {
            Err(format!(
                "{curl:?} was built without {} support",
                protocol.scheme()
            ))
        }
    }

    fn url(&self, key: &str) -> Result<String, String> {
        check_key(key)?;
        Ok(format!(
            "{}{}{}",
            self.cfg.origin,
            percent_encode(&self.cfg.dir),
            percent_encode(key)
        ))
    }

    fn dir_url(&self, key_dir: &str) -> String {
        format!(
            "{}{}{}",
            self.cfg.origin,
            percent_encode(&self.cfg.dir),
            percent_encode(key_dir)
        )
    }

    /// The path of `key` as `-Q` commands name it. FTP paths are relative to the login directory
    /// (the URL's leading `/` is not part of them); SFTP ones are absolute.
    fn quote_path(&self, key: &str) -> String {
        let full = format!("{}{key}", self.cfg.dir);
        match self.cfg.protocol {
            Protocol::Sftp => format!("\"{}\"", full.replace('\\', "\\\\").replace('"', "\\\"")),
            _ => full.strip_prefix('/').unwrap_or(&full).to_string(),
        }
    }

    fn delete_command(&self, key: &str) -> String {
        match self.cfg.protocol {
            Protocol::Sftp => format!("rm {}", self.quote_path(key)),
            _ => format!("DELE {}", self.quote_path(key)),
        }
    }

    fn rename_command(&self, from: &str, to: &str) -> Vec<String> {
        match self.cfg.protocol {
            Protocol::Sftp => vec![format!(
                "-rename {} {}",
                self.quote_path(from),
                self.quote_path(to)
            )],
            // The post-transfer commands run in the directory `curl` changed into for the upload,
            // and both names live there.
            _ => vec![
                format!("-RNFR {}", from.rsplit('/').next().unwrap_or(from)),
                format!("-RNTO {}", to.rsplit('/').next().unwrap_or(to)),
            ],
        }
    }

    /// Writes the private config for one process and returns it with the other arguments.
    fn command(&self, url: &str, extra: &[String]) -> Result<(Command, ConfigFile), String> {
        let mut lines = vec![format!("url = {}", cfg_quote(url))];
        if let Some(u) = &self.cfg.user {
            let cred = match &self.cfg.password {
                Some(p) => format!("{u}:{p}"),
                None => u.clone(),
            };
            lines.push(format!("user = {}", cfg_quote(&cred)));
        }
        if let Some(k) = &self.cfg.key_file {
            lines.push(format!("key = {}", cfg_quote(k)));
        }
        if let Some(h) = &self.cfg.host_key_sha256 {
            lines.push(format!("hostpubsha256 = {}", cfg_quote(h)));
        }
        if self.cfg.require_tls && self.cfg.protocol == Protocol::Ftp {
            lines.push("ssl-reqd".into());
        }
        for e in extra {
            lines.push(e.clone());
        }
        let file = ConfigFile::create(&lines.join("\n"))?;
        let mut cmd = Command::new(&self.cfg.curl);
        cmd.args(["-sS", "--proto-redir", "=none", "--connect-timeout", "15"])
            .arg("--proto")
            .arg(format!("={}", self.cfg.protocol.scheme()))
            .arg("--max-time")
            .arg(self.cfg.timeout.as_secs().max(1).to_string())
            .arg("-K")
            .arg(file.path())
            .stderr(Stdio::piped());
        Ok((cmd, file))
    }

    fn spawn(&self, url: &str, extra: &[String], stdin: bool) -> Result<Proc, String> {
        let (mut cmd, file) = self.command(url, extra)?;
        cmd.stdout(Stdio::piped())
            .stdin(if stdin { Stdio::piped() } else { Stdio::null() });
        let child = cmd
            .spawn()
            .map_err(|e| format!("cannot run {:?}: {e}", self.cfg.curl))?;
        Ok(Proc {
            child: Some(child),
            _file: file,
        })
    }

    fn list_dir(&self, key_dir: &str) -> Result<Vec<(String, bool)>, String> {
        let mut p = self.spawn(&self.dir_url(key_dir), &[], false)?;
        let mut text = String::new();
        p.stdout()
            .read_to_string(&mut text)
            .map_err(|e| format!("list: {e}"))?;
        match p.finish() {
            Ok(()) => Ok(parse_listing(&text)),
            Err(f) if f.code == CURL_REMOTE_FILE_NOT_FOUND || f.not_found() => Ok(Vec::new()),
            Err(f) => Err(f.message("list")),
        }
    }

    fn walk(
        &self,
        dir: &str,
        prefix: &str,
        limit: usize,
        depth: usize,
        out: &mut Vec<String>,
    ) -> Result<(), String> {
        for (name, is_dir) in self.list_dir(dir)? {
            if out.len() >= limit {
                return Ok(());
            }
            let key = format!("{dir}{name}");
            if is_dir {
                let sub = format!("{key}/");
                // Only descend where the prefix can still match.
                if depth < MAX_LIST_DEPTH && (prefix.starts_with(&sub) || sub.starts_with(prefix)) {
                    self.walk(&sub, prefix, limit, depth + 1, out)?;
                }
            } else if key.starts_with(prefix) && !name.contains(TMP_MARK) {
                out.push(key);
            }
        }
        Ok(())
    }
}

impl ObjectBackend for CurlBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let Some(mut r) = self.open(key)? else {
            return Ok(None);
        };
        let mut body = Vec::new();
        (&mut r)
            .take(GET_CAP + 1)
            .read_to_end(&mut body)
            .map_err(|e| format!("{key}: {e}"))?;
        if body.len() as u64 > GET_CAP {
            return Err(format!("{key}: too large for get; use open"));
        }
        Ok(Some(body))
    }

    fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        let mut w = self.create(key)?;
        w.write(&body)?;
        w.commit()
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        // `curl` needs a transfer for `-Q` to run, so it lists the base directory and discards it.
        check_key(key)?;
        let quote = format!("quote = {}", cfg_quote(&self.delete_command(key)));
        let mut p = self.spawn(&self.dir_url(""), &[quote], false)?;
        let _ = std::io::copy(&mut p.stdout(), &mut std::io::sink());
        match p.finish() {
            Ok(()) => Ok(()),
            Err(f) if f.code == CURL_QUOTE_ERROR && f.not_found() => Ok(()),
            Err(f) => Err(f.message(key)),
        }
    }

    fn open(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>, String> {
        let mut p = self.spawn(&self.url(key)?, &[], false)?;
        // Read ahead: an empty first read is either an empty file or a failed transfer.
        let mut first = vec![0u8; 8192];
        let n = read_some(&mut p.stdout(), &mut first).map_err(|e| format!("{key}: {e}"))?;
        first.truncate(n);
        if n == 0 {
            return match p.finish() {
                Ok(()) => Ok(Some(Box::new(std::io::empty()))),
                Err(f) if f.code == CURL_REMOTE_FILE_NOT_FOUND || f.not_found() => Ok(None),
                Err(f) => Err(f.message(key)),
            };
        }
        Ok(Some(Box::new(Download {
            first: std::io::Cursor::new(first),
            proc: Some(p),
            key: key.to_string(),
            done: false,
        })))
    }

    fn create(&self, key: &str) -> Result<Box<dyn ObjectWriter>, String> {
        check_key(key)?;
        let (dir, name) = key.rsplit_once('/').map_or(("", key), |(d, n)| (d, n));
        let tmp_name = format!("{name}{TMP_MARK}{}", unique());
        let tmp_key = if dir.is_empty() {
            tmp_name
        } else {
            format!("{dir}/{tmp_name}")
        };
        let mut extra = vec![
            "upload-file = \"-\"".to_string(),
            "ftp-create-dirs".to_string(),
        ];
        for q in self.rename_command(&tmp_key, key) {
            extra.push(format!("quote = {}", cfg_quote(&q)));
        }
        let mut proc = self.spawn(&self.url(&tmp_key)?, &extra, true)?;
        let stdin = proc.child().stdin.take();
        Ok(Box::new(Upload {
            stdin,
            proc: Some(proc),
            key: key.to_string(),
            tmp_key,
            backend: self.clone(),
            committed: false,
        }))
    }

    fn list(&self, prefix: &str, limit: usize) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        // Start in the deepest directory the prefix names.
        let start = prefix.rfind('/').map_or("", |i| &prefix[..=i]);
        if !start.is_empty() {
            check_key(start.trim_end_matches('/'))?;
        }
        self.walk(start, prefix, limit, 0, &mut out)?;
        out.sort();
        out.truncate(limit);
        Ok(out)
    }
}

/// One running `curl`, killed if dropped before `finish`.
struct Proc {
    child: Option<Child>,
    _file: ConfigFile,
}

impl Proc {
    fn child(&mut self) -> &mut Child {
        self.child.as_mut().expect("process already finished")
    }

    fn stdout(&mut self) -> &mut std::process::ChildStdout {
        self.child().stdout.as_mut().expect("stdout is piped")
    }

    /// Waits for `curl` to exit; it must already have closed (or been drained of) its stdout.
    fn finish(mut self) -> Result<(), Failure> {
        let mut child = self.child.take().expect("process already finished");
        drop(child.stdin.take());
        let mut stderr = String::new();
        if let Some(mut e) = child.stderr.take() {
            let _ = e.read_to_string(&mut stderr);
        }
        let status = child.wait().map_err(|e| Failure {
            code: -1,
            stderr: e.to_string(),
        })?;
        if status.success() {
            Ok(())
        } else {
            Err(Failure {
                code: status.code().unwrap_or(-1),
                stderr,
            })
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

#[derive(Debug)]
struct Failure {
    code: i32,
    stderr: String,
}

impl Failure {
    /// Whether the server said the path does not exist.
    fn not_found(&self) -> bool {
        let e = &self.stderr;
        e.contains("550")
            || e.contains("No such file")
            || e.contains("not found")
            || e.contains("change to the given directory")
    }

    fn message(&self, what: &str) -> String {
        format!("{what}: curl exit {}: {}", self.code, self.stderr.trim())
    }
}

/// A download in flight: the bytes read ahead, then the rest of `curl`'s stdout. A `curl` that
/// fails partway surfaces as a read error rather than a silently short object.
struct Download {
    first: std::io::Cursor<Vec<u8>>,
    proc: Option<Proc>,
    key: String,
    done: bool,
}

impl Read for Download {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.first.read(buf)?;
        if n > 0 || self.done {
            return Ok(n);
        }
        let Some(proc) = self.proc.as_mut() else {
            return Ok(0);
        };
        let n = proc.stdout().read(buf)?;
        if n == 0 {
            self.done = true;
            if let Some(p) = self.proc.take() {
                p.finish()
                    .map_err(|f| std::io::Error::other(f.message(&self.key)))?;
            }
        }
        Ok(n)
    }
}

/// An upload to a temporary name; `commit` waits for the transfer, whose `-Q` rename then
/// publishes the object under its real name.
struct Upload {
    stdin: Option<std::process::ChildStdin>,
    proc: Option<Proc>,
    key: String,
    tmp_key: String,
    backend: CurlBackend,
    committed: bool,
}

impl ObjectWriter for Upload {
    fn write(&mut self, data: &[u8]) -> Result<(), String> {
        let stdin = self.stdin.as_mut().ok_or("writer is closed")?;
        match stdin.write_all(data) {
            Ok(()) => Ok(()),
            Err(e) => {
                // A broken pipe means `curl` already exited; its stderr says why.
                self.stdin = None;
                match self.proc.take().map(Proc::finish) {
                    Some(Err(f)) => Err(f.message(&self.key)),
                    _ => Err(format!("{}: {e}", self.key)),
                }
            }
        }
    }

    fn commit(mut self: Box<Self>) -> Result<(), String> {
        drop(self.stdin.take());
        let proc = self.proc.take().ok_or("upload already failed")?;
        proc.finish().map_err(|f| f.message(&self.key))?;
        self.committed = true;
        Ok(())
    }

    fn abort(self: Box<Self>) {}
}

impl Drop for Upload {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        drop(self.stdin.take());
        drop(self.proc.take()); // kills `curl`
        let _ = self.backend.delete(&self.tmp_key); // a killed STOR leaves a partial file
    }
}

/// A `-K` file holding credentials, readable only by us, removed when dropped.
struct ConfigFile(std::path::PathBuf);

impl ConfigFile {
    fn create(contents: &str) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!("integraal-curl-{}", unique()));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts
            .open(&path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let file = Self(path);
        f.write_all(contents.as_bytes())
            .map_err(|e| format!("{}: {e}", file.0.display()))?;
        Ok(file)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for ConfigFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn unique() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        "{}-{}-{nanos:x}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

fn read_some(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    loop {
        match r.read(buf) {
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            other => return other,
        }
    }
}

/// Same rules as the other backends' keys: relative, no `.`/`..`, no backslashes or control characters.
fn check_key(key: &str) -> Result<(), String> {
    if key.is_empty()
        || key
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == ".." || c.contains('\\'))
        || key.chars().any(char::is_control)
    {
        return Err(format!("bad key {key:?}"));
    }
    Ok(())
}

/// A value for a `curl -K` config file.
fn cfg_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Entries of a Unix-style long listing (what FTP `LIST` and curl's SFTP listing return) as
/// `(name, is_directory)`. A line that is just a name is taken as a file.
fn parse_listing(text: &str) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    for line in text.lines().map(str::trim_end).filter(|l| !l.is_empty()) {
        if line.starts_with("total ") {
            continue;
        }
        let mut fields = line.splitn(9, char::is_whitespace);
        let kind = line.chars().next().unwrap_or(' ');
        let (name, is_dir) =
            if matches!(kind, 'd' | '-' | 'l') && line.split_whitespace().count() >= 9 {
                // Names can hold spaces, and runs of spaces separate the columns: skip 8 columns.
                let mut rest = line;
                for _ in 0..8 {
                    rest = rest.trim_start();
                    rest = rest.split_once(char::is_whitespace).map_or("", |(_, r)| r);
                }
                let _ = fields.next();
                (rest.trim_start().to_string(), kind == 'd')
            } else {
                (line.trim().to_string(), false)
            };
        if kind == 'l' && name.contains(" -> ") || name == "." || name == ".." || name.is_empty() {
            continue;
        }
        out.push((name, is_dir));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(proto: Protocol, path: &str) -> CurlBackend {
        RemoteConfig::check_dir(path).unwrap();
        CurlBackend::new(RemoteConfig {
            protocol: proto,
            origin: RemoteConfig::origin(proto, "h.example", Some(2121)).unwrap(),
            dir: path.to_string(),
            user: None,
            password: None,
            key_file: None,
            host_key_sha256: None,
            require_tls: false,
            timeout: Duration::from_secs(30),
            curl: "curl".into(),
        })
    }

    #[test]
    fn hosts_and_dirs_are_validated() {
        assert!(RemoteConfig::origin(Protocol::Ftp, "ftp.example.com", None).is_ok());
        assert!(RemoteConfig::origin(Protocol::Sftp, "10.0.0.1", Some(22)).is_ok());
        for bad in ["", "h/x", "u@h", "h h", "h?x", "h#"] {
            assert!(
                RemoteConfig::origin(Protocol::Ftp, bad, None).is_err(),
                "{bad}"
            );
        }
        for ok in ["/", "/out/", "//srv/out/", "/a b/"] {
            assert!(RemoteConfig::check_dir(ok).is_ok(), "{ok}");
        }
        for bad in ["", "out/", "/out", "/a/../b/", "/a/./b/", "/a\nb/"] {
            assert!(RemoteConfig::check_dir(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn keys_become_encoded_urls() {
        let b = backend(Protocol::Ftp, "/out box/");
        assert_eq!(
            b.url("a b/c#d.csv").unwrap(),
            "ftp://h.example:2121/out%20box/a%20b/c%23d.csv"
        );
        assert!(b.url("../x").is_err());
        assert!(b.url("a//b").is_err());
        assert!(b.url("").is_err());
    }

    #[test]
    fn quote_paths_follow_each_protocols_convention() {
        let ftp = backend(Protocol::Ftp, "/out/");
        assert_eq!(ftp.quote_path("a/b"), "out/a/b");
        assert_eq!(ftp.delete_command("a"), "DELE out/a");
        assert_eq!(
            ftp.rename_command("d/a.tmp", "d/a"),
            vec!["-RNFR a.tmp", "-RNTO a"]
        );
        let abs = backend(Protocol::Ftp, "//srv/out/");
        assert_eq!(abs.quote_path("a"), "/srv/out/a");
        let sftp = backend(Protocol::Sftp, "/home/acme/");
        assert_eq!(sftp.delete_command("a b"), "rm \"/home/acme/a b\"");
        assert_eq!(
            sftp.rename_command("t", "a\"b"),
            vec!["-rename \"/home/acme/t\" \"/home/acme/a\\\"b\""]
        );
    }

    #[test]
    fn config_values_are_escaped() {
        assert_eq!(cfg_quote("a\"b\\c\nd"), "\"a\\\"b\\\\c\\nd\"");
    }

    #[test]
    fn listings_are_parsed() {
        let l = "total 12\n\
drwxr-xr-x    2 ftp      ftp          4096 Oct 25 10:00 sub dir\n\
-rw-r--r--    1 ftp      ftp           120 Oct 25 10:00 a.csv\n\
-rw-r--r--    1 ftp      ftp             0 Oct 25 10:00 my  file.txt\n\
lrwxrwxrwx    1 ftp      ftp             3 Oct 25 10:00 link -> a.csv\n\
drwxr-xr-x    2 ftp      ftp          4096 Oct 25 10:00 .\n\
drwxr-xr-x    2 ftp      ftp          4096 Oct 25 10:00 ..\n\
bare.txt\n";
        assert_eq!(
            parse_listing(l),
            vec![
                ("sub dir".to_string(), true),
                ("a.csv".to_string(), false),
                ("my  file.txt".to_string(), false),
                ("bare.txt".to_string(), false),
            ]
        );
    }

    /// Needs a running FTP server: `INTEGRAAL_TEST_FTP=host:port/path/ user:password`-style is
    /// avoided; set `INTEGRAAL_TEST_FTP_HOST`, `_PORT`, `_USER`, `_PASSWORD` and run with `--ignored`.
    #[test]
    #[ignore = "needs an FTP server"]
    fn round_trip_against_a_real_ftp_server() {
        let env = |k: &str| std::env::var(format!("INTEGRAAL_TEST_FTP_{k}")).unwrap();
        let mut b = backend(Protocol::Ftp, "/");
        b.cfg.origin = RemoteConfig::origin(
            Protocol::Ftp,
            &env("HOST"),
            Some(env("PORT").parse().unwrap()),
        )
        .unwrap();
        b.cfg.user = Some(env("USER"));
        b.cfg.password = Some(env("PASSWORD"));
        CurlBackend::check_curl("curl", Protocol::Ftp).unwrap();

        assert_eq!(b.get("nothing/here.csv").unwrap(), None);
        assert!(b.open("nothing/here.csv").unwrap().is_none());
        b.put("in/a b.csv", b"1,2\n".to_vec()).unwrap();
        b.put("in/sub/c.csv", b"3".to_vec()).unwrap();
        b.put("empty.txt", Vec::new()).unwrap();
        assert_eq!(b.get("in/a b.csv").unwrap().unwrap(), b"1,2\n");
        assert_eq!(b.get("empty.txt").unwrap().unwrap(), b"");
        let mut got = String::new();
        b.open("in/a b.csv")
            .unwrap()
            .unwrap()
            .read_to_string(&mut got)
            .unwrap();
        assert_eq!(got, "1,2\n");
        assert_eq!(
            b.list("in/", 100).unwrap(),
            vec!["in/a b.csv", "in/sub/c.csv"]
        );
        assert_eq!(b.list("in/a", 100).unwrap(), vec!["in/a b.csv"]);
        assert_eq!(b.list("", 2).unwrap().len(), 2);

        // A big streamed upload, then an aborted one that must leave nothing behind.
        let mut w = b.create("big.bin").unwrap();
        for _ in 0..64 {
            w.write(&[7u8; 64 * 1024]).unwrap();
        }
        w.commit().unwrap();
        let mut n = 0;
        let mut r = b.open("big.bin").unwrap().unwrap();
        let mut buf = vec![0u8; 100_000];
        loop {
            let k = r.read(&mut buf).unwrap();
            if k == 0 {
                break;
            }
            n += k;
        }
        assert_eq!(n, 64 * 64 * 1024);
        let mut w = b.create("aborted.bin").unwrap();
        w.write(b"partial").unwrap();
        w.abort();
        assert_eq!(b.get("aborted.bin").unwrap(), None);
        assert!(!b
            .list("", 100)
            .unwrap()
            .iter()
            .any(|k| k.contains("aborted")));

        b.delete("in/a b.csv").unwrap();
        b.delete("in/a b.csv").unwrap(); // already gone
        assert_eq!(b.get("in/a b.csv").unwrap(), None);
    }
}
