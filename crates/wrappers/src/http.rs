use std::net::IpAddr;

use serde::Deserialize;
use url::Url;

/// Allowlist of outbound HTTP targets, loaded from the tenant's `config.json`:
///
/// ```json
/// { "http": { "allow": [
///     "https://api.example.com/v1/",
///     "https://*.partner.io",
///     { "url": "https://hooks.example.com/", "methods": ["POST"] }
/// ] } }
/// ```
///
/// Entries match on scheme, host (`*.` matches subdomains only), port and path
/// prefix. An empty policy denies everything.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct HttpPolicy {
    #[serde(default)]
    pub allow: Vec<AllowRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AllowRule {
    Url(String),
    Detailed {
        url: String,
        #[serde(default)]
        methods: Vec<String>,
    },
}

impl HttpPolicy {
    /// Substitutes `${name}` in allow rules from the tenant's string vars.
    pub fn expand(&self, vars: &serde_json::Map<String, serde_json::Value>) -> Result<HttpPolicy, String> {
        let sub = |s: &str| -> Result<String, String> {
            let mut out = String::new();
            let mut rest = s;
            while let Some(i) = rest.find("${") {
                out.push_str(&rest[..i]);
                let end = rest[i..].find('}').ok_or_else(|| format!("unterminated ${{ in {s:?}"))? + i;
                let name = &rest[i + 2..end];
                match vars.get(name) {
                    Some(serde_json::Value::String(v)) => out.push_str(v),
                    Some(_) => return Err(format!("var {name:?} must be a string to be used in http.allow")),
                    None => return Err(format!("undefined var {name:?} in http.allow")),
                }
                rest = &rest[end + 1..];
            }
            out.push_str(rest);
            Ok(out)
        };
        let allow = self
            .allow
            .iter()
            .map(|r| {
                Ok(match r {
                    AllowRule::Url(u) => AllowRule::Url(sub(u)?),
                    AllowRule::Detailed { url, methods } => {
                        AllowRule::Detailed { url: sub(url)?, methods: methods.clone() }
                    }
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(HttpPolicy { allow })
    }

    /// Returns the parsed URL if `method url` is permitted.
    pub fn check(&self, method: &str, raw: &str) -> Result<Url, String> {
        let url = Url::parse(raw).map_err(|e| format!("invalid url: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") || !url.username().is_empty() || url.password().is_some() {
            return Err("only http(s) urls without credentials are allowed".into());
        }
        let host = url.host_str().ok_or("url has no host")?;
        if let Ok(ip) = host.trim_matches(['[', ']']).parse::<IpAddr>() {
            if !is_public(ip) {
                return Err(format!("address {host} is not publicly routable"));
            }
        }
        for rule in &self.allow {
            let (pattern, methods) = match rule {
                AllowRule::Url(u) => (u.as_str(), &[][..]),
                AllowRule::Detailed { url, methods } => (url.as_str(), methods.as_slice()),
            };
            if matches_rule(pattern, &url) && (methods.is_empty() || methods.iter().any(|m| m.eq_ignore_ascii_case(method))) {
                return Ok(url);
            }
        }
        Err(format!("{method} {raw} is not in this tenant's http allowlist"))
    }
}

fn matches_rule(pattern: &str, url: &Url) -> bool {
    // `Url` cannot parse a wildcard host, so swap it for a placeholder first.
    let (pattern, wildcard) = match pattern.split_once("://*.") {
        Some((scheme, rest)) => (format!("{scheme}://wildcard.{rest}"), true),
        None => (pattern.to_string(), false),
    };
    let Ok(rule) = Url::parse(&pattern) else { return false };
    let (Some(rule_host), Some(host)) = (rule.host_str(), url.host_str()) else { return false };
    let host_ok = if wildcard {
        let suffix = &rule_host["wildcard".len()..]; // ".example.com"
        host.len() > suffix.len() && host.ends_with(suffix)
    } else {
        host == rule_host
    };
    host_ok
        && rule.scheme() == url.scheme()
        && rule.port_or_known_default() == url.port_or_known_default()
        && url.path().starts_with(rule.path())
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified() || v4.is_broadcast())
        }
        IpAddr::V6(v6) => !(v6.is_loopback() || v6.is_unspecified() || (v6.segments()[0] & 0xfe00) == 0xfc00 || (v6.segments()[0] & 0xffc0) == 0xfe80),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(json: &str) -> HttpPolicy {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn matches_host_path_method() {
        let p = policy(
            r#"{"allow":["https://api.example.com/v1/","https://*.partner.io",
                {"url":"https://hooks.example.com/","methods":["POST"]}]}"#,
        );
        assert!(p.check("GET", "https://api.example.com/v1/users").is_ok());
        assert!(p.check("GET", "https://api.example.com/v2/users").is_err());
        assert!(p.check("GET", "http://api.example.com/v1/users").is_err());
        assert!(p.check("GET", "https://a.b.partner.io/x").is_ok());
        assert!(p.check("GET", "https://partner.io/x").is_err());
        assert!(p.check("GET", "https://evilpartner.io/x").is_err());
        assert!(p.check("POST", "https://hooks.example.com/h").is_ok());
        assert!(p.check("GET", "https://hooks.example.com/h").is_err());
    }

    #[test]
    fn expands_vars() {
        let p = policy(r#"{"allow":["${api_base}orders/"]}"#);
        let vars = serde_json::json!({"api_base": "https://api.example.com/v1/"});
        let p = p.expand(vars.as_object().unwrap()).unwrap();
        assert!(p.check("GET", "https://api.example.com/v1/orders/9").is_ok());
        assert!(policy(r#"{"allow":["${nope}"]}"#).expand(&Default::default()).is_err());
    }

    #[test]
    fn empty_policy_and_private_ips_deny() {
        assert!(HttpPolicy::default().check("GET", "https://example.com/").is_err());
        let p = policy(r#"{"allow":["http://169.254.169.254/"]}"#);
        assert!(p.check("GET", "http://169.254.169.254/latest/meta-data").is_err());
    }
}
