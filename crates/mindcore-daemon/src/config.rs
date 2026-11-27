//! The daemon config file: read once at start, never re-read.
//!
//! The file is TOML. Every key is denied when unknown, so a typo is a start failure rather than a
//! silently ignored setting; `peers` is one table of `{public_key, principal}` rows so the
//! allowlist, the peer keys and the principal mapping cannot disagree.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::DaemonError;

/// One admitted peer: the Ed25519 public key its hello must prove, and the principal it maps to.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// The peer's Ed25519 public key, 64 hex digits.
    pub public_key: String,
    /// The `Principal.principal_id` an authenticated peer with this key answers as.
    pub principal: String,
}

/// The adoption route an admitted Mind adopts from. Present as a whole or not at all.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionConfig {
    /// The host-owned id naming the adoption source on the wire.
    pub scope_id: String,
    /// The one collection every admitted Mind adopts from.
    pub source_collection: String,
}

/// The validated daemon config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    /// The one SQLite file the daemon serves.
    pub store_path: PathBuf,
    /// The Unix socket the listener binds.
    pub socket_path: PathBuf,
    /// `HostCapabilityManifest.host_id` in the signed hello.
    pub host_id: String,
    /// Whether the per-Mind history face is composed.
    #[serde(default)]
    pub history: bool,
    /// The responder's `invoke_timeout_ms`; absent keeps spoke_connect's own default.
    pub invoke_timeout_ms: Option<u64>,
    /// The file holding the responder's Ed25519 seed as 64 hex digits.
    pub identity_seed_file: PathBuf,
    /// The peer id → key and principal table. Empty serves nobody.
    #[serde(default)]
    pub peers: BTreeMap<String, PeerConfig>,
    /// The journal root the publisher writes under.
    pub journal_dir: PathBuf,
    /// The adoption route, absent or present as a whole.
    pub adoption: Option<AdoptionConfig>,
}

/// Reads and validates `path`.
///
/// Every failure is [`DaemonError::Config`] and names the file, and the key when one value is at
/// fault.
pub fn load(path: &Path) -> Result<DaemonConfig, DaemonError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| DaemonError::Config(format!("{}: {error}", path.display())))?;
    // `toml`'s own message already quotes the offending key.
    let config: DaemonConfig = toml::from_str(&text)
        .map_err(|error| DaemonError::Config(format!("{}: {error}", path.display())))?;
    config.validate(path)?;
    Ok(config)
}

/// The responder's Ed25519 seed: `path` must hold exactly 64 hex digits.
pub fn read_seed(path: &Path) -> Result<[u8; 32], DaemonError> {
    std::fs::read_to_string(path)
        .map_err(|error| {
            DaemonError::Config(format!("identity_seed_file {}: {error}", path.display()))
        })
        .and_then(|seed| {
            hex32(seed.trim()).map_err(|fault| {
                DaemonError::Config(format!("identity_seed_file {}: {fault}", path.display()))
            })
        })
}

impl DaemonConfig {
    /// Checks the values TOML's types cannot: the seed file's contents, each peer key, and the
    /// adoption route's two members.
    fn validate(&self, path: &Path) -> Result<(), DaemonError> {
        let named = |key: &str, fault: String| {
            DaemonError::Config(format!("{}: {key}: {fault}", path.display()))
        };
        if self.host_id.trim().is_empty() {
            return Err(named("host_id", "must not be empty".into()));
        }
        read_seed(&self.identity_seed_file)?;
        for (peer_id, peer) in &self.peers {
            if peer.principal.trim().is_empty() {
                return Err(named(
                    &format!("peers.{peer_id}.principal"),
                    "must not be empty".into(),
                ));
            }
            hex32(&peer.public_key)
                .map_err(|fault| named(&format!("peers.{peer_id}.public_key"), fault))?;
        }
        if let Some(adoption) = &self.adoption {
            if adoption.scope_id.trim().is_empty() {
                return Err(named("adoption.scope_id", "must not be empty".into()));
            }
            if adoption.source_collection.trim().is_empty() {
                return Err(named(
                    "adoption.source_collection",
                    "must not be empty".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Decodes a value that must be exactly 64 hex digits.
fn hex32(value: &str) -> Result<[u8; 32], String> {
    let digits: Vec<u8> = value
        .bytes()
        .map(|byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(format!("expected 64 hex digits, found {value:?}")),
        })
        .collect::<Result<_, _>>()?;
    if digits.len() != 64 {
        return Err(format!(
            "expected 64 hex digits, found {} in {value:?}",
            digits.len()
        ));
    }
    let mut key = [0u8; 32];
    for (byte, pair) in key.iter_mut().zip(digits.as_chunks::<2>().0) {
        *byte = pair[0] << 4 | pair[1];
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: &str = "0101010101010101010101010101010101010101010101010101010101010101";
    const PEER_KEY: &str = "0202020202020202020202020202020202020202020202020202020202020202";

    /// Writes a seed file into `dir` and answers the minimal config naming it.
    fn minimal(dir: &Path) -> String {
        let seed = dir.join("seed");
        std::fs::write(&seed, SEED).expect("write seed");
        format!(
            r#"
store_path = "{}"
socket_path = "{}"
host_id = "host-1"
identity_seed_file = "{}"
journal_dir = "{}"

[peers.peer-a]
public_key = "{PEER_KEY}"
principal = "principal-a"
"#,
            dir.join("mind.db").display(),
            dir.join("mind.sock").display(),
            seed.display(),
            dir.join("journal").display(),
        )
    }

    fn write(dir: &Path, text: &str) -> PathBuf {
        let path = dir.join("daemon.toml");
        std::fs::write(&path, text).expect("write config");
        path
    }

    #[test]
    fn a_minimal_toml_parses() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = load(&write(dir.path(), &minimal(dir.path()))).expect("load");

        assert_eq!(config.host_id, "host-1");
        assert!(!config.history, "history defaults off");
        assert_eq!(config.invoke_timeout_ms, None);
        assert!(config.adoption.is_none(), "no adoption route by default");
        assert_eq!(config.peers["peer-a"].principal, "principal-a");
        assert_eq!(config.peers["peer-a"].public_key, PEER_KEY);
    }

    #[test]
    fn the_optional_keys_parse() {
        let dir = tempfile::tempdir().expect("tempdir");
        // The optional keys lead: TOML keeps a bare key after a table inside that table.
        let text = minimal(dir.path()).replace(
            "host_id = \"host-1\"\n",
            "host_id = \"host-1\"\nhistory = true\ninvoke_timeout_ms = 2500\n",
        );
        let text = format!(
            "{text}\n[adoption]\nscope_id = \"scope-1\"\nsource_collection = \"collected\"\n"
        );
        let config = load(&write(dir.path(), &text)).expect("load");

        assert!(config.history);
        assert_eq!(config.invoke_timeout_ms, Some(2500));
        let adoption = config.adoption.expect("adoption");
        assert_eq!(adoption.scope_id, "scope-1");
        assert_eq!(adoption.source_collection, "collected");
    }

    #[test]
    fn a_missing_key_names_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = minimal(dir.path()).replace("host_id = \"host-1\"\n", "");
        let error = load(&write(dir.path(), &text)).expect_err("missing host_id");

        assert!(
            error.to_string().contains("host_id"),
            "the message names the key: {error}"
        );
    }

    #[test]
    fn an_unknown_key_names_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = format!("{}\nsurprise = 1\n", minimal(dir.path()));
        let error = load(&write(dir.path(), &text)).expect_err("unknown key");

        assert!(
            error.to_string().contains("surprise"),
            "the message names the key: {error}"
        );
    }

    #[test]
    fn a_short_seed_names_the_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = minimal(dir.path());
        std::fs::write(dir.path().join("seed"), &SEED[..62]).expect("short seed");
        let error = load(&write(dir.path(), &text)).expect_err("short seed");

        let message = error.to_string();
        assert!(
            message.contains("identity_seed_file"),
            "the message names the key: {message}"
        );
        assert!(
            message.contains("62"),
            "the message names the digits found: {message}"
        );
    }

    #[test]
    fn a_short_peer_key_names_the_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = minimal(dir.path()).replace(PEER_KEY, &PEER_KEY[..62]);
        let error = load(&write(dir.path(), &text)).expect_err("short public key");

        let message = error.to_string();
        assert!(
            message.contains("peers.peer-a.public_key"),
            "the message names the key: {message}"
        );
        assert!(
            message.contains("62"),
            "the message names the digits found: {message}"
        );
    }

    #[test]
    fn a_missing_file_names_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("absent.toml");
        let error = load(&path).expect_err("missing file");

        assert!(
            error.to_string().contains(&path.display().to_string()),
            "the message names the path: {error}"
        );
    }
}
