//! The daemon config file: read once at start, never re-read.
//!
//! The file is TOML. The top-level keys are `store_path`, `socket_path`, `host_id`, `history` and
//! `[peers.<peer_id>]`; `invoke.timeout_ms`, `identity.seed_file`, `journal.dir` and
//! `adoption.{scope_id, source.collection}` sit in the tables the contract names. Every table
//! denies unknown keys, so a typo is a start failure rather than a silently ignored setting, and
//! `peers` is one table of `{public_key, principal}` rows so the allowlist, the peer keys and the
//! principal mapping cannot disagree.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::DaemonError;

/// `[invoke]`: the responder's invocation budget.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvokeConfig {
    /// `ConnectResponderOptions.invoke_timeout_ms`; absent keeps spoke_connect's own default.
    pub timeout_ms: Option<u64>,
}

/// `[identity]`: the responder's own key.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    /// The file holding the responder's Ed25519 seed as 64 hex digits.
    pub seed_file: PathBuf,
}

/// `[journal]`: where the publisher writes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalConfig {
    /// The journal root the publisher writes under.
    pub dir: PathBuf,
}

/// One admitted peer: the Ed25519 public key its hello must prove, and the principal it maps to.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// The peer's Ed25519 public key, 64 hex digits.
    pub public_key: String,
    /// The `Principal.principal_id` an authenticated peer with this key answers as.
    pub principal: String,
}

/// `[adoption]`: the route an admitted Mind adopts from. Present as a whole or not at all.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionConfig {
    /// The host-owned id naming the adoption source on the wire.
    pub scope_id: String,
    /// `[adoption.source]`, or the same as an inline table.
    pub source: AdoptionSource,
}

/// `[adoption.source]`: the one collection every admitted Mind adopts from.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionSource {
    /// The one collection every admitted Mind adopts from.
    pub collection: String,
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
    /// `[invoke]`; absent keeps every default.
    #[serde(default)]
    pub invoke: InvokeConfig,
    /// `[identity]`.
    pub identity: IdentityConfig,
    /// The peer id → key and principal table. Empty serves nobody.
    #[serde(default)]
    pub peers: BTreeMap<String, PeerConfig>,
    /// `[journal]`.
    pub journal: JournalConfig,
    /// `[adoption]`, absent or present as a whole.
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
    refuse_readable_seed(path)?;
    std::fs::read_to_string(path)
        .map_err(|error| {
            DaemonError::Config(format!("identity.seed_file {}: {error}", path.display()))
        })
        .and_then(|seed| {
            hex32(seed.trim()).map_err(|fault| {
                DaemonError::Config(format!("identity.seed_file {}: {fault}", path.display()))
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
        read_seed(&self.identity.seed_file)?;
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
            if adoption.source.collection.trim().is_empty() {
                return Err(named(
                    "adoption.source.collection",
                    "must not be empty".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Refuses a seed file group or other can read, write or execute.
///
/// The seed is the responder's signing identity. Anyone who can read it can speak as this daemon, and
/// anyone who can write it can put their own key in its place and speak as this daemon too — so a
/// mode with any group or other bit set is a configuration fault rather than a warning. The execute
/// bits are included because the message says "group or other": a mode the guard lets through must
/// not be one the wording claims to refuse. Only the mode bits are reported, never the bytes, so the
/// refusal can be shown without leaking the key.
#[cfg(unix)]
fn refuse_readable_seed(path: &Path) -> Result<(), DaemonError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path)
        .map_err(|error| {
            DaemonError::Config(format!("identity.seed_file {}: {error}", path.display()))
        })?
        .permissions()
        .mode()
        & 0o777;
    // Every group and other bit, execute included: `0o0620` is writable by group, as fatal as
    // readable, and `0o0611` is executable — the wording says "group or other", so all three count.
    if mode & 0o077 != 0 {
        return Err(DaemonError::Config(format!(
            "identity.seed_file {}: mode {mode:04o} is readable, writable or executable by group or \
             other; use 0600",
            path.display()
        )));
    }
    Ok(())
}

/// Decodes a value that must be exactly 64 hex digits.
///
/// The messages name the failure and the observed length only: the same helper decodes the
/// responder's private seed, and an error reaches stderr through `main`.
pub(crate) fn hex32(value: &str) -> Result<[u8; 32], String> {
    let mut digits = Vec::with_capacity(64);
    for byte in value.bytes() {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => {
                return Err(format!(
                    "expected 64 hex digits, found a non-hex digit; observed length {}",
                    value.len()
                ))
            }
        };
        digits.push(digit);
    }
    if digits.len() != 64 {
        return Err(format!(
            "expected 64 hex digits; observed length {}",
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
    /// A seed file no other user may read, as the contract requires of a real one.
    fn write_seed(path: &Path, contents: &str) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, contents).expect("write seed");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("make the seed owner-only");
    }

    fn minimal(dir: &Path) -> String {
        let seed = dir.join("seed");
        write_seed(&seed, SEED);
        format!(
            r#"
store_path = "{}"
socket_path = "{}"
host_id = "host-1"

[identity]
seed_file = "{}"

[journal]
dir = "{}"

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
        assert_eq!(config.invoke.timeout_ms, None);
        assert!(config.adoption.is_none(), "no adoption route by default");
        assert_eq!(config.peers["peer-a"].principal, "principal-a");
        assert_eq!(config.peers["peer-a"].public_key, PEER_KEY);
    }

    #[test]
    fn the_optional_tables_parse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = minimal(dir.path()).replace(
            "host_id = \"host-1\"\n",
            "host_id = \"host-1\"\nhistory = true\n\n[invoke]\ntimeout_ms = 30000\n",
        );
        let text = format!(
            "{text}\n[adoption]\nscope_id = \"scope-1\"\nsource = {{ collection = \"collected\" }}\n"
        );
        let config = load(&write(dir.path(), &text)).expect("load");

        assert!(config.history);
        assert_eq!(config.invoke.timeout_ms, Some(30000));
        let adoption = config.adoption.expect("adoption");
        assert_eq!(adoption.scope_id, "scope-1");
        assert_eq!(adoption.source.collection, "collected");
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
    fn a_missing_required_table_names_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = minimal(dir.path()).replace("seed_file", "ignored");
        let error = load(&write(dir.path(), &text)).expect_err("missing seed_file");

        assert!(
            error.to_string().contains("seed_file"),
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
    fn the_old_flat_keys_are_unknown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let seed = dir.path().join("seed");
        write_seed(&seed, SEED);
        let text = format!(
            r#"
store_path = "{}"
socket_path = "{}"
host_id = "host-1"
identity_seed_file = "{}"
journal_dir = "{}"
"#,
            dir.path().join("mind.db").display(),
            dir.path().join("mind.sock").display(),
            seed.display(),
            dir.path().join("journal").display(),
        );
        let error = load(&write(dir.path(), &text)).expect_err("flat keys");

        assert!(
            error.to_string().contains("identity_seed_file"),
            "the message names the flat key: {error}"
        );
    }

    #[test]
    fn a_short_seed_names_the_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = minimal(dir.path());
        write_seed(&dir.path().join("seed"), &SEED[..62]);
        let error = load(&write(dir.path(), &text)).expect_err("short seed");

        let message = error.to_string();
        assert!(
            message.contains("identity.seed_file"),
            "the message names the key: {message}"
        );
        assert!(
            message.contains("observed length 62"),
            "the message names the length: {message}"
        );
    }

    /// A seed any other user could read is the responder's identity handed out: refused before it is
    /// decoded, with the mode named and never the bytes.
    #[test]
    fn a_world_readable_seed_is_refused() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let seed = dir.path().join("seed");
        write_seed(&seed, SEED);
        std::fs::set_permissions(&seed, std::fs::Permissions::from_mode(0o644))
            .expect("widen the seed");

        match read_seed(&seed) {
            Err(DaemonError::Config(message)) => {
                assert!(message.contains("identity.seed_file"), "{message}");
                assert!(message.contains("0644"), "the mode is named: {message}");
                assert!(
                    message.contains("readable, writable or executable by group or other"),
                    "{message}"
                );
                assert!(
                    !message.contains(&SEED[..8]),
                    "the refusal must not carry the seed: {message}"
                );
            }
            other => panic!("a world-readable seed is a config fault, not {other:?}"),
        }

        std::fs::set_permissions(&seed, std::fs::Permissions::from_mode(0o600))
            .expect("narrow the seed");
        assert_eq!(read_seed(&seed).expect("a 0600 seed parses"), {
            let mut bytes = [0u8; 32];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&SEED[index * 2..index * 2 + 2], 16).expect("hex");
            }
            bytes
        });
    }

    /// Any group or other bit is refused, not only a read bit: `0620` is group-writable, which lets
    /// an attacker replace the key rather than merely read it.
    #[test]
    fn every_group_or_other_bit_is_refused() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let seed = dir.path().join("seed");
        for (mode, label) in [
            (0o640u32, "group-readable"),
            (0o604, "other-readable"),
            (0o620, "group-writable"),
            (0o602, "other-writable"),
            (0o622, "group- and other-writable"),
            (0o611, "group-executable and other-executable"),
            (0o641, "group-readable and other-executable"),
            (0o601, "other-executable"),
        ] {
            write_seed(&seed, SEED);
            std::fs::set_permissions(&seed, std::fs::Permissions::from_mode(mode))
                .expect("set the mode");
            match read_seed(&seed) {
                Err(DaemonError::Config(message)) => {
                    assert!(
                        message.contains(&format!("{mode:04o}")),
                        "{label}: the mode is named: {message}"
                    );
                    assert!(
                        message.contains("readable, writable or executable by group or other"),
                        "{label}: {message}"
                    );
                    assert!(
                        !message.contains(&SEED[..8]),
                        "{label}: the refusal must not carry the seed: {message}"
                    );
                }
                other => panic!("a {label} seed is a config fault, not {other:?}"),
            }
        }

        write_seed(&seed, SEED);
        read_seed(&seed).expect("a 0600 seed parses");
    }

    /// The seed is a private key, and `main` prints the error: the digits must never travel.
    #[test]
    fn the_seed_value_never_reaches_the_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = minimal(dir.path());
        let secret = format!("{SEED}x");
        write_seed(&dir.path().join("seed"), &secret);
        let error = load(&write(dir.path(), &text)).expect_err("malformed seed");

        let message = error.to_string();
        assert!(
            message.contains("identity.seed_file"),
            "the message names the key: {message}"
        );
        assert!(
            message.contains("observed length 65"),
            "the message names the length: {message}"
        );
        for leak in [SEED, secret.as_str(), &SEED[..8]] {
            assert!(
                !message.contains(leak),
                "the message carries no seed digits: {message}"
            );
        }
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
