//! Journaled file effects. A staged mutation declares, for every projection it
//! owes, the target path and the exact bytes that publish it; the caller
//! publishes those targets after committing and the effect's digest lets it
//! confirm what landed.

use serde_json::{json, Value};

use mindcore_base::ir::wire::{memory_candidate_to_wire, memory_record_to_wire, WireVersion};
use mindcore_base::ir::{MemoryCandidateIr, MemoryProfileRow, MemoryRecordIr};

use crate::policy;
use crate::project;
use crate::reject::AResult;

fn version() -> WireVersion {
    WireVersion::new(serde_json::Number::from(project::SCHEMA_VERSION))
        .expect("positive schema version")
}

/// One caller-derived string as a front-matter scalar. A safe identifier stays
/// bare (byte-identical); anything else becomes a JSON-encoded double-quoted
/// scalar, which YAML 1.2 reads identically and a newline can never split into
/// injected keys.
fn yaml_str(value: &str) -> String {
    static YAML_WORDS: &[&str] = &[
        "y", "yes", "n", "no", "true", "false", "on", "off", "null", "none", "~",
    ];
    let bare = !value.is_empty()
        && value
            .chars()
            .next()
            .is_some_and(|head| head.is_ascii_alphabetic() || head == '_')
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !YAML_WORDS.contains(&value.to_ascii_lowercase().as_str());
    if bare {
        value.to_string()
    } else {
        serde_json::to_string(value).expect("a string serializes")
    }
}

/// §6.6 record projection target.
pub(crate) fn record_target(record_id: &str) -> String {
    format!("records/{record_id}.md")
}

/// §6.7 consumed-candidate shell target.
pub(crate) fn candidate_target(candidate_id: &str) -> String {
    format!("candidates/{candidate_id}.md")
}

/// §6.8 profile target; the file name digests the subject/kind pair, so an
/// open-vocabulary id never becomes a path component.
pub(crate) fn profile_target(subject_id: &str, profile_kind: &str) -> AResult<String> {
    let digest =
        policy::digest(&json!({ "subject_id": subject_id, "profile_kind": profile_kind }))?;
    Ok(format!("profiles/{digest}.md"))
}

/// One file effect: the target, the digest of its bytes, the provider's own
/// previous projection, and the content to publish.
pub(crate) struct FileEffect {
    pub(crate) path: String,
    pub(crate) digest: String,
    pub(crate) prior: Option<String>,
    pub(crate) content: String,
}

impl FileEffect {
    fn new(path: String, content: String, prior: Option<String>) -> AResult<Self> {
        // shortcut: the digest is JCS/SHA-256 over the content object, not the
        // file's own bytes — the adapter and its publisher recompute it the
        // same way; upgrade to a raw-bytes hash if a foreign tool must verify.
        let digest = policy::digest(&json!({ "content": content }))?;
        Ok(Self {
            path,
            digest,
            prior,
            content,
        })
    }

    pub(crate) fn into_value(self) -> Value {
        json!({
            "path": self.path,
            "digest": self.digest,
            "prior": self.prior,
            "content": self.content,
        })
    }
}

/// §6.6 record projection: the stored row as markdown front matter.
pub(crate) fn record_effect(record: &MemoryRecordIr, prior: Option<String>) -> AResult<Value> {
    let wire = memory_record_to_wire(version(), record.clone());
    let visibility = wire
        .get("scope")
        .and_then(|scope| scope.get("visibility"))
        .and_then(Value::as_str)
        .unwrap_or("shared");
    let context = wire
        .get("scope")
        .and_then(|scope| scope.get("context_id"))
        .and_then(Value::as_str);
    let mut front = format!(
        "---\nrecord_id: {}\nspace_id: {}\nsubject_id: {}\nkind: {}\nvisibility: {}\n",
        yaml_str(&record.record_id),
        yaml_str(record.space_id.as_str()),
        yaml_str(record.subject_id.as_str()),
        yaml_str(&record.kind),
        visibility
    );
    if let Some(context_id) = context {
        front.push_str(&format!("context_id: {}\n", yaml_str(context_id)));
    }
    front.push_str(&format!(
        "revision: {}\ncreated_at: {}\nupdated_at: {}\n---\n\n{}\n",
        record.revision,
        record.created_at,
        record.updated_at,
        serde_json::to_string(&wire.get("content").cloned().unwrap_or(Value::Null))
            .unwrap_or_default()
    ));
    Ok(FileEffect::new(record_target(&record.record_id), front, prior)?.into_value())
}

/// §6.7 consumed-candidate shell projection.
pub(crate) fn candidate_effect(candidate: &MemoryCandidateIr) -> AResult<Value> {
    let mut wire = memory_candidate_to_wire(version(), candidate.clone());
    let object = wire
        .as_object_mut()
        .expect("candidate projection is an object");
    object.insert(
        "proposed".to_string(),
        json!({ "kind": candidate_kind(candidate) }),
    );
    let content = format!(
        "---\ncandidate_id: {}\nspace_id: {}\nsubject_id: {}\nkind: {}\nstatus: {}\n\
         revision: {}\ncreated_at: {}\n---\n\n{}\n",
        yaml_str(&candidate.candidate_id),
        yaml_str(candidate.space_id.as_str()),
        yaml_str(candidate.subject_id.as_str()),
        yaml_str(candidate_kind(candidate)),
        candidate_status(candidate),
        candidate.revision,
        candidate.created_at,
        serde_json::to_string(&wire).unwrap_or_default()
    );
    Ok(FileEffect::new(candidate_target(&candidate.candidate_id), content, None)?.into_value())
}

fn candidate_kind(candidate: &MemoryCandidateIr) -> &str {
    match &candidate.state {
        mindcore_base::ir::CandidateState::Pending { kind, .. } => kind.as_str(),
        mindcore_base::ir::CandidateState::Consumed { proposed_kind, .. } => {
            proposed_kind.as_deref().unwrap_or("note")
        }
    }
}

fn candidate_status(candidate: &MemoryCandidateIr) -> &'static str {
    match &candidate.state {
        mindcore_base::ir::CandidateState::Pending { .. } => "pending",
        mindcore_base::ir::CandidateState::Consumed { .. } => "consumed",
    }
}

/// §6.8 profile projection.
pub(crate) fn profile_effect(profile: &MemoryProfileRow) -> AResult<Value> {
    let document = serde_json::to_string(&profile.document).unwrap_or_else(|_| "{}".to_string());
    let content = format!(
        "---\nspace_id: {}\nsubject_id: {}\nprofile_kind: {}\nupdated_by: {}\n\
         revision: {}\ncreated_at: {}\nupdated_at: {}\n---\n\n{document}\n",
        yaml_str(profile.space_id.as_str()),
        yaml_str(profile.subject_id.as_str()),
        yaml_str(&profile.profile_kind),
        yaml_str(profile.updated_by.as_str()),
        profile.revision,
        profile.created_at,
        profile.updated_at
    );
    Ok(FileEffect::new(
        profile_target(profile.subject_id.as_str(), &profile.profile_kind)?,
        content,
        None,
    )?
    .into_value())
}

/// The digest a replaced projection's effect names as its predecessor.
pub(crate) fn projection_digest(path: &str, content: &str) -> AResult<String> {
    let _ = path;
    policy::digest(&json!({ "content": content }))
}
