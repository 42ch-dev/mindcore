//! Success-response projections. Every body is rebuilt from the stored IR or
//! the pinned row through the wire codec; the result is then validated against
//! the pinned response type, so a projection can never drift from the schema it
//! is published under.

use serde::de::DeserializeOwned;
use serde_json::{json, Number, Value};

use mindcore_base::ir::wire::{
    memory_candidate_to_wire, memory_record_to_wire, review_receipt_to_wire, WireVersion,
};
use mindcore_base::ir::{
    MemoryCandidateIr, MemoryGrantRow, MemoryProfileRow, MemoryRecordIr, MemoryReflectionRow,
    MemorySpaceRow, MemorySubjectRow, Placement, ReviewReceiptIr,
};

use crate::reject::{AResult, Reject};

/// The only schema version these projections speak.
pub(crate) const SCHEMA_VERSION: u64 = 1;

fn version() -> WireVersion {
    WireVersion::new(Number::from(SCHEMA_VERSION)).expect("positive schema version")
}

/// Validate one rebuilt body against the pinned response type without rewriting
/// its member presence: the caller's wire bytes stay exactly as built.
pub(crate) fn checked<T: DeserializeOwned>(body: Value) -> AResult<Value> {
    serde_json::from_value::<T>(body.clone()).map_err(|error| {
        Reject::terminal(
            spoke_operations::SpokeRejectCode::InternalError,
            format!("response projection does not match its schema: {error}"),
        )
    })?;
    Ok(body)
}

/// §6.2 space projection.
pub(crate) fn space_body(space: &MemorySpaceRow) -> Value {
    json!({
        "schema_version": SCHEMA_VERSION,
        "space_id": space.space_id.as_str(),
        "display_name": space.display_name,
        "authority_host_id": space.authority_host_id,
        "authority_generation": space.authority_generation,
        "status": space.status,
        "pending_receipts": space.pending_receipts,
        "created_at": space.created_at,
        "revision": space.revision,
        "extensions": {},
    })
}

/// §6.3 subject projection. An absent `kind` is omitted, never `null`: the typed
/// client drops `None` on the wire, so a `null` here would make the chunk it sends
/// back disagree with the digest this store sealed.
pub(crate) fn subject_body(subject: &MemorySubjectRow) -> Value {
    let mut body = json!({
        "schema_version": SCHEMA_VERSION,
        "space_id": subject.space_id.as_str(),
        "subject_id": subject.subject_id.as_str(),
        "status": subject.status,
        "fenced_contexts": subject.fenced_contexts.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "created_at": subject.created_at,
        "revision": subject.revision,
        "extensions": {},
    });
    if let Some((ns, id)) = &subject.kind {
        body.as_object_mut()
            .expect("subject projection is an object")
            .insert("kind".to_string(), json!({ "ns": ns, "id": id }));
    }
    body
}

/// §6.4 grant projection; a restricted grant carries its context set.
pub(crate) fn grant_body(grant: &MemoryGrantRow) -> Value {
    let context_ids: Vec<&str> = grant.context_ids.iter().map(|id| id.as_str()).collect();
    let mut body = json!({
        "schema_version": SCHEMA_VERSION,
        "grant_id": grant.grant_id,
        "space_id": grant.space_id.as_str(),
        "subject_id": grant.subject_id.as_ref().map(|id| id.as_str()),
        "principal_id": grant.principal_id.as_str(),
        "capabilities": grant.capabilities,
        "created_at": grant.created_at,
        "expires_at": grant.expires_at,
        "revoked_at": grant.revoked_at,
        "revision": grant.revision,
        "extensions": {},
    });
    if !context_ids.is_empty() {
        let members = body.as_object_mut().expect("grant projection is an object");
        members.insert("context_ids".to_string(), json!(context_ids));
        members.insert("shared_access".to_string(), json!(grant.shared_access));
    }
    body
}

/// §6.5 placement projection (`scope` / `capture_scope`).
pub(crate) fn placement_body(placement: &Placement) -> Value {
    match placement {
        Placement::Shared => json!({ "visibility": "shared" }),
        Placement::Context(context_id) => {
            json!({ "visibility": "context", "context_id": context_id })
        }
    }
}

/// §6.7 candidate projection, rebuilt from the decoded IR.
pub(crate) fn candidate_body(candidate: &MemoryCandidateIr) -> Value {
    memory_candidate_to_wire(version(), candidate.clone())
}

/// §6.6 record projection, rebuilt from the decoded IR.
pub(crate) fn record_body(record: &MemoryRecordIr) -> Value {
    memory_record_to_wire(version(), record.clone())
}

/// §6.8 profile projection.
pub(crate) fn profile_body(profile: &MemoryProfileRow) -> Value {
    json!({
        "schema_version": SCHEMA_VERSION,
        "space_id": profile.space_id.as_str(),
        "subject_id": profile.subject_id.as_str(),
        "profile_kind": profile.profile_kind,
        "document": profile.document,
        "updated_by": profile.updated_by.as_str(),
        "created_at": profile.created_at,
        "updated_at": profile.updated_at,
        "revision": profile.revision,
        "extensions": {},
    })
}

/// §7.26/§7.27 envelope projection. `stale` is the read's own recomputation,
/// never the stored write-time cache.
pub(crate) fn reflection_body(reflection: &MemoryReflectionRow, stale: bool) -> Value {
    let mut body = json!({
        "reflection_id": reflection.reflection_id,
        "space_id": reflection.space_id.as_str(),
        "subject_id": reflection.subject_id.as_str(),
        "revision": reflection.revision,
        "schema_version": reflection.schema_version,
        "created_at": reflection.created_at,
        "selection": reflection.selection,
        "basis": reflection.basis,
        "body": { "body": reflection.body_text, "format": reflection.body_format },
        "generator": { "engine_policy_version": reflection.engine_policy_version },
        "extensions": reflection.extensions,
        "stale": stale,
    });
    let members = body
        .as_object_mut()
        .expect("reflection projection is an object");
    if let Some(filter) = &reflection.filter {
        members.insert("filter".to_string(), filter.clone());
    }
    if let Some(cognition_filter) = &reflection.cognition_filter {
        members.insert("cognition_filter".to_string(), cognition_filter.clone());
    }
    if let Some((unit_kind, units)) = &reflection.cost {
        members.insert(
            "cost".to_string(),
            json!({ "unit_kind": unit_kind, "units": units }),
        );
    }
    if let Some(model) = &reflection.model {
        if let Some(generator) = members.get_mut("generator").and_then(Value::as_object_mut) {
            generator.insert("model".to_string(), json!(model));
        }
    }
    body
}

/// §6.10 receipt projection, as every mutating op returns it.
pub(crate) fn receipt_body(receipt: &ReviewReceiptIr) -> Value {
    review_receipt_to_wire(version(), receipt.clone())
}
