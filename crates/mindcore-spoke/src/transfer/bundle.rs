//! §6.11 bundle projection: every member of `MemoryExportBundle` from one
//! borrowed-transaction snapshot, then the JCS digest seal.
//!
//! The bundle is a typed projection of the stored rows, never a table dump: each
//! scan is scoped to the authorized subject/space and selection, every array is
//! ordered by its stable row key, and unreadable stored data fails the export
//! instead of being replaced with an empty projection.

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use mindcore_base::ir::{
    CandidateState, MemoryGrantRow, MemoryRecordIr, MemoryReflectionRow, MemorySpaceRow,
    MemorySubjectRow, Placement, PlacementSelection, RecordState,
};
use mindcore_sqlite::MemoryReceiptSummary;

use crate::admission;
use crate::policy;
use crate::project;
use crate::reject::{AResult, Reject};
use crate::wire;

use super::{ExportSpec, SubjectSnapshot};

/// Transfer-format version; independent of the protocol and schema versions.
pub(super) const BUNDLE_FORMAT_VERSION: u64 = 1;

/// SHA-256 over JCS of `value` with `field` removed — the bundle and chunk seal.
pub(super) fn digest_excluding(value: &Value, field: &str) -> AResult<String> {
    match spoke_operations::jcs_sha256_hex_excluding(value, field) {
        spoke_operations::SpokeResult::Ok(digest) => Ok(digest),
        spoke_operations::SpokeResult::Reject(reject) => Err(Reject::invalid(reject.message)),
    }
}

/// A bundle-local opaque label for one principal. Salted with the export id, so the
/// same principal correlates across this bundle's grants and receipt summaries but
/// not with any other export or with the stored id.
fn principal_label(export_id: &str, principal: &str) -> AResult<String> {
    let digest = policy::digest(&json!({
        "export_id": export_id,
        "principal_id": principal,
    }))?;
    Ok(format!("prl_{}", &digest[..32]))
}

/// Project the sealed logical bundle, and validate it against the pinned schema
/// before it can leave the adapter.
pub(super) fn assemble(
    spec: &ExportSpec,
    space: &MemorySpaceRow,
    subjects: Vec<SubjectSnapshot>,
    grants: &[MemoryGrantRow],
    receipts: &[MemoryReceiptSummary],
) -> AResult<Value> {
    let whole_space = spec.subject_id.is_none();
    let include_profiles = whole_space || matches!(spec.selection, PlacementSelection::SubjectAll);
    let restricted: Option<Vec<String>> = match &spec.selection {
        PlacementSelection::SubjectAll => None,
        PlacementSelection::SharedOnly => Some(Vec::new()),
        PlacementSelection::SelectedContexts { context_ids, .. } => {
            Some(context_ids.iter().map(ToString::to_string).collect())
        }
    };

    let mut subjects = subjects;
    // why: an export of one subject still carries the row it names, even when the
    // store has no other subject.
    subjects.sort_by(|left, right| {
        left.subject
            .subject_id
            .as_str()
            .cmp(right.subject.subject_id.as_str())
    });

    let mut subject_values = Vec::with_capacity(subjects.len());
    let mut record_values = Vec::new();
    let mut candidate_values = Vec::new();
    let mut profile_values = Vec::new();
    let mut reflection_values = Vec::new();
    for snapshot in &mut subjects {
        subject_values.push(subject_value(&snapshot.subject, restricted.as_deref()));
        record_values.extend(snapshot.records.iter().map(project::record_body));
        candidate_values.extend(std::mem::take(&mut snapshot.candidates));
        if include_profiles {
            profile_values.extend(snapshot.profiles.iter().map(project::profile_body));
        }
        if spec.include_reflections {
            let profile_revisions = profile_revisions(&snapshot.profiles);
            for reflection in &snapshot.reflections {
                if let Some(value) =
                    reflection_value(reflection, &snapshot.records, &profile_revisions)?
                {
                    reflection_values.push(value);
                }
            }
        }
    }

    // A consumed candidate names the records its review produced. An id the export
    // does not carry would disclose an excluded row and dangle at the target, so the
    // list keeps only retained ids — possibly none, which is a valid empty array.
    let retained: std::collections::BTreeSet<String> = record_values
        .iter()
        .filter_map(|record| record["record_id"].as_str().map(str::to_string))
        .collect();
    let candidate_values: Vec<Value> = candidate_values
        .iter_mut()
        .map(|candidate| {
            if let CandidateState::Consumed { consumed_by, .. } = &mut candidate.state {
                consumed_by
                    .record_ids
                    .retain(|record_id| retained.contains(record_id));
            }
            project::candidate_body(candidate)
        })
        .collect();

    record_values.sort_by(|left, right| key(left, "record_id").cmp(key(right, "record_id")));
    let mut candidate_values = candidate_values;
    candidate_values
        .sort_by(|left, right| key(left, "candidate_id").cmp(key(right, "candidate_id")));
    profile_values.sort_by(|left, right| {
        (key(left, "subject_id"), key(left, "profile_kind"))
            .cmp(&(key(right, "subject_id"), key(right, "profile_kind")))
    });
    reflection_values
        .sort_by(|left, right| key(left, "reflection_id").cmp(key(right, "reflection_id")));

    let mut grant_values: Vec<Value> = grants
        .iter()
        .filter(|grant| grant_in_scope(grant, spec, spec.created_at))
        .map(|grant| grant_value(grant, spec))
        .collect::<AResult<Vec<_>>>()?;
    grant_values.sort_by(|left, right| key(left, "grant_id").cmp(key(right, "grant_id")));

    // A receipt row carries no subject or placement, so no summary can be scoped to
    // a subject without disclosing unrelated operations.
    let mut receipt_values = if whole_space {
        receipts
            .iter()
            .map(|receipt| receipt_value(receipt, &spec.export_id))
            .collect::<AResult<Vec<_>>>()?
    } else {
        Vec::new()
    };
    receipt_values.sort_by(|left, right| key(left, "receipt_id").cmp(key(right, "receipt_id")));

    let mut members = Map::new();
    members.insert("schema_version".to_string(), json!(project::SCHEMA_VERSION));
    members.insert(
        "bundle_format_version".to_string(),
        json!(BUNDLE_FORMAT_VERSION),
    );
    members.insert(
        "memory_protocol_version".to_string(),
        json!(policy::MEMORY_PROTOCOL_VERSION),
    );
    members.insert("export_id".to_string(), json!(spec.export_id));
    members.insert("space_id".to_string(), json!(space.space_id.as_str()));
    members.insert(
        "created_at".to_string(),
        json!(policy::timestamp(spec.created_at)),
    );
    members.insert(
        "source".to_string(),
        json!({
            "authority_host_id": space.authority_host_id,
            "authority_generation": space.authority_generation,
            "engine_policy_version": policy::ENGINE_POLICY_VERSION,
        }),
    );
    members.insert("subjects".to_string(), Value::Array(subject_values));
    members.insert("records".to_string(), Value::Array(record_values));
    members.insert("profiles".to_string(), Value::Array(profile_values));
    if spec.include_reflections {
        members.insert("reflections".to_string(), Value::Array(reflection_values));
    }
    members.insert("candidates".to_string(), Value::Array(candidate_values));
    members.insert(
        "receipt_summaries".to_string(),
        Value::Array(receipt_values),
    );
    members.insert("grants".to_string(), Value::Array(grant_values));
    members.insert("extensions".to_string(), json!({}));

    let mut bundle = Value::Object(members);
    let digest = digest_excluding(&bundle, "digest")?;
    bundle
        .as_object_mut()
        .expect("the bundle is an object")
        .insert("digest".to_string(), json!(digest));
    project::checked::<spoke_schemas::MemoryExportBundle>(bundle)
}

fn key<'a>(value: &'a Value, member: &str) -> &'a str {
    value[member].as_str().unwrap_or_default()
}

/// §6.3 subject projection; a placement-restricted export exposes only the fence
/// ids its selection admits.
fn subject_value(subject: &MemorySubjectRow, restricted: Option<&[String]>) -> Value {
    let mut body = project::subject_body(subject);
    if let Some(contexts) = restricted {
        body["fenced_contexts"] = json!(subject
            .fenced_contexts
            .iter()
            .map(ToString::to_string)
            .filter(|context| contexts.contains(context))
            .collect::<Vec<_>>());
    }
    body
}

fn profile_revisions(profiles: &[mindcore_base::ir::MemoryProfileRow]) -> policy::MapOfRevisions {
    let mut revisions: Vec<(String, i64)> = profiles
        .iter()
        .map(|profile| {
            (
                profile.profile_kind.clone(),
                profile.revision.as_i64().unwrap_or(0),
            )
        })
        .collect();
    revisions.sort();
    revisions
}

/// A reflection enters the bundle only when its pinned source set still verifies
/// against the live inputs. The local generator stores a digest and count, not a
/// record manifest, so a mismatch can mean a source was tombstoned after
/// generation — the body may derive deleted content and is withheld.
fn reflection_value(
    reflection: &MemoryReflectionRow,
    records: &[MemoryRecordIr],
    profile_revisions: &policy::MapOfRevisions,
) -> AResult<Option<Value>> {
    let pinned = wire::PlacementSelectionWire::from_json(&reflection.selection)?.ir();
    let mut filter_object = Map::new();
    if let Some(filter) = &reflection.filter {
        filter_object.insert("filter".to_string(), filter.clone());
    }
    let filter = wire::PlacementSelectionWire::filter(&filter_object, "filter")?;

    let mut live: Vec<(String, i64)> = records
        .iter()
        .filter(|record| matches!(record.state, RecordState::Live { .. }))
        .filter(|record| placement_admits(&pinned, &record.scope))
        .filter(|record| provenance_admits(filter.as_deref(), record))
        .map(|record| {
            (
                record.record_id.clone(),
                record.revision.as_i64().unwrap_or(0),
            )
        })
        .collect();
    live.sort();

    let declared = reflection
        .basis
        .get("source_set_digest")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if policy::reflection_source_set_digest(&live)? != declared {
        return Ok(None);
    }
    let current =
        policy::reflection_state_digest(&live, profile_revisions, &pinned, filter.as_deref())?;
    let stale =
        reflection.basis.get("state_digest").and_then(Value::as_str) != Some(current.as_str());
    Ok(Some(project::reflection_body(reflection, stale)))
}

fn placement_admits(selection: &PlacementSelection, placement: &Placement) -> bool {
    match (selection, placement) {
        (PlacementSelection::SubjectAll, _) => true,
        (PlacementSelection::SharedOnly, Placement::Shared) => true,
        (
            PlacementSelection::SelectedContexts {
                context_ids,
                include_shared,
            },
            placement,
        ) => match placement {
            Placement::Shared => *include_shared,
            Placement::Context(context_id) => {
                context_ids.iter().any(|id| id.as_str() == context_id)
            }
        },
        _ => false,
    }
}

fn provenance_admits(filter: Option<&[String]>, record: &MemoryRecordIr) -> bool {
    match filter {
        None => true,
        Some(contexts) => record
            .provenance
            .source_context_ids
            .iter()
            .any(|context| contexts.contains(context)),
    }
}

/// Live grants wholly inside the export scope: a grant is exported only when every
/// placement it covers is part of the bundle. An unrestricted grant or an unrelated
/// subject's grant is never described by a restricted selection.
fn grant_in_scope(grant: &MemoryGrantRow, spec: &ExportSpec, now: DateTime<Utc>) -> bool {
    if !admission::live_grant(grant, now) {
        return false;
    }
    if let Some(subject) = &spec.subject_id {
        if grant.subject_id.as_ref() != Some(subject) {
            return false;
        }
    }
    match &spec.selection {
        PlacementSelection::SubjectAll => true,
        PlacementSelection::SharedOnly => false,
        PlacementSelection::SelectedContexts {
            context_ids,
            include_shared,
        } => {
            !grant.context_ids.is_empty()
                && grant
                    .context_ids
                    .iter()
                    .all(|id| context_ids.iter().any(|selected| selected == id))
                && (!grant.shared_access || *include_shared)
        }
    }
}

/// §6.11 `GrantSummary`: the required summary plus the coverage facets the grant
/// actually carries, with an opaque principal label instead of the stored id.
fn grant_value(grant: &MemoryGrantRow, spec: &ExportSpec) -> AResult<Value> {
    let mut summary = json!({
        "grant_id": grant.grant_id,
        "principal_ref": principal_label(&spec.export_id, grant.principal_id.as_str())?,
        "capabilities": grant.capabilities,
        "revision": grant.revision,
    });
    let members = summary
        .as_object_mut()
        .expect("the grant summary is an object");
    if let Some(subject_id) = &grant.subject_id {
        members.insert("subject_id".to_string(), json!(subject_id.as_str()));
    }
    if !grant.context_ids.is_empty() {
        members.insert(
            "context_ids".to_string(),
            json!(grant
                .context_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()),
        );
        members.insert("shared_access".to_string(), json!(grant.shared_access));
    }
    Ok(summary)
}

/// §6.11 `ReceiptSummary`: content-free dedup provenance. The raw idempotency key is
/// omitted, and no result, error or lease fact crosses.
fn receipt_value(receipt: &MemoryReceiptSummary, export_id: &str) -> AResult<Value> {
    Ok(json!({
        "receipt_id": receipt.receipt_id,
        "op": receipt.operation,
        "requester_principal_ref": principal_label(export_id, &receipt.requester)?,
        "payload_digest": receipt.payload_digest,
        "status": receipt.status,
        "created_at": receipt.created_at,
    }))
}
