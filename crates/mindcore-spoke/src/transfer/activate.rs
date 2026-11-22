//! §7.32/§7.33 inbound transfer completion: one atomic visibility flip that
//! applies the verified staged bundle to the target space, and the
//! pre-activation rollback that deletes staging without applying anything.
//!
//! Activation rebuilds the bundle from the stored chunks, fixes the identity
//! mapping (subjects kept unless renamed, records and candidates fresh), applies
//! the ordinal-zero reference translations to transferred content, rechecks the
//! target it is about to write, and only then flips the import, releases the
//! staged payload and advances the space's authority generation.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Number, Value};
use spoke_operations::{GrantRequirement, SpokeRejectCode, SpokeResult};
use spoke_schemas::{
    MemoryImportsActivateRequest, MemoryImportsActivateResponse, MemoryImportsDiscardRequest,
    MemoryImportsDiscardResponse, Principal,
};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::wire::{memory_candidate_from_wire, memory_record_from_wire};
use mindcore_base::ir::{
    MemoryCandidateIr, MemoryGrantRow, MemoryProfileRow, MemoryRecordIr, MemoryReflectionRow,
    MemoryRepository, MemorySubjectRow,
};
use mindcore_sqlite::{MemoryImportRow, SqliteMemoryRepository};

use super::stage::{conflict, expired, expiry_conflict, space_requirement};
use crate::admission::{self, Admission, Coverage};
use crate::policy;
use crate::project;
use crate::reject::{settle, AResult, Reject};
use crate::wire;
use crate::{bump, mint, principal_id, MemoryAdapter, MemoryStage, Tx};

pub(crate) const ACTIVATE: &str = "memory.imports.activate";
pub(crate) const DISCARD: &str = "memory.imports.discard";

/// One cognition reference: the complete `(kind, namespace, id)` identity tuple.
type Reference = (String, String, String);

/// A caller who cannot read the target space sees the same generic absence as a
/// caller naming an import the store never held.
fn absent(reject: Reject, import_id: &str) -> Reject {
    match reject.code {
        SpokeRejectCode::NotAuthorized | SpokeRejectCode::NotFound => {
            Reject::not_found(format!("import {import_id}"))
        }
        _ => reject,
    }
}

/// The state an import has left, named so a second activation or discard is
/// distinguishable from a malformed request.
fn state_conflict(import: &MemoryImportRow) -> Reject {
    conflict(format!("import {} is {}", import.import_id, import.state))
        .detail("reason", json!("import_state"))
        .detail("import_id", json!(import.import_id))
        .detail("state", json!(import.state))
}

fn unreadable(what: &str) -> Reject {
    Reject::internal(format!("stored import {what} is unreadable"))
}

fn text<'a>(object: &'a Map<String, Value>, field: &str) -> AResult<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| unreadable(field))
}

fn member<'a>(value: &'a Value, field: &str) -> AResult<&'a Map<String, Value>> {
    value.as_object().ok_or_else(|| unreadable(field))
}

/// Every element of one bundle array; an absent array is empty.
fn members<'a>(bundle: &'a Value, field: &str) -> &'a [Value] {
    bundle
        .get(field)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn strings(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The reference a closed `{kind, namespace, id}` object carries, if it is one.
fn reference_of(object: &Map<String, Value>) -> Option<Reference> {
    if object.len() != 3 {
        return None;
    }
    let read = |field: &str| {
        object
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    Some((read("kind")?, read("namespace")?, read("id")?))
}

/// The ordinal-zero translation pairs, as reference tuples.
fn translations(value: &Value) -> AResult<Vec<(Reference, Reference)>> {
    let pairs = value.as_array().map(Vec::as_slice).unwrap_or_default();
    let mut out = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let object = member(pair, "reference translation")?;
        let from = object
            .get("from")
            .and_then(Value::as_object)
            .and_then(reference_of)
            .ok_or_else(|| unreadable("reference translation from"))?;
        let to = object
            .get("to")
            .and_then(Value::as_object)
            .and_then(reference_of)
            .ok_or_else(|| unreadable("reference translation to"))?;
        out.push((from, to));
    }
    Ok(out)
}

/// Apply the explicit translation pairs and the import's own id maps to every
/// typed reference position inside transferred content. A reference the map does
/// not name keeps its source identity rather than collapsing to nothing.
fn translate_references(
    value: &mut Value,
    translations: &[(Reference, Reference)],
    records: &BTreeMap<String, String>,
    subjects: &BTreeMap<String, String>,
) {
    match value {
        Value::Array(items) => {
            for item in items {
                translate_references(item, translations, records, subjects);
            }
        }
        Value::Object(object) => {
            let Some((kind, namespace, id)) = reference_of(object) else {
                for nested in object.values_mut() {
                    translate_references(nested, translations, records, subjects);
                }
                return;
            };
            let target = translations
                .iter()
                .find(|(from, _)| *from == (kind.clone(), namespace.clone(), id.clone()))
                .map(|(_, to)| to.clone())
                .or_else(|| {
                    let mapped = match kind.as_str() {
                        "memory_record" => records.get(&id),
                        "memory_subject" => subjects.get(&id),
                        _ => None,
                    }?;
                    Some((kind, namespace, mapped.clone()))
                });
            if let Some((kind, namespace, id)) = target {
                object.insert("kind".to_string(), json!(kind));
                object.insert("namespace".to_string(), json!(namespace));
                object.insert("id".to_string(), json!(id));
            }
        }
        _ => {}
    }
}

fn subject_row(value: &Value, space: &SpaceId, subject_id: &str) -> AResult<MemorySubjectRow> {
    let object = member(value, "subject")?;
    let kind = match object.get("kind") {
        None | Some(Value::Null) => None,
        Some(kind) => {
            let kind = kind.as_object().ok_or_else(|| unreadable("subject kind"))?;
            Some((text(kind, "ns")?.to_string(), text(kind, "id")?.to_string()))
        }
    };
    let fenced: &[Value] = object
        .get("fenced_contexts")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice);
    let mut fenced_contexts = Vec::with_capacity(fenced.len());
    for context in fenced {
        let context = context
            .as_str()
            .ok_or_else(|| unreadable("subject fenced context"))?;
        fenced_contexts.push(ContextId::new(context));
    }
    Ok(MemorySubjectRow {
        space_id: space.clone(),
        subject_id: SubjectId::new(subject_id),
        kind,
        status: text(object, "status")?.to_string(),
        fenced_contexts,
        revision: Number::from(1),
        created_at: text(object, "created_at")?.to_string(),
        extensions: Map::new(),
    })
}

fn mapped_subject(
    object: &Map<String, Value>,
    subjects: &BTreeMap<String, String>,
) -> AResult<String> {
    let source = text(object, "subject_id")?;
    subjects
        .get(source)
        .cloned()
        .ok_or_else(|| unreadable("subject mapping"))
}

/// What final verification freezes: the translated rows activation applies, and the
/// §7.31 report that fixes the identity mapping. Staging owns every decision here.
pub(super) struct Prepared {
    pub(super) payload: Value,
    pub(super) report: Value,
}

/// The translated rows a frozen `staged_payload_json` carries, decoded for the one
/// visibility flip. Activation never re-derives a mapping, a translation or a
/// collision decision; it rechecks these rows against the live target and applies them.
struct Staged {
    subjects: Vec<MemorySubjectRow>,
    records: Vec<MemoryRecordIr>,
    candidates: Vec<MemoryCandidateIr>,
    profiles: Vec<MemoryProfileRow>,
    reflections: Vec<MemoryReflectionRow>,
    grants: Vec<MemoryGrantRow>,
    receipt_summaries: Value,
}

/// Build the frozen result from the verified bundle: fix the identity mapping, translate
/// references, and recheck every target collision and adopted grant before anything is
/// written. Staging calls this once, at the final chunk.
#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    space: &SpaceId,
    import: &MemoryImportRow,
    bundle: &Value,
    caller_grants: &[MemoryGrantRow],
    principal: &str,
    now: DateTime<Utc>,
) -> AResult<Prepared> {
    // Subjects keep their identity unless the caller asked for a rename, in which
    // case a target collision mints a fresh one; `fail` refuses the collision.
    let mut subjects = Vec::new();
    let mut subject_map: BTreeMap<String, String> = BTreeMap::new();
    for value in members(bundle, "subjects") {
        let source = text(member(value, "subject")?, "subject_id")?.to_string();
        let target = if repository.subject(tx, space, &source).await?.is_some() {
            if import.conflict_policy == "rename_subject" {
                mint("sub")
            } else {
                return Err(
                    conflict(format!("the target space already holds subject {source}"))
                        .detail("reason", json!("subject_collision"))
                        .detail("subject_id", json!(source)),
                );
            }
        } else {
            source.clone()
        };
        subjects.push(subject_row(value, space, &target)?);
        subject_map.insert(source, target);
    }

    // Records and candidates always take fresh identities; their source ids stay
    // reachable through `import_ref` and the report.
    let mut record_map = BTreeMap::new();
    for value in members(bundle, "records") {
        let source = text(member(value, "record")?, "record_id")?.to_string();
        record_map.insert(source, mint("rec"));
    }
    let mut candidate_map = BTreeMap::new();
    for value in members(bundle, "candidates") {
        let source = text(member(value, "candidate")?, "candidate_id")?.to_string();
        candidate_map.insert(source, mint("cnd"));
    }
    let mut reflection_map = BTreeMap::new();
    for value in members(bundle, "reflections") {
        let source = text(member(value, "reflection")?, "reflection_id")?.to_string();
        reflection_map.insert(source, mint("rfl"));
    }
    let translations = translations(&import.reference_translations)?;

    let mut records = Vec::new();
    for value in members(bundle, "records") {
        let object = member(value, "record")?;
        let source = text(object, "record_id")?.to_string();
        let target = record_map
            .get(&source)
            .cloned()
            .ok_or_else(|| unreadable("record mapping"))?;
        let subject = mapped_subject(object, &subject_map)?;
        let source_revision = object.get("revision").cloned().unwrap_or_else(|| json!(1));
        let mut stored = value.clone();
        let fields = stored.as_object_mut().expect("a record object");
        fields.insert("record_id".to_string(), json!(target));
        fields.insert("subject_id".to_string(), json!(subject));
        fields.insert("revision".to_string(), json!(1));
        if let Some(content) = fields.get_mut("content") {
            if let Some(cognition) = content.get_mut("cognition") {
                translate_references(cognition, &translations, &record_map, &subject_map);
            }
        }
        // why: an `import_ref` already on the record is the identity an earlier
        // authority move gave it; §7.31 adopts it verbatim as historical attribution,
        // so this hop only stamps records that carry none.
        if !fields.contains_key("import_ref")
            && fields.contains_key("content")
            && !fields.contains_key("tombstone")
        {
            fields.insert(
                "import_ref".to_string(),
                json!({
                    "import_id": import.import_id,
                    "source_record_id": source,
                    "source_revision": source_revision,
                }),
            );
        }
        let (_, mut record) = memory_record_from_wire(stored)
            .map_err(|error| Reject::internal(format!("staged record: {error}")))?;
        record.space_id = space.as_str().to_string();
        records.push(record);
    }

    let mut candidates = Vec::new();
    for value in members(bundle, "candidates") {
        let object = member(value, "candidate")?;
        let source = text(object, "candidate_id")?.to_string();
        let target = candidate_map
            .get(&source)
            .cloned()
            .ok_or_else(|| unreadable("candidate mapping"))?;
        let mut stored = value.clone();
        let fields = stored.as_object_mut().expect("a candidate object");
        fields.insert("candidate_id".to_string(), json!(target));
        fields.insert(
            "subject_id".to_string(),
            json!(mapped_subject(object, &subject_map)?),
        );
        fields.insert("revision".to_string(), json!(1));
        if let Some(proposed) = fields.get_mut("proposed") {
            if let Some(content) = proposed.get_mut("content") {
                if let Some(cognition) = content.get_mut("cognition") {
                    translate_references(cognition, &translations, &record_map, &subject_map);
                }
            }
        }
        if let Some(ids) = fields
            .get_mut("consumed_by")
            .and_then(|consumed| consumed.get_mut("record_ids"))
            .and_then(Value::as_array_mut)
        {
            for id in ids.iter_mut() {
                if let Some(mapped) = id.as_str().and_then(|source| record_map.get(source)) {
                    *id = json!(mapped);
                }
            }
        }
        let (_, mut candidate) = memory_candidate_from_wire(stored)
            .map_err(|error| Reject::internal(format!("staged candidate: {error}")))?;
        candidate.space_id = space.as_str().to_string();
        candidates.push(candidate);
    }

    let mut profiles = Vec::new();
    for value in members(bundle, "profiles") {
        let object = member(value, "profile")?;
        let subject = mapped_subject(object, &subject_map)?;
        let kind = text(object, "profile_kind")?.to_string();
        if !repository
            .list_profiles(tx, space, &subject, std::slice::from_ref(&kind))
            .await?
            .is_empty()
        {
            return Err(conflict(format!(
                "the target space already holds profile {kind} of subject {subject}"
            ))
            .detail("reason", json!("profile_collision"))
            .detail("subject_id", json!(subject))
            .detail("profile_kind", json!(kind)));
        }
        profiles.push(MemoryProfileRow {
            space_id: space.clone(),
            subject_id: SubjectId::new(subject),
            profile_kind: kind,
            document: object
                .get("document")
                .cloned()
                .ok_or_else(|| unreadable("profile document"))?,
            updated_by: CallerId::new(text(object, "updated_by")?),
            revision: Number::from(1),
            created_at: text(object, "created_at")?.to_string(),
            updated_at: text(object, "updated_at")?.to_string(),
            extensions: Map::new(),
        });
    }

    // Imported reflections keep their pinned basis as historical provenance and
    // carry the write-time stale hint: the target's live inputs are not the ones
    // the source derived from.
    let mut reflections = Vec::new();
    for value in members(bundle, "reflections") {
        let object = member(value, "reflection")?;
        let source = text(object, "reflection_id")?.to_string();
        let generator = object
            .get("generator")
            .and_then(Value::as_object)
            .ok_or_else(|| unreadable("reflection generator"))?;
        let body = object
            .get("body")
            .and_then(Value::as_object)
            .ok_or_else(|| unreadable("reflection body"))?;
        let cost = match object.get("cost") {
            Some(cost) => {
                let cost = cost
                    .as_object()
                    .ok_or_else(|| unreadable("reflection cost"))?;
                Some((
                    text(cost, "unit_kind")?.to_string(),
                    cost.get("units")
                        .and_then(Value::as_f64)
                        .ok_or_else(|| unreadable("reflection cost units"))?,
                ))
            }
            None => None,
        };
        reflections.push(MemoryReflectionRow {
            seq: 0,
            reflection_id: reflection_map
                .get(&source)
                .cloned()
                .ok_or_else(|| unreadable("reflection mapping"))?,
            space_id: space.clone(),
            subject_id: SubjectId::new(mapped_subject(object, &subject_map)?),
            requester: CallerId::new(principal),
            selection: object
                .get("selection")
                .cloned()
                .ok_or_else(|| unreadable("reflection selection"))?,
            filter: object.get("filter").cloned(),
            cognition_filter: object.get("cognition_filter").cloned(),
            basis: object
                .get("basis")
                .cloned()
                .ok_or_else(|| unreadable("reflection basis"))?,
            engine_policy_version: text(generator, "engine_policy_version")?.to_string(),
            model: generator
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string),
            schema_version: object
                .get("schema_version")
                .and_then(Value::as_i64)
                .ok_or_else(|| unreadable("reflection schema_version"))?,
            revision: Number::from(1),
            extensions: object
                .get("extensions")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            created_at: text(object, "created_at")?.to_string(),
            body_text: text(body, "body")?.to_string(),
            body_format: text(body, "format")?.to_string(),
            cost,
            stale_cache: true,
        });
    }

    // Adoption creates only the grants the caller listed, and never one wider
    // than the caller's own current coverage: no unlisted grant and no
    // amplification, and a non-expiring import needs a non-expiring cover.
    let summaries = members(bundle, "grants");
    let mut grants = Vec::new();
    let mut grant_map = BTreeMap::new();
    let mut adopted = BTreeSet::new();
    let mut dropped = 0_u64;
    for value in import
        .adopt_grants
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
    {
        let value = member(value, "adopted grant")?;
        let grant_id = text(value, "grant_id")?.to_string();
        if !adopted.insert(grant_id.clone()) {
            dropped += 1;
            continue;
        }
        let target_principal = text(value, "principal_id")?.to_string();
        let summary = summaries
            .iter()
            .filter_map(Value::as_object)
            .find(|summary| summary.get("grant_id").and_then(Value::as_str) == Some(&grant_id))
            .ok_or_else(|| {
                conflict(format!(
                    "adopted grant {grant_id} is not a grant summary in the staged bundle"
                ))
                .detail("reason", json!("verification"))
            })?;
        let capabilities = strings(summary.get("capabilities"));
        let context_ids = strings(summary.get("context_ids"));
        let shared_access = summary
            .get("shared_access")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let subject = match summary.get("subject_id").and_then(Value::as_str) {
            Some(subject) => Some(SubjectId::new(
                subject_map
                    .get(subject)
                    .cloned()
                    .ok_or_else(|| unreadable("grant subject mapping"))?,
            )),
            None => None,
        };
        // The row keeps the source identity until the coverage check has named it.
        grants.push(MemoryGrantRow {
            grant_id: grant_id.clone(),
            space_id: space.clone(),
            subject_id: subject,
            principal_id: CallerId::new(target_principal),
            capabilities,
            context_ids: context_ids.into_iter().map(ContextId::new).collect(),
            shared_access,
            expires_at: None,
            revoked_at: None,
            revision: Number::from(1),
            created_at: policy::timestamp(now),
            extensions: Map::new(),
        });
    }
    not_subsumed(&grants, caller_grants, space, principal, now)?;
    // `grant_id` is a store-wide primary key, so an adopted grant takes a fresh
    // target identity like any imported row; the report keeps the mapping.
    for grant in &mut grants {
        let target = mint("grt");
        grant_map.insert(grant.grant_id.clone(), target.clone());
        grant.grant_id = target;
    }

    let source = bundle
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| unreadable("bundle source"))?;
    let host = text(source, "authority_host_id")?;
    let source_space = bundle
        .get("space_id")
        .and_then(Value::as_str)
        .ok_or_else(|| unreadable("bundle space_id"))?;
    let mut seen = BTreeSet::new();
    let mut receipt_summaries = Vec::new();
    for value in members(bundle, "receipt_summaries") {
        let value = member(value, "receipt summary")?;
        let receipt_id = text(value, "receipt_id")?.to_string();
        if !seen.insert(receipt_id.clone()) {
            continue;
        }
        receipt_summaries.push(json!({
            "authority_host_id": host,
            "space_id": source_space,
            "receipt_id": receipt_id,
            "op": text(value, "op")?,
            "payload_digest": text(value, "payload_digest")?,
            "status": text(value, "status")?,
            "created_at": text(value, "created_at")?,
        }));
    }
    let receipt_summaries = Value::Array(receipt_summaries);

    let counts = json!({
        "candidates": candidates.len(),
        "profiles": profiles.len(),
        "receipt_summaries": receipt_summaries.as_array().map_or(0, Vec::len),
        "records": records.len(),
        "reflections": reflections.len(),
        "subjects": subjects.len(),
    });
    // §7.31 `StagedReport`: the fixed identity mapping and what the sequence verified.
    let report = json!({
        "candidate_id_map": candidate_map,
        "counts": counts,
        "grants_adopted": grants.len() as u64,
        "grants_dropped": dropped,
        "record_id_map": record_map,
        "subject_id_map": subject_map,
        "verification": { "bundle_digest_ok": true, "errors": [] },
    });
    // The translated rows to apply, plus the content-free source provenance the
    // activation retains. Every id in here is final; activation decodes and rechecks it.
    let payload = json!({
        "candidates": candidates.iter().map(project::candidate_body).collect::<Vec<_>>(),
        "grants": grants.iter().map(project::grant_body).collect::<Vec<_>>(),
        "profiles": profiles.iter().map(project::profile_body).collect::<Vec<_>>(),
        "receipt_summaries": receipt_summaries,
        "records": records.iter().map(project::record_body).collect::<Vec<_>>(),
        "reflections": reflections.iter().map(|row| project::reflection_body(row, true)).collect::<Vec<_>>(),
        "subjects": subjects.iter().map(project::subject_body).collect::<Vec<_>>(),
    });

    Ok(Prepared { payload, report })
}

/// No adopted grant may be wider than the caller's own current coverage: no
/// amplification, and a non-expiring import needs a non-expiring cover. Staging and
/// activation both run it — the grants become permanent only at activation, so the cover
/// is re-derived there rather than trusted from the frozen import.
fn not_subsumed(
    grants: &[MemoryGrantRow],
    caller_grants: &[MemoryGrantRow],
    space: &SpaceId,
    principal: &str,
    now: DateTime<Utc>,
) -> AResult<()> {
    let caller_wire = admission::wire_grants(caller_grants)?;
    for grant in grants {
        for capability in &grant.capabilities {
            let requirement = GrantRequirement {
                capability,
                subject_id: grant.subject_id.as_ref().map(SubjectId::as_str),
                context_ids: grant.context_ids.iter().map(ToString::to_string).collect(),
                reads_shared: grant.shared_access || grant.context_ids.is_empty(),
                subject_all: grant.context_ids.is_empty(),
            };
            let covered = caller_grants.iter().zip(&caller_wire).any(|(row, wire)| {
                row.expires_at.is_none()
                    && spoke_operations::grant_covers(
                        wire,
                        principal,
                        space.as_str(),
                        now,
                        &requirement,
                    )
            });
            if !covered {
                return Err(conflict(format!(
                    "adopted grant {} would amplify coverage the caller does not hold",
                    grant.grant_id
                ))
                .detail("reason", json!("grant_not_subsumed"))
                .detail("grant_id", json!(grant.grant_id))
                .detail("capability", json!(capability)));
            }
        }
    }
    Ok(())
}

/// One bundle array of the frozen payload; an absent array is a defect, not empty.
fn rows<'a>(payload: &'a Value, field: &str) -> AResult<&'a [Value]> {
    payload
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| unreadable(field))
}

fn count_of(value: Option<&Value>, field: &str) -> AResult<Number> {
    value
        .and_then(Value::as_number)
        .cloned()
        .ok_or_else(|| unreadable(field))
}

/// Decode the frozen translated rows. The payload is trusted only because staging
/// wrote it in the same store; a row it cannot read is an internal defect, never a
/// caller-visible refusal.
fn decode_staged(payload: &Value, space: &SpaceId, requester: &str) -> AResult<Staged> {
    let subjects = rows(payload, "subjects")?
        .iter()
        .map(|value| stored_subject(value, space))
        .collect::<AResult<Vec<_>>>()?;
    let records = rows(payload, "records")?
        .iter()
        .map(|value| stored_record(value, space))
        .collect::<AResult<Vec<_>>>()?;
    let candidates = rows(payload, "candidates")?
        .iter()
        .map(|value| stored_candidate(value, space))
        .collect::<AResult<Vec<_>>>()?;
    let profiles = rows(payload, "profiles")?
        .iter()
        .map(|value| stored_profile(value, space))
        .collect::<AResult<Vec<_>>>()?;
    let reflections = rows(payload, "reflections")?
        .iter()
        .map(|value| stored_reflection(value, space, requester))
        .collect::<AResult<Vec<_>>>()?;
    let grants = rows(payload, "grants")?
        .iter()
        .map(|value| stored_grant(value, space))
        .collect::<AResult<Vec<_>>>()?;
    let receipt_summaries = payload
        .get("receipt_summaries")
        .cloned()
        .ok_or_else(|| unreadable("receipt_summaries"))?;
    Ok(Staged {
        subjects,
        records,
        candidates,
        profiles,
        reflections,
        grants,
        receipt_summaries,
    })
}

fn stored_subject(value: &Value, space: &SpaceId) -> AResult<MemorySubjectRow> {
    let object = value.as_object().ok_or_else(|| unreadable("subject"))?;
    let kind = match object.get("kind") {
        None | Some(Value::Null) => None,
        Some(kind) => {
            let kind = kind.as_object().ok_or_else(|| unreadable("subject kind"))?;
            Some((text(kind, "ns")?.to_string(), text(kind, "id")?.to_string()))
        }
    };
    Ok(MemorySubjectRow {
        space_id: space.clone(),
        subject_id: SubjectId::new(text(object, "subject_id")?),
        kind,
        status: text(object, "status")?.to_string(),
        fenced_contexts: strings(object.get("fenced_contexts"))
            .into_iter()
            .map(ContextId::new)
            .collect(),
        revision: count_of(object.get("revision"), "subject revision")?,
        created_at: text(object, "created_at")?.to_string(),
        extensions: Map::new(),
    })
}

fn stored_record(value: &Value, space: &SpaceId) -> AResult<MemoryRecordIr> {
    let (_, mut record) = memory_record_from_wire(value.clone())
        .map_err(|error| Reject::internal(format!("staged record: {error}")))?;
    record.space_id = space.as_str().to_string();
    Ok(record)
}

fn stored_candidate(value: &Value, space: &SpaceId) -> AResult<MemoryCandidateIr> {
    let (_, mut candidate) = memory_candidate_from_wire(value.clone())
        .map_err(|error| Reject::internal(format!("staged candidate: {error}")))?;
    candidate.space_id = space.as_str().to_string();
    Ok(candidate)
}

fn stored_profile(value: &Value, space: &SpaceId) -> AResult<MemoryProfileRow> {
    let object = value.as_object().ok_or_else(|| unreadable("profile"))?;
    Ok(MemoryProfileRow {
        space_id: space.clone(),
        subject_id: SubjectId::new(text(object, "subject_id")?),
        profile_kind: text(object, "profile_kind")?.to_string(),
        document: object
            .get("document")
            .cloned()
            .ok_or_else(|| unreadable("profile document"))?,
        updated_by: CallerId::new(text(object, "updated_by")?),
        revision: count_of(object.get("revision"), "profile revision")?,
        created_at: text(object, "created_at")?.to_string(),
        updated_at: text(object, "updated_at")?.to_string(),
        extensions: Map::new(),
    })
}

fn stored_reflection(
    value: &Value,
    space: &SpaceId,
    requester: &str,
) -> AResult<MemoryReflectionRow> {
    let object = value.as_object().ok_or_else(|| unreadable("reflection"))?;
    let generator = object
        .get("generator")
        .and_then(Value::as_object)
        .ok_or_else(|| unreadable("reflection generator"))?;
    let body = object
        .get("body")
        .and_then(Value::as_object)
        .ok_or_else(|| unreadable("reflection body"))?;
    let cost = match object.get("cost") {
        None | Some(Value::Null) => None,
        Some(cost) => {
            let cost = cost
                .as_object()
                .ok_or_else(|| unreadable("reflection cost"))?;
            Some((
                text(cost, "unit_kind")?.to_string(),
                cost.get("units")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| unreadable("reflection cost units"))?,
            ))
        }
    };
    Ok(MemoryReflectionRow {
        seq: 0,
        reflection_id: text(object, "reflection_id")?.to_string(),
        space_id: space.clone(),
        subject_id: SubjectId::new(text(object, "subject_id")?),
        requester: CallerId::new(requester),
        selection: object
            .get("selection")
            .cloned()
            .ok_or_else(|| unreadable("reflection selection"))?,
        filter: object.get("filter").cloned(),
        cognition_filter: object.get("cognition_filter").cloned(),
        basis: object
            .get("basis")
            .cloned()
            .ok_or_else(|| unreadable("reflection basis"))?,
        engine_policy_version: text(generator, "engine_policy_version")?.to_string(),
        model: generator
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string),
        schema_version: object
            .get("schema_version")
            .and_then(Value::as_i64)
            .ok_or_else(|| unreadable("reflection schema_version"))?,
        revision: count_of(object.get("revision"), "reflection revision")?,
        extensions: object
            .get("extensions")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default(),
        created_at: text(object, "created_at")?.to_string(),
        body_text: text(body, "body")?.to_string(),
        body_format: text(body, "format")?.to_string(),
        cost,
        // why: the target's live inputs are not the ones the source derived from.
        stale_cache: true,
    })
}

fn stored_grant(value: &Value, space: &SpaceId) -> AResult<MemoryGrantRow> {
    let object = value.as_object().ok_or_else(|| unreadable("grant"))?;
    let timestamp = |field: &str| {
        object
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    Ok(MemoryGrantRow {
        grant_id: text(object, "grant_id")?.to_string(),
        space_id: space.clone(),
        subject_id: object
            .get("subject_id")
            .and_then(Value::as_str)
            .map(SubjectId::new),
        principal_id: CallerId::new(text(object, "principal_id")?),
        capabilities: strings(object.get("capabilities")),
        context_ids: strings(object.get("context_ids"))
            .into_iter()
            .map(ContextId::new)
            .collect(),
        shared_access: object
            .get("shared_access")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        expires_at: timestamp("expires_at"),
        revoked_at: timestamp("revoked_at"),
        revision: count_of(object.get("revision"), "grant revision")?,
        created_at: text(object, "created_at")?.to_string(),
        extensions: Map::new(),
    })
}

/// Recheck the frozen rows against the live target before the flip: a subject or a
/// profile the target gained since staging refuses the activation instead of surfacing
/// as a storage failure. The mapping itself is never re-derived; adopted-grant coverage
/// is a separate check against the activating caller's current grants.
async fn recheck(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    space: &SpaceId,
    staged: &Staged,
) -> AResult<()> {
    for subject in &staged.subjects {
        let subject_id = subject.subject_id.as_str();
        if repository.subject(tx, space, subject_id).await?.is_some() {
            return Err(conflict(format!(
                "the target space already holds subject {subject_id}"
            ))
            .detail("reason", json!("subject_collision"))
            .detail("subject_id", json!(subject_id)));
        }
    }
    for profile in &staged.profiles {
        let kind = std::slice::from_ref(&profile.profile_kind);
        if !repository
            .list_profiles(tx, space, profile.subject_id.as_str(), kind)
            .await?
            .is_empty()
        {
            return Err(conflict(format!(
                "the target space already holds profile {} of subject {}",
                profile.profile_kind,
                profile.subject_id.as_str()
            ))
            .detail("reason", json!("profile_collision"))
            .detail("subject_id", json!(profile.subject_id.as_str()))
            .detail("profile_kind", json!(profile.profile_kind)));
        }
    }
    Ok(())
}

impl MemoryAdapter {
    // contract §7.32 imports.activate

    pub async fn activate_import(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.activate_import_body(tx, principal, request).await)
    }

    async fn activate_import_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryImportsActivateRequest =
            wire::decode(&raw, "memory.imports.activate request")?;
        let object = wire::object(&raw, "memory.imports.activate request")?;
        let principal_id = principal_id(principal);
        let space_id = wire::required(object, "space_id")?;
        let idempotency_key = wire::required(object, "idempotency_key")?;
        let import_id = request.import_id.to_string();
        let op = Self::op(
            space_id,
            principal_id,
            ACTIVATE,
            Some(idempotency_key),
            &raw,
        )?;
        let admission = Admission {
            operation: ACTIVATE,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(vec![
                space_requirement("transfer"),
                space_requirement("admin"),
            ]),
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            // why: the transfer family is on the §8.2 freeze allowlist, so an import
            // applies into a frozen space as a maintenance path.
            freeze_gated: false,
        };
        if let Err(reject) = admission::authorize(&self.repository, tx, &admission, op.now).await {
            return Err(absent(reject, &import_id));
        }
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryImportsActivateResponse>(&receipt);
        }
        self.repository
            .expire_imports(tx, &op.space, &policy::timestamp(op.now))
            .await?;

        let Some(import) = self.repository.import(tx, &op.space, &import_id).await? else {
            return Err(Reject::not_found(format!("import {import_id}")));
        };
        if expired(&import, op.now)? {
            return Err(expiry_conflict(&import));
        }
        if import.state != "staged" {
            return Err(state_conflict(&import));
        }

        // The frozen payload is the applied source: staging fixed the mapping, the
        // translations and the report, and released the raw chunks. Activation
        // decodes those rows and rechecks them against the live target.
        let Some(payload) = import.staged_payload.clone() else {
            return Err(Reject::internal(
                "the staged import carries no translated rows",
            ));
        };
        let staged = decode_staged(&payload, &op.space, principal_id)?;
        recheck(&self.repository, tx, &op.space, &staged).await?;
        // why: an adopted grant is written without an expiry, so the cover that justified it at
        // staging is re-derived from the activating caller's current grants — a live but expiring
        // `admin` at cutover must not install one permanently.
        let caller_grants = admission::grants(&self.repository, tx, &op.space).await?;
        not_subsumed(
            &staged.grants,
            &caller_grants,
            &op.space,
            principal_id,
            op.now,
        )?;

        let space_row = admission::space(&self.repository, tx, &op.space).await?;
        let observed = space_row.revision.clone();
        let generation = space_row.authority_generation.saturating_add(1);

        for row in &staged.subjects {
            self.repository.insert_subject(tx, row).await?;
        }
        for row in &staged.records {
            self.repository.insert_record(tx, row).await?;
        }
        for row in &staged.candidates {
            self.repository.insert_candidate(tx, row).await?;
        }
        for row in &staged.profiles {
            self.repository.insert_profile(tx, row).await?;
        }
        for row in &staged.reflections {
            self.repository.insert_reflection(tx, row).await?;
        }
        for row in &staged.grants {
            self.repository.insert_grant(tx, row).await?;
        }

        let provenance = serde_json::to_string(&staged.receipt_summaries)
            .map_err(|error| Reject::internal(format!("import provenance: {error}")))?;
        if !self
            .repository
            .mark_import_activated(tx, &import_id, &policy::timestamp(op.now), &provenance)
            .await?
        {
            // Another activation won the same import: refuse rather than apply twice.
            return Err(state_conflict(&import));
        }

        let mut flipped = space_row;
        flipped.authority_generation = generation;
        flipped.revision = bump(&observed);
        if !self
            .repository
            .update_space(tx, &flipped, &observed)
            .await?
        {
            return Err(
                conflict("the target space revision moved under the activation")
                    .detail("reason", json!("space_revision")),
            );
        }

        let counts = json!({
            "candidates": staged.candidates.len(),
            "profiles": staged.profiles.len(),
            "receipt_summaries": staged
                .receipt_summaries
                .as_array()
                .map_or(0, Vec::len),
            "records": staged.records.len(),
            "reflections": staged.reflections.len(),
            "subjects": staged.subjects.len(),
        });
        let result = json!({
            "activated": counts,
            "authority_generation": u64::try_from(generation).unwrap_or(1),
            "import_id": import_id,
        });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemoryImportsActivateResponse>(project::receipt_body(&receipt))
    }

    // contract §7.33 imports.discard

    pub async fn discard_import(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.discard_import_body(tx, principal, request).await)
    }

    async fn discard_import_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryImportsDiscardRequest =
            wire::decode(&raw, "memory.imports.discard request")?;
        let object = wire::object(&raw, "memory.imports.discard request")?;
        let principal_id = principal_id(principal);
        let space_id = wire::required(object, "space_id")?;
        let idempotency_key = wire::required(object, "idempotency_key")?;
        let import_id = request.import_id.to_string();
        let op = Self::op(space_id, principal_id, DISCARD, Some(idempotency_key), &raw)?;
        let admission = Admission {
            operation: DISCARD,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(vec![
                space_requirement("transfer"),
                space_requirement("admin"),
            ]),
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: false,
        };
        if let Err(reject) = admission::authorize(&self.repository, tx, &admission, op.now).await {
            return Err(absent(reject, &import_id));
        }
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryImportsDiscardResponse>(&receipt);
        }
        self.repository
            .expire_imports(tx, &op.space, &policy::timestamp(op.now))
            .await?;

        let Some(import) = self.repository.import(tx, &op.space, &import_id).await? else {
            return Err(Reject::not_found(format!("import {import_id}")));
        };
        if !self
            .repository
            .mark_import_discarded(tx, &import_id)
            .await?
        {
            return Err(state_conflict(&import));
        }
        self.repository.delete_import_chunks(tx, &import_id).await?;

        let result = json!({ "discarded": true, "import_id": import_id });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemoryImportsDiscardResponse>(project::receipt_body(&receipt))
    }
}
