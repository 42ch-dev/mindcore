//! §7.31/§7.32 inbound transfer: strictly ordered chunk staging, the whole-sequence
//! verification the final chunk runs, and the fixed staged-import expiry.
//!
//! A chunk is durable on its own: ordinal zero writes the import header, and every
//! later ordinal advances it. Nothing is activated here — a verified sequence stops
//! at `staged`.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use mindcore_base::ir::MemoryRepository;
use mindcore_sqlite::MemoryImportRow;
use serde_json::{json, Value};
use spoke_operations::{
    validate_memory_imports_stage, GrantRequirement, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{
    MemoryExportBundle, MemoryImportsStageRequest, MemoryImportsStageResponse, Principal,
};

use super::bundle::{digest_excluding, BUNDLE_FORMAT_VERSION};
use super::canonical_len;
use super::chunk::BUNDLE_ARRAY_FIELDS;
use crate::admission::{self, Admission, Coverage};
use crate::policy;
use crate::project;
use crate::reject::{settle, AResult, Reject};
use crate::wire;
use crate::{mint, principal_id, MemoryAdapter, MemoryStage, Tx};

const STAGE: &str = "memory.imports.stage";

/// The space-wide coverage every import mutation needs.
pub(super) fn space_requirement(capability: &str) -> GrantRequirement<'_> {
    GrantRequirement {
        capability,
        subject_id: None,
        context_ids: Vec::new(),
        reads_shared: false,
        subject_all: true,
    }
}

fn validated(result: SpokeResult<()>) -> AResult<()> {
    match result {
        SpokeResult::Ok(()) => Ok(()),
        SpokeResult::Reject(reject) => Err(Reject::terminal(reject.code, reject.message)),
    }
}

pub(super) fn conflict(message: impl Into<String>) -> Reject {
    Reject::terminal(SpokeRejectCode::ImportConflict, message)
}

/// The one absence answer a staging call may give: it names no import, no ordinal
/// and no expiry, so a caller who may not read the source export learns nothing.
pub(super) fn no_staging_import(source_export_id: &str) -> Reject {
    conflict(format!(
        "no staging import holds source export {source_export_id}"
    ))
    .detail("reason", json!("no_staging_import"))
}

/// A verification refusal names every failed check, so one attempt reports the
/// whole broken sequence rather than its first symptom.
pub(super) fn unverified(errors: Vec<String>) -> Reject {
    conflict("the staged import sequence does not verify")
        .detail("reason", json!("verification"))
        .detail("errors", json!(errors))
}

pub(super) fn expired(row: &MemoryImportRow, now: DateTime<Utc>) -> AResult<bool> {
    let expires = DateTime::parse_from_rfc3339(&row.expires_at)
        .map_err(|_| Reject::internal("stored import expiry is unreadable"))?
        .with_timezone(&Utc);
    Ok(now >= expires)
}

pub(super) fn expiry_conflict(row: &MemoryImportRow) -> Reject {
    conflict(format!("import {} has expired", row.import_id))
        .detail("reason", json!("expired"))
        .detail("import_id", json!(row.import_id))
        .detail("expires_at", json!(row.expires_at))
}

/// The per-chunk checks every accepted ordinal passes before it is written.
fn chunk_errors(
    chunk: &Value,
    ordinal: u64,
    expected_count: u64,
    bundle_digest: &str,
    source_export_id: &str,
) -> Vec<String> {
    let mut errors = Vec::new();
    if chunk.get("chunk_ordinal").and_then(Value::as_u64) != Some(ordinal) {
        errors.push(format!(
            "chunk {ordinal}: chunk_ordinal does not name this ordinal"
        ));
    }
    if chunk.get("chunk_count").and_then(Value::as_u64) != Some(expected_count) {
        errors.push(format!(
            "chunk {ordinal}: chunk_count does not match the import's {expected_count} chunks"
        ));
    }
    if chunk.get("bundle_digest").and_then(Value::as_str) != Some(bundle_digest) {
        errors.push(format!(
            "chunk {ordinal}: bundle_digest does not match the import's bundle"
        ));
    }
    if chunk.get("export_id").and_then(Value::as_str) != Some(source_export_id) {
        errors.push(format!(
            "chunk {ordinal}: export_id does not name the staged source export"
        ));
    }
    match (
        chunk.get("chunk_digest").and_then(Value::as_str),
        digest_excluding(chunk, "chunk_digest"),
    ) {
        (Some(published), Ok(computed)) if published == computed => {}
        _ => errors.push(format!("chunk {ordinal}: chunk_digest does not verify")),
    }
    match (
        chunk.get("bytes").and_then(Value::as_u64),
        canonical_len(chunk),
    ) {
        (Some(bytes), Ok(length)) if bytes == length as u64 => {}
        _ => errors.push(format!("chunk {ordinal}: bytes is not the chunk's length")),
    }
    errors
}

/// The logical bundle a complete chunk sequence carries: the ordinal-zero header
/// plus every array in its fixed concatenation order.
pub(super) fn reassemble(sequence: &[Value]) -> Option<Value> {
    let mut bundle = sequence.first()?.get("header")?.as_object()?.clone();
    // why: the wire header omits an empty `extensions`, while the logical bundle the
    // sender digested always carries it; restoring `{}` keeps the digest and the
    // payload schema in agreement.
    bundle
        .entry("extensions".to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    for field in BUNDLE_ARRAY_FIELDS {
        if !sequence.iter().any(|chunk| chunk.get(field).is_some()) {
            continue;
        }
        let items: Vec<Value> = sequence
            .iter()
            .filter_map(|chunk| chunk.get(field).and_then(Value::as_array))
            .flatten()
            .cloned()
            .collect();
        bundle.insert(field.to_string(), Value::Array(items));
    }
    Some(Value::Object(bundle))
}

fn key_set<'a>(bundle: &'a Value, field: &str, member: &str) -> BTreeSet<&'a str> {
    bundle[field]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item.get(member).and_then(Value::as_str))
        .collect()
}

/// `GrantSummary.principal_ref` is an opaque correlation label — "never credentials
/// or tokens" (`spoke-schemas` 0.17.0 `PrincipalRef`). The published contract states
/// only a minimum length, so the screen is our own emission grammar: `prl_` plus 32
/// lowercase hex digits. Anything else carries key material, a raw principal id or a
/// foreign token, and is refused rather than echoed.
fn correlation_label(value: &str) -> bool {
    value.strip_prefix("prl_").is_some_and(|label| {
        label.len() == 32
            && label
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// References and grant summaries: every cross-reference resolves inside the
/// bundle, and an adopted grant is one the bundle actually carries.
fn reference_errors(bundle: &Value, adopted: &Value) -> Vec<String> {
    let mut errors = Vec::new();
    let subjects = key_set(bundle, "subjects", "subject_id");
    let records = key_set(bundle, "records", "record_id");

    for field in ["records", "profiles", "candidates", "reflections", "grants"] {
        for item in bundle[field].as_array().into_iter().flatten() {
            let Some(subject) = item.get("subject_id").and_then(Value::as_str) else {
                continue;
            };
            if !subjects.contains(subject) {
                errors.push(format!(
                    "{field} names subject {subject} that the bundle does not carry"
                ));
            }
        }
    }

    let mut grant_ids = BTreeSet::new();
    for grant in bundle["grants"].as_array().into_iter().flatten() {
        if !grant
            .get("principal_ref")
            .and_then(Value::as_str)
            .is_some_and(correlation_label)
        {
            errors.push(
                "a grant summary carries a principal_ref that is not an opaque correlation label"
                    .to_string(),
            );
        }
        let Some(grant_id) = grant.get("grant_id").and_then(Value::as_str) else {
            continue;
        };
        if !grant_ids.insert(grant_id) {
            errors.push(format!("grant {grant_id} appears more than once"));
        }
    }
    for candidate in bundle["candidates"].as_array().into_iter().flatten() {
        let produced = candidate
            .get("consumed_by")
            .and_then(|consumed| consumed.get("record_ids"))
            .and_then(Value::as_array);
        for record_id in produced.into_iter().flatten().filter_map(Value::as_str) {
            if !records.contains(record_id) {
                errors.push(format!(
                    "candidate names record {record_id} that the bundle does not carry"
                ));
            }
        }
    }
    for grant_id in adopted
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|grant| grant.get("grant_id").and_then(Value::as_str))
    {
        if !grant_ids.contains(grant_id) {
            errors.push(format!(
                "adopted grant {grant_id} is not a grant summary in the bundle"
            ));
        }
    }
    errors
}

/// Verify the whole stored sequence against the header facts ordinal zero fixed:
/// every chunk's own digest, the reassembled bundle digest, the three versions,
/// the payload schema, references and grant summaries.
pub(super) fn verification_errors(
    sequence: &[Value],
    expected_count: u64,
    bundle_digest: &str,
    source_export_id: &str,
    adopted: &Value,
) -> Vec<String> {
    if sequence.len() as u64 != expected_count {
        return vec![format!(
            "the sequence holds {} chunks, expected {expected_count}",
            sequence.len()
        )];
    }
    let mut errors = Vec::new();
    for (ordinal, chunk) in sequence.iter().enumerate() {
        errors.extend(chunk_errors(
            chunk,
            ordinal as u64,
            expected_count,
            bundle_digest,
            source_export_id,
        ));
    }

    let Some(bundle) = reassemble(sequence) else {
        errors.push("chunk 0 carries no logical header".to_string());
        return errors;
    };
    if bundle.get("digest").and_then(Value::as_str) != Some(bundle_digest) {
        errors.push("the header digest does not match the import's bundle digest".to_string());
    }
    match digest_excluding(&bundle, "digest") {
        Ok(computed) if computed == bundle_digest => {}
        _ => errors.push("the reassembled bundle digest does not verify".to_string()),
    }
    if bundle.get("bundle_format_version").and_then(Value::as_u64) != Some(BUNDLE_FORMAT_VERSION) {
        errors.push(format!(
            "bundle_format_version is not {BUNDLE_FORMAT_VERSION}"
        ));
    }
    if bundle
        .get("memory_protocol_version")
        .and_then(Value::as_u64)
        != Some(policy::MEMORY_PROTOCOL_VERSION)
    {
        errors.push(format!(
            "memory_protocol_version is not {}",
            policy::MEMORY_PROTOCOL_VERSION
        ));
    }
    if bundle.get("schema_version").and_then(Value::as_u64) != Some(project::SCHEMA_VERSION) {
        errors.push(format!("schema_version is not {}", project::SCHEMA_VERSION));
    }
    if bundle.get("export_id").and_then(Value::as_str) != Some(source_export_id) {
        errors.push("the header names another export".to_string());
    }

    match serde_json::from_value::<MemoryExportBundle>(bundle.clone()) {
        Ok(_) => errors.extend(reference_errors(&bundle, adopted)),
        Err(error) => errors.push(format!(
            "the reassembled bundle does not match the export payload schema: {error}"
        )),
    }
    errors
}

impl MemoryAdapter {
    // contract §7.31 imports.stage

    pub async fn stage_import(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.stage_import_body(tx, principal, request).await)
    }

    async fn stage_import_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryImportsStageRequest =
            wire::decode(&raw, "memory.imports.stage request")?;
        validated(validate_memory_imports_stage(&request))?;
        let object = wire::object(&raw, "memory.imports.stage request")?;
        let principal_id = principal_id(principal);
        let space_id = wire::required(object, "space_id")?;
        let idempotency_key = wire::required(object, "idempotency_key")?;
        let chunk = object
            .get("chunk")
            .cloned()
            .ok_or_else(|| Reject::invalid("chunk is required"))?;
        let (ordinal, wire_count, wire_digest, source_export_id, header) = match &request {
            MemoryImportsStageRequest::Variant0 {
                chunk,
                conflict_policy,
                adopt_grants,
                reference_translations,
                ..
            } => (
                0,
                chunk.chunk_count.get(),
                chunk.bundle_digest.to_string(),
                chunk.export_id.to_string(),
                Some((
                    conflict_policy.to_string(),
                    serde_json::to_value(adopt_grants)
                        .map_err(|error| Reject::invalid(format!("adopt_grants: {error}")))?,
                    serde_json::to_value(reference_translations).map_err(|error| {
                        Reject::invalid(format!("reference_translations: {error}"))
                    })?,
                )),
            ),
            MemoryImportsStageRequest::Variant1 { chunk, .. } => (
                chunk.chunk_ordinal.get(),
                chunk.chunk_count.get(),
                chunk.bundle_digest.to_string(),
                chunk.export_id.to_string(),
                None,
            ),
        };

        let op = Self::op(space_id, principal_id, STAGE, Some(idempotency_key), &raw)?;
        let admission = Admission {
            operation: STAGE,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(vec![
                space_requirement("transfer"),
                space_requirement("admin"),
            ]),
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            // why: the family is on the §8.2 freeze allowlist, so staging into a
            // frozen space is a maintenance path, not a refusal.
            freeze_gated: false,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryImportsStageResponse>(&receipt);
        }

        // §7.30/§7.31: a caller who could not page the source export is answered the
        // same absence as a caller who never staged, before any state is read.
        // why: an export this store never held is foreign, so there is no local
        // coverage to check and the state a stage reveals is the caller's own.
        if let Some(export) = self.repository.export_by_id(tx, &source_export_id).await? {
            if !super::exports::readable(&self.repository, tx, &export, principal_id, op.now)
                .await?
            {
                return Err(no_staging_import(&source_export_id));
            }
        }

        let staging = self
            .repository
            .import_by_source(tx, &op.space, principal_id, &source_export_id)
            .await?;
        let row = if ordinal == 0 {
            if let Some(row) = &staging {
                if expired(row, op.now)? {
                    return Err(expiry_conflict(row));
                }
                return Err(conflict(format!(
                    "import {} already holds this source export's header",
                    row.import_id
                ))
                .detail("reason", json!("duplicate_header"))
                .detail("import_id", json!(row.import_id)));
            }
            None
        } else {
            let Some(row) = staging else {
                return Err(no_staging_import(&source_export_id));
            };
            if expired(&row, op.now)? {
                return Err(expiry_conflict(&row));
            }
            if ordinal != row.next_ordinal {
                if ordinal < row.next_ordinal {
                    return Err(conflict(format!(
                        "chunk {ordinal} is out of order; the import expects {}",
                        row.next_ordinal
                    ))
                    .detail("reason", json!("out_of_order"))
                    .detail("expected_ordinal", json!(row.next_ordinal))
                    .detail("received_ordinal", json!(ordinal)));
                }
                // why: a skipped ordinal can never be repaired, and a refusal's writes
                // roll back with the caller's transaction — so the gap ends the import
                // as a committed outcome instead of being refused.
                self.repository
                    .mark_import_discarded(tx, &row.import_id)
                    .await?;
                self.repository
                    .delete_import_chunks(tx, &row.import_id)
                    .await?;
                let result = json!({
                    "chunks_received": 0,
                    "import_id": &row.import_id,
                });
                let receipt = admission::committed_receipt(&op, &mint("rct"), result);
                self.repository.insert_receipt(tx, &receipt, None).await?;
                return Self::ready::<MemoryImportsStageResponse>(project::receipt_body(&receipt));
            }
            Some(row)
        };

        let (expected_count, bundle_digest, adopted) = match &row {
            Some(row) => (
                row.expected_count,
                row.bundle_digest.clone(),
                row.adopt_grants.clone(),
            ),
            None => (
                wire_count,
                wire_digest,
                header
                    .as_ref()
                    .map(|(_, adopted, _)| adopted.clone())
                    .unwrap_or_else(|| json!([])),
            ),
        };
        let errors = chunk_errors(
            &chunk,
            ordinal,
            expected_count,
            &bundle_digest,
            &source_export_id,
        );
        if !errors.is_empty() {
            return Err(unverified(errors));
        }

        let import_id = row
            .as_ref()
            .map(|row| row.import_id.clone())
            .unwrap_or_else(|| mint("imp"));
        let final_chunk = ordinal + 1 == expected_count;
        if final_chunk {
            let mut sequence = Vec::with_capacity(expected_count as usize);
            for stored in 0..ordinal {
                let payload = self
                    .repository
                    .import_chunk(tx, &import_id, stored)
                    .await?
                    .ok_or_else(|| {
                        Reject::internal(
                            "the staging import names a chunk this store does not hold",
                        )
                    })?;
                sequence.push(
                    serde_json::from_str(&payload)
                        .map_err(|_| Reject::internal("stored import chunk is unreadable"))?,
                );
            }
            sequence.push(chunk.clone());
            let errors = verification_errors(
                &sequence,
                expected_count,
                &bundle_digest,
                &source_export_id,
                &adopted,
            );
            if !errors.is_empty() {
                return Err(unverified(errors));
            }
        }

        if row.is_none() {
            let ttl = i64::try_from(policy::STAGED_IMPORT_TTL_SECONDS)
                .map_err(|_| Reject::internal("staged import ttl is out of range"))?;
            let (conflict_policy, adopt_grants, reference_translations) =
                header.expect("ordinal zero carries the header selection");
            self.repository
                .insert_import(
                    tx,
                    &MemoryImportRow {
                        import_id: import_id.clone(),
                        space_id: op.space.as_str().to_string(),
                        requester: principal_id.to_string(),
                        source_export_id: source_export_id.clone(),
                        bundle_digest: bundle_digest.clone(),
                        expected_count,
                        next_ordinal: if final_chunk { expected_count } else { 1 },
                        conflict_policy,
                        adopt_grants,
                        reference_translations,
                        state: if final_chunk { "staged" } else { "staging" }.to_string(),
                        created_at: policy::timestamp(op.now),
                        expires_at: policy::timestamp(op.now + chrono::Duration::seconds(ttl)),
                    },
                )
                .await?;
        }
        self.repository
            .insert_import_chunk(tx, &import_id, ordinal, &chunk.to_string())
            .await?;
        if row.is_some() {
            self.repository
                .advance_import(tx, &import_id, ordinal + 1, final_chunk)
                .await?;
        }

        let result = json!({
            "chunks_received": ordinal + 1,
            "import_id": &import_id,
        });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemoryImportsStageResponse>(project::receipt_body(&receipt))
    }
}
