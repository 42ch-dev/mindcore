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

pub(crate) const STAGE: &str = "memory.imports.stage";

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
    // why: only an import nobody has activated carries a deadline. `activated` and
    // `discarded` are terminal, so a late chunk or a repeat activation against one must
    // answer its state, never the clock that its staging once ran against.
    match row.state.as_str() {
        // why: a reclaimed row is dead whatever the clock says; deciding it by time alone
        // would let a backward clock step revive the import as a live header.
        "expired" => return Ok(true),
        "staging" | "staged" => {}
        _ => return Ok(false),
    }
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
/// only that minimum, so the screen refuses a value that matches a known credential's
/// *whole shape*: a live key prefix followed by that provider's token length and
/// alphabet, or a JWT. A prefix alone is not evidence — a foreign exporter's plain
/// label such as `sk_invoice_42` or `ghp_batch_7` stays a correlation label. This crate
/// emits `prl_` plus 32 lowercase hex digits (`bundle::principal_label`); that grammar
/// is our convention, not an inbound requirement.
fn correlation_label(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_whitespace) && !credential_shaped(value)
}

/// Prefix paired with the tail a real credential carries, never the bare prefix.
fn credential_shaped(value: &str) -> bool {
    // Stripe-style live/test secrets: `sk_live_`/`rk_live_` + 24+ base62.
    for prefix in ["sk_live_", "sk_test_", "rk_live_", "rk_test_", "pk_live_"] {
        if let Some(tail) = value.strip_prefix(prefix) {
            if tail.len() >= 24 && tail.chars().all(|c| c.is_ascii_alphanumeric()) {
                return true;
            }
        }
    }
    // GitHub tokens: `ghp_`/`gho_`/`ghs_`/`ghu_` + exactly 36 base62.
    for prefix in ["ghp_", "gho_", "ghs_", "ghu_"] {
        if let Some(tail) = value.strip_prefix(prefix) {
            if tail.len() == 36 && tail.chars().all(|c| c.is_ascii_alphanumeric()) {
                return true;
            }
        }
    }
    // Slack tokens: `xoxb-`/`xoxp-` + long digit groups.
    for prefix in ["xoxb-", "xoxp-", "xoxa-", "xoxo-"] {
        if let Some(tail) = value.strip_prefix(prefix) {
            if tail.len() >= 24
                && tail
                    .chars()
                    .all(|c| c.is_ascii_digit() || c.is_ascii_alphabetic() || c == '-')
            {
                return true;
            }
        }
    }
    // AWS access key id: exactly `AKIA` + 16 uppercase alphanumerics.
    if let Some(tail) = value.strip_prefix("AKIA") {
        if tail.len() == 16
            && tail
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        {
            return true;
        }
    }
    // JWT: three base64url segments, the header starting `eyJ`.
    let segments: Vec<&str> = value.split('.').collect();
    segments.len() == 3
        && value.starts_with("eyJ")
        && segments.iter().all(|segment| {
            segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
}

/// References and grant summaries: every cross-reference resolves inside the
/// bundle, and an adopted grant is one the bundle actually carries.
fn reference_errors(bundle: &Value, adopted: &Value) -> Vec<String> {
    let mut errors = Vec::new();
    let subjects = key_set(bundle, "subjects", "subject_id");
    let records = key_set(bundle, "records", "record_id");

    // why: the identity maps are many-to-one keys. A duplicate would pass verification
    // and then break activation's mapping, stranding the import mid-sequence.
    for (field, member, noun) in [
        ("subjects", "subject_id", "subject"),
        ("records", "record_id", "record"),
        ("candidates", "candidate_id", "candidate"),
    ] {
        let mut ids = BTreeSet::new();
        for item in bundle[field].as_array().into_iter().flatten() {
            let Some(id) = item.get(member).and_then(Value::as_str) else {
                continue;
            };
            if !ids.insert(id) {
                errors.push(format!("{noun} {id} appears more than once"));
            }
        }
    }

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
        let grants = admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryImportsStageResponse>(&receipt);
        }

        // why: a local export the caller may not page is treated exactly like an export
        // this store never held. Refusing one and accepting the other would disclose
        // which guessed export ids exist; staging is the caller's own durable state, and
        // the chunks it carries verify against the bundle on their own.
        let staging = self
            .repository
            .import_by_source(tx, &op.space, principal_id, &source_export_id)
            .await?;
        let mut live = staging;
        if let Some(row) = &live {
            if expired(row, op.now)? {
                // why: an expired import is dead state, not a live one. Reclaiming it
                // here — a successful, committed write — releases its payload and its
                // source-export key, so this call can start the sequence again.
                if ordinal > 0 {
                    return Err(expiry_conflict(row));
                }
                self.repository.expire_import(tx, &row.import_id).await?;
                self.repository
                    .delete_import_chunks(tx, &row.import_id)
                    .await?;
                live = None;
            }
        }
        // why: retention purges expired staging and expired export payloads on any
        // successful transfer write for the space, so neither abandoned transfer state
        // holds storage until its own source returns. The exact-source reclaim above runs
        // first, so a late chunk still names its expiry.
        self.repository
            .expire_imports(tx, &op.space, &policy::timestamp(op.now))
            .await?;
        self.repository
            .expire_exports(tx, &op.space, &policy::timestamp(op.now))
            .await?;
        let row = if ordinal == 0 {
            if let Some(row) = &live {
                return Err(conflict(format!(
                    "import {} already holds this source export's header",
                    row.import_id
                ))
                .detail("reason", json!("duplicate_header"))
                .detail("import_id", json!(row.import_id))
                .detail("expected_ordinal", json!(row.next_ordinal))
                .detail("received_ordinal", json!(ordinal)));
            }
            None
        } else {
            let Some(row) = live else {
                return Err(no_staging_import(&source_export_id));
            };
            // why: `next_ordinal` alone decides what is acceptable, so a repeat and a
            // skipped ordinal take the same refusal — and a refusal writes nothing,
            // deletes nothing, and leaves `next_ordinal` where it was.
            if ordinal != row.next_ordinal {
                return Err(conflict(format!(
                    "chunk {ordinal} is out of order; the import expects {}",
                    row.next_ordinal
                ))
                .detail("reason", json!("out_of_order"))
                .detail("expected_ordinal", json!(row.next_ordinal))
                .detail("received_ordinal", json!(ordinal)));
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
        let mut bundle = None;
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
            bundle = reassemble(&sequence);
        }

        let fresh = if row.is_none() {
            let ttl = i64::try_from(policy::STAGED_IMPORT_TTL_SECONDS)
                .map_err(|_| Reject::internal("staged import ttl is out of range"))?;
            let (conflict_policy, adopt_grants, reference_translations) =
                header.expect("ordinal zero carries the header selection");
            Some(MemoryImportRow {
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
                staged_payload: None,
                state: "staging".to_string(),
                created_at: policy::timestamp(op.now),
                expires_at: policy::timestamp(op.now + chrono::Duration::seconds(ttl)),
            })
        } else {
            None
        };
        if let Some(fresh) = &fresh {
            self.repository.insert_import(tx, fresh).await?;
        }

        if final_chunk {
            let import = fresh.or(row).ok_or_else(|| {
                Reject::internal("a staged import carries neither a fresh nor a live header")
            })?;
            let bundle = bundle
                .ok_or_else(|| Reject::internal("the verified sequence carries no header"))?;
            // §7.31/§12.1: the final chunk is where the identity mapping, the translated
            // payload and the report are fixed. Activation applies this frozen result.
            let prepared = super::activate::prepare(
                &self.repository,
                tx,
                &op.space,
                &import,
                &bundle,
                &grants,
                principal_id,
                op.now,
            )
            .await?;
            let encode = |value: &Value, what: &str| {
                serde_json::to_string(value)
                    .map_err(|error| Reject::internal(format!("{what}: {error}")))
            };
            if !self
                .repository
                .finish_import(
                    tx,
                    &import_id,
                    &encode(&prepared.payload, "staged payload")?,
                    &encode(&prepared.report, "staged report")?,
                )
                .await?
            {
                return Err(Reject::internal(
                    "the staging import left staging under its final chunk",
                ));
            }
            // The frozen result replaces the raw sequence: nothing downstream re-reads it.
            self.repository.delete_import_chunks(tx, &import_id).await?;

            let result = json!({
                "chunks_received": ordinal + 1,
                "import_id": &import_id,
                "staged_report": prepared.report,
            });
            let receipt = admission::committed_receipt(&op, &mint("rct"), result);
            self.repository.insert_receipt(tx, &receipt, None).await?;
            return Self::ready::<MemoryImportsStageResponse>(project::receipt_body(&receipt));
        }

        self.repository
            .insert_import_chunk(tx, &import_id, ordinal, &chunk.to_string())
            .await?;
        if row.is_some() {
            self.repository
                .advance_import(tx, &import_id, ordinal + 1)
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
