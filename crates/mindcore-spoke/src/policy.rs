//! mindcore-owned provider policy: the surface `describe` advertises, the page
//! bound every list enforces, the deterministic digest/cursor compositions the
//! paged reads bind to, and the content algorithm the protocol leaves to the
//! provider. Nothing here is protocol vocabulary except the identifiers the
//! pinned schema fixes.

use std::num::NonZeroU64;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use spoke_operations::jcs_sha256_hex;
use spoke_operations::SpokeResult;

use crate::reject::{AResult, Reject};

/// §3: the memory protocol revision this adapter speaks.
pub(crate) const MEMORY_PROTOCOL_VERSION: u64 = 1;
/// §4.1: the families this build serves with Reflection enabled. The transfer
/// family is unconditional: this adapter serves all five §7.29–§7.33 operations.
pub(crate) const CAPABILITIES: [&str; 3] = [
    "spoke-memory",
    "spoke-memory-reflection",
    "spoke-memory-transfer",
];
/// §4.1: the same provider with the reflection sub-capability withheld.
pub(crate) const CAPABILITIES_WITHOUT_REFLECTION: [&str; 2] =
    ["spoke-memory", "spoke-memory-transfer"];
/// §4.2: the authored kinds generation requires before it composes.
pub(crate) const REQUIRED_PROFILE_KINDS: [&str; 1] = ["personality"];
/// §6.1: opaque policy stamp clients pin recall/reflection bases to.
pub(crate) const ENGINE_POLICY_VERSION: &str = "mindcore-local-memory-v1";

const MAX_CONTENT_BYTES: u64 = 65_536;
const MAX_CANDIDATES_PER_CAPTURE: u64 = 32;
const MAX_PAGE_SIZE: u64 = 100;
pub(crate) const EXPORT_CHUNK_BYTES: u64 = 262_144;
const IDEMPOTENCY_RETENTION_SECONDS: u64 = 2_592_000;
const RECEIPT_RETENTION_SECONDS: u64 = 2_592_000;
const DEDUP_RETENTION_SECONDS: u64 = 5_184_000;
pub(crate) const STAGED_IMPORT_TTL_SECONDS: u64 = 86_400;
pub(crate) const EXPORT_RETENTION_SECONDS: u64 = 86_400;

/// §7.0.7: the caller's `limit` defaults to and may not exceed `max_page_size`.
pub(crate) const MAX_PAGE_SIZE_BOUND: NonZeroU64 = match NonZeroU64::new(MAX_PAGE_SIZE) {
    Some(bound) => bound,
    None => panic!("max_page_size must be positive"),
};

/// §7.1 `limits`.
pub(crate) fn limits() -> Value {
    json!({
        "max_content_bytes": MAX_CONTENT_BYTES,
        "max_candidates_per_capture": MAX_CANDIDATES_PER_CAPTURE,
        "max_page_size": MAX_PAGE_SIZE,
        "export_chunk_bytes": EXPORT_CHUNK_BYTES,
        "idempotency_retention_seconds": IDEMPOTENCY_RETENTION_SECONDS,
        "receipt_retention_seconds": RECEIPT_RETENTION_SECONDS,
        "dedup_retention_seconds": DEDUP_RETENTION_SECONDS,
        "staged_import_ttl_seconds": STAGED_IMPORT_TTL_SECONDS,
        "export_retention_seconds": EXPORT_RETENTION_SECONDS,
    })
}

pub(crate) fn max_content_bytes() -> usize {
    usize::try_from(MAX_CONTENT_BYTES).unwrap_or(usize::MAX)
}

/// §7.1 when the fixture policy withholds Reflection: the three reflection ops
/// refuse before any storage access.
pub(crate) fn describe_body(reflections: bool) -> Value {
    let capabilities: &[&str] = if reflections {
        &CAPABILITIES
    } else {
        &CAPABILITIES_WITHOUT_REFLECTION
    };
    json!({
        "memory_protocol_version": MEMORY_PROTOCOL_VERSION,
        "capabilities": capabilities,
        "limits": limits(),
        "required_profile_kinds": REQUIRED_PROFILE_KINDS,
        "engine_policy_version": ENGINE_POLICY_VERSION,
    })
}

/// §6.1 `payload_digest` over the caller's original request: the canonical
/// (JCS) form of exactly what arrived, with no wrapper member and no DTO
/// normalization, so two distinct inputs can never collide onto one key.
pub(crate) fn payload_digest(request: &Value) -> AResult<String> {
    digest(request)
}

/// 64 lowercase hex over a canonical object.
pub(crate) fn digest(value: &Value) -> AResult<String> {
    canonical_digest(value, None)
}

/// `2^53`: the inclusive revision endpoint, an exact IEEE-754 integer the
/// TypeScript canonicalizer spells without an exponent.
const EXACT_INTEGER_BOUND: u64 = 1 << 53;

/// RFC 8785 digest over `value` with `excluded` removed. An integer exactly at
/// `±2^53` is re-spelled as its equal `f64` first: `serde_jcs` refuses the
/// endpoint, while the peer stack (TypeScript `canonicalize`, and the connect
/// stack's own JCS) emits it. A value beyond the endpoint still refuses, so an
/// unrepresentable integer never collapses onto a neighbour's digest.
///
/// shortcut: every member at the endpoint is re-spelled, not only the
/// revision-typed paths the connect signature owns; upgrade if a product-owned
/// bag member at exactly `±2^53` must stay refused here.
pub(crate) fn canonical_digest(value: &Value, excluded: Option<&str>) -> AResult<String> {
    let mut stripped = value.clone();
    if let (Some(members), Some(field)) = (stripped.as_object_mut(), excluded) {
        members.remove(field);
    }
    match jcs_sha256_hex(&respell_endpoint_digests(stripped)) {
        SpokeResult::Ok(digest) => Ok(digest),
        SpokeResult::Reject(reject) => Err(Reject::invalid(reject.message)),
    }
}

fn respell_endpoint_digests(value: Value) -> Value {
    match value {
        Value::Number(number) => {
            if number.as_u64() == Some(EXACT_INTEGER_BOUND) {
                Value::from(EXACT_INTEGER_BOUND as f64)
            } else if number.as_i64() == Some(-(EXACT_INTEGER_BOUND as i64)) {
                Value::from(-(EXACT_INTEGER_BOUND as f64))
            } else {
                Value::Number(number)
            }
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(respell_endpoint_digests).collect())
        }
        Value::Object(members) => Value::Object(
            members
                .into_iter()
                .map(|(key, member)| (key, respell_endpoint_digests(member)))
                .collect(),
        ),
        other => other,
    }
}

/// Full-second-precision stamp the schema's `timestamp` format reads back.
pub(crate) fn timestamp(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

pub(crate) const fn page_limit(limit: Option<NonZeroU64>) -> u64 {
    match limit {
        Some(limit) => limit.get(),
        None => MAX_PAGE_SIZE_BOUND.get(),
    }
}

/// Cut one page out of an already-ordered list, reporting the next offset.
pub(crate) fn page<T>(items: Vec<T>, limit: u64, offset: u64) -> (Vec<T>, Option<u64>) {
    let total = items.len();
    let start = usize::try_from(offset).unwrap_or(total).min(total);
    let take = usize::try_from(limit).unwrap_or(total);
    let page: Vec<T> = items.into_iter().skip(start).take(take).collect();
    let consumed = start + page.len();
    (page, (consumed < total).then_some(consumed as u64))
}

/// The request's id set canonicalized wherever it is treated as a set.
pub(crate) fn canonical_ids(ids: &[String]) -> Vec<String> {
    let mut ids = ids.to_vec();
    ids.sort();
    ids.dedup();
    ids
}

/// Opaque page cursor bound to principal + scope + snapshot (§7.0.7/MEM-R026).
#[derive(Serialize, Deserialize)]
struct PageCursor {
    principal: String,
    scope: String,
    offset: u64,
    marker: String,
}

pub(crate) fn cursor_scope(
    op: &str,
    space_id: Option<&str>,
    subject_id: Option<&str>,
    principal_filter: Option<&str>,
) -> String {
    format!(
        "{op}|space={}|subject={}|principal={}|policy={ENGINE_POLICY_VERSION}",
        space_id.unwrap_or("*"),
        subject_id.unwrap_or("*"),
        principal_filter.unwrap_or("*"),
    )
}

pub(crate) fn encode_cursor(principal: &str, scope: &str, offset: u64, marker: &str) -> String {
    let cursor = PageCursor {
        principal: principal.to_string(),
        scope: scope.to_string(),
        offset,
        marker: marker.to_string(),
    };
    serde_json::to_string(&cursor).expect("a page cursor always serializes")
}

/// The offset and claimed marker a cursor names, verified against its scope
/// before the page is cut.
pub(crate) fn cursor_page(
    raw: Option<&str>,
    principal: &str,
    scope: &str,
) -> AResult<(u64, Option<String>)> {
    let Some(raw) = raw else {
        return Ok((0, None));
    };
    let cursor: PageCursor = serde_json::from_str(raw).map_err(|_| {
        Reject::terminal(
            spoke_operations::SpokeRejectCode::CursorInvalid,
            "cursor is not a page cursor",
        )
    })?;
    if cursor.principal != principal || cursor.scope != scope {
        return Err(Reject::terminal(
            spoke_operations::SpokeRejectCode::CursorInvalid,
            "cursor was issued for another query",
        ));
    }
    Ok((cursor.offset, Some(cursor.marker)))
}

/// A page read against a different snapshot than the cursor was minted on.
pub(crate) fn verify_cursor(claimed: Option<&str>, marker: &str) -> AResult<()> {
    match claimed {
        Some(claimed) if claimed != marker => Err(Reject::terminal(
            spoke_operations::SpokeRejectCode::CursorInvalid,
            "the authorized set changed since the cursor was issued",
        )),
        _ => Ok(()),
    }
}

/// The placement selection rendered into the digest/bound-query key.
pub(crate) fn selection_key(selection: &mindcore_base::ir::PlacementSelection) -> String {
    use mindcore_base::ir::PlacementSelection;
    match selection {
        PlacementSelection::SubjectAll => "subject_all".to_string(),
        PlacementSelection::SharedOnly => "shared_only".to_string(),
        PlacementSelection::SelectedContexts {
            context_ids,
            include_shared,
        } => {
            let ids: Vec<String> = context_ids.iter().map(ToString::to_string).collect();
            format!(
                "selected_contexts({}){}",
                canonical_ids(&ids).join(","),
                if *include_shared { "+shared" } else { "" }
            )
        }
    }
}

pub(crate) fn filter_key(filter: Option<&[String]>) -> String {
    filter.map_or_else(
        || "*".to_string(),
        |sources| canonical_ids(sources).join(","),
    )
}

/// §7.25 `state_digest`: the canonical object both the `state` op publishes and
/// a pinned basis carries.
pub(crate) fn reflection_state_digest(
    records: &[(String, i64)],
    profile_revisions: &MapOfRevisions,
    selection: &mindcore_base::ir::PlacementSelection,
    filter: Option<&[String]>,
) -> AResult<String> {
    digest(&json!({
        "engine_policy_version": ENGINE_POLICY_VERSION,
        "filter": filter_key(filter),
        "profile_revisions": profile_revisions,
        "records": records,
        "selection": selection_key(selection),
    }))
}

/// §7.27 `source_set_digest`: 64 hex over the authorized source id+revision set.
pub(crate) fn reflection_source_set_digest(records: &[(String, i64)]) -> AResult<String> {
    digest(&json!({ "records": records }))
}

pub(crate) type MapOfRevisions = Vec<(String, i64)>;

/// The MEM-R026 invalidation marker: everything that decides which rows a page
/// holds — the page's own identities and revisions, every grant revision in the
/// space, and the subject's lifecycle status.
pub(crate) fn snapshot_marker(
    rows: Vec<(String, i64)>,
    grants: &[(String, i64)],
    subject_status: Option<&str>,
) -> AResult<String> {
    let mut rows = rows;
    rows.sort();
    let mut grants = grants.to_vec();
    grants.sort();
    digest(&json!({
        "rows": rows,
        "grants": grants,
        "subject": subject_status,
    }))
}

/// §7.25 provider policy verdict over the authorized basis.
pub(crate) const fn data_sufficiency(record_count: usize) -> &'static str {
    if record_count == 0 {
        "insufficient"
    } else {
        "sufficient"
    }
}

/// A subject's profile kinds and the required ones it misses. Presence alone is
/// not completeness: a required kind whose stored document carries no content
/// is still missing (§7.27 gates generation on authored content).
pub(crate) fn profile_projection(profiles: &[(String, bool)]) -> (Vec<String>, Vec<String>) {
    let kinds: Vec<String> = profiles.iter().map(|(kind, _)| kind.clone()).collect();
    let missing: Vec<String> = REQUIRED_PROFILE_KINDS
        .iter()
        .filter(|required| {
            !profiles
                .iter()
                .any(|(kind, has_content)| kind == *required && *has_content)
        })
        .map(|required| (*required).to_string())
        .collect();
    (kinds, missing)
}

/// Whether one stored profile document carries authored content. The §7.24
/// shape is `{body, format}`; an empty or whitespace-only body is no content.
pub(crate) fn profile_document_has_content(document: &Value) -> bool {
    match document {
        Value::Null => false,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => match members.get("body").and_then(Value::as_str) {
            Some(body) => !body.trim().is_empty(),
            None => members.values().any(profile_document_has_content),
        },
        Value::Bool(_) | Value::Number(_) => true,
    }
}

/// §6.7 advisory classification of a capture; the reviewer stays authoritative.
pub(crate) struct Classification {
    pub(crate) disposition: &'static str,
    pub(crate) rationale_codes: Vec<&'static str>,
}

const PROMOTABLE_MIN_WORDS: usize = 8;

pub(crate) fn classify(text: &str, kind_hint: Option<&str>) -> Classification {
    if text.split_whitespace().next().is_none() {
        return Classification {
            disposition: "drop",
            rationale_codes: vec!["empty_content"],
        };
    }
    if kind_hint.is_some_and(|hint| !hint.is_empty()) {
        return Classification {
            disposition: "promote",
            rationale_codes: vec!["kind_hint"],
        };
    }
    if text.split_whitespace().count() < PROMOTABLE_MIN_WORDS {
        return Classification {
            disposition: "fragment",
            rationale_codes: vec!["short_capture"],
        };
    }
    Classification {
        disposition: "promote",
        rationale_codes: vec!["multi_word_capture"],
    }
}

/// §9.2 ranking hint terms: lowercase alphanumeric words, de-duplicated.
pub(crate) fn query_terms(query: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for term in query.split(|character: char| !character.is_alphanumeric()) {
        if term.is_empty() {
            continue;
        }
        let term = term.to_lowercase();
        if !terms.contains(&term) {
            terms.push(term);
        }
    }
    terms
}

/// §9.2: how many distinct query terms occur in the record text.
pub(crate) fn recall_score(text: &str, terms: &[String]) -> usize {
    if terms.is_empty() {
        return 0;
    }
    let text = text.to_lowercase();
    terms
        .iter()
        .filter(|term| text.contains(term.as_str()))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(value: &Value) -> String {
        match jcs_sha256_hex(value) {
            SpokeResult::Ok(digest) => digest,
            SpokeResult::Reject(_) => panic!("expected a digest from the upstream canonicalizer"),
        }
    }

    fn local(value: &Value, excluded: Option<&str>) -> String {
        match canonical_digest(value, excluded) {
            Ok(digest) => digest,
            Err(_) => panic!("expected a digest"),
        }
    }

    /// The inclusive `2^53` revision endpoint is an exact `f64`, so the digest must hash
    /// the same bytes the peer stack emits instead of refusing the integer token.
    #[test]
    fn digest_admits_the_exact_revision_endpoint() {
        let integer =
            json!({"records": [{"record_id": "rec_a", "revision": 9_007_199_254_740_992u64}]});
        let float =
            json!({"records": [{"record_id": "rec_a", "revision": 9_007_199_254_740_992.0}]});
        assert_eq!(local(&integer, None), upstream(&float));
    }

    #[test]
    fn digest_refuses_an_integer_past_the_endpoint() {
        let past =
            json!({"records": [{"record_id": "rec_a", "revision": 9_007_199_254_740_993u64}]});
        assert!(digest(&past).is_err());
    }

    /// The seal excludes its own member before hashing, endpoint re-spelling included.
    #[test]
    fn digest_excluding_removes_the_seal_member() {
        let sealed = json!({
            "chunk_digest": "seal",
            "records": [{"record_id": "rec_a", "revision": 9_007_199_254_740_992u64}],
        });
        let unsealed =
            json!({"records": [{"record_id": "rec_a", "revision": 9_007_199_254_740_992.0}]});
        assert_eq!(local(&sealed, Some("chunk_digest")), upstream(&unsealed));
    }
}
