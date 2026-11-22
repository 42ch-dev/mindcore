//! §6.11/§6.13 outbound transfer: one borrowed-transaction snapshot projected
//! into the logical export bundle, sealed by its JCS digest, and partitioned
//! into element-atomic wire chunks.
//!
//! Storage not served by [`MemoryRepository`] (the space's subjects and its
//! content-free receipt rows) is read here with a runtime query over the caller's
//! transaction; every other row comes through the repository.

// why: the adapter's `create_export` (the next task on this branch) is the caller;
// until it lands this staged module has no caller outside its own tests.
#![allow(dead_code)]

mod bundle;
mod chunk;
#[cfg(test)]
mod tests;

use chrono::{DateTime, Utc};
use serde_json::{Map, Number, Value};

use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::{
    CandidateSelection, MemoryCandidateIr, MemoryProfileRow, MemoryRecordIr, MemoryReflectionRow,
    MemoryRepository, MemorySubjectRow, PlacementSelection, RecordSelection,
};
use mindcore_sqlite::SqliteMemoryRepository;
use spoke_operations::SpokeRejectCode;

use crate::policy;
use crate::reject::{AResult, Reject};
use crate::Tx;

/// One export request, with the scope already admitted by the caller.
pub(crate) struct ExportSpec {
    /// Freshly minted for the first execution; a replay returns the stored one.
    pub(crate) export_id: String,
    pub(crate) space_id: SpaceId,
    /// `None` exports the whole space; `Some` narrows to one subject.
    pub(crate) subject_id: Option<SubjectId>,
    /// The placement scope; whole-space and subject-default exports use
    /// `SubjectAll`.
    pub(crate) selection: PlacementSelection,
    pub(crate) include_tombstoned: bool,
    pub(crate) include_reflections: bool,
    /// The operation instant, never the space's creation time.
    pub(crate) created_at: DateTime<Utc>,
}

/// The sealed logical bundle and its ordered wire chunks.
pub(crate) struct ExportArtifact {
    pub(crate) bundle: Value,
    pub(crate) digest: String,
    pub(crate) chunks: Vec<Value>,
}

/// One subject's scoped rows: read once, then projected and verified in memory.
struct SubjectSnapshot {
    subject: MemorySubjectRow,
    records: Vec<MemoryRecordIr>,
    candidates: Vec<MemoryCandidateIr>,
    profiles: Vec<MemoryProfileRow>,
    reflections: Vec<MemoryReflectionRow>,
}

/// One content-free `memory_receipts` row, for the whole-space summary.
pub(super) struct ReceiptRow {
    receipt_id: String,
    requester: String,
    operation: String,
    payload_digest: String,
    status: String,
    created_at: String,
}

/// Build and seal the export bundle, then partition it into ordered chunks.
pub(crate) async fn build(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    spec: &ExportSpec,
) -> AResult<ExportArtifact> {
    let space = repository
        .space(tx, &spec.space_id)
        .await?
        .ok_or_else(|| Reject::not_found(format!("space {}", spec.space_id.as_str())))?;

    let mut snapshots = Vec::new();
    for subject in subject_rows(repository, tx, spec).await? {
        let subject_id = subject.subject_id.as_str();
        let records = repository
            .list_records(tx, &spec.space_id, subject_id, &record_selection(spec))
            .await?;
        let candidates = repository
            .list_candidates(tx, &spec.space_id, subject_id, &candidate_selection(spec))
            .await?;
        // Reflection staleness recomputes the pinned state digest, which covers the
        // subject's profile revisions even when the profiles themselves are withheld.
        let profiles = if profiles_exported(spec) || spec.include_reflections {
            repository
                .list_profiles(tx, &spec.space_id, subject_id, &[])
                .await?
        } else {
            Vec::new()
        };
        let reflections = if spec.include_reflections {
            repository
                .list_reflections(tx, &spec.space_id, subject_id, Some(&spec.selection))
                .await?
        } else {
            Vec::new()
        };
        snapshots.push(SubjectSnapshot {
            subject,
            records,
            candidates,
            profiles,
            reflections,
        });
    }

    let grants = repository.list_grants(tx, &spec.space_id, None).await?;
    let receipts = if spec.subject_id.is_none() {
        receipt_rows(tx, spec).await?
    } else {
        Vec::new()
    };

    let bundle = bundle::assemble(spec, &space, snapshots, &grants, &receipts)?;
    let digest = bundle["digest"]
        .as_str()
        .ok_or_else(|| Reject::internal("the export bundle carries no digest"))?
        .to_string();
    let chunks = chunk::partition(&bundle, policy::EXPORT_CHUNK_BYTES)?;
    Ok(ExportArtifact {
        bundle,
        digest,
        chunks,
    })
}

/// §6.11: profiles are subject-wide with no placement column, so a placement-restricted
/// export cannot carry them.
fn profiles_exported(spec: &ExportSpec) -> bool {
    spec.subject_id.is_none() || matches!(spec.selection, PlacementSelection::SubjectAll)
}

fn record_selection(spec: &ExportSpec) -> RecordSelection {
    RecordSelection {
        placement: spec.selection.clone(),
        provenance_filter: None,
        kinds: Vec::new(),
        include_tombstoned: spec.include_tombstoned,
    }
}

fn candidate_selection(spec: &ExportSpec) -> CandidateSelection {
    CandidateSelection {
        placement: spec.selection.clone(),
        status: None,
    }
}

/// The subjects the export names: the one subject, or every subject of the space.
async fn subject_rows(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    spec: &ExportSpec,
) -> AResult<Vec<MemorySubjectRow>> {
    match &spec.subject_id {
        Some(subject_id) => Ok(repository
            .subject(tx, &spec.space_id, subject_id.as_str())
            .await?
            .into_iter()
            .collect()),
        None => scan_subjects(tx, &spec.space_id).await,
    }
}

/// Every subject row of one space, in stable id order.
async fn scan_subjects(tx: &mut Tx<'_>, space: &SpaceId) -> AResult<Vec<MemorySubjectRow>> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<String>,
            String,
            String,
            i64,
            String,
        ),
    >(
        "SELECT subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, created_at \
         FROM memory_subjects WHERE space_id = ? ORDER BY subject_id",
    )
    .bind(space.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(|_| storage_failure())?;
    rows.into_iter()
        .map(
            |(subject_id, namespace, kind, status, fenced_contexts, revision, created_at)| {
                let kind = match (namespace, kind) {
                    (Some(namespace), Some(kind)) => Some((namespace, kind)),
                    (None, None) => None,
                    _ => return Err(unreadable_row("memory_subjects.kind")),
                };
                let fenced: Vec<String> = serde_json::from_str(&fenced_contexts)
                    .map_err(|_| unreadable_row("memory_subjects.fenced_contexts_json"))?;
                Ok(MemorySubjectRow {
                    space_id: space.clone(),
                    subject_id: SubjectId::new(subject_id),
                    kind,
                    status,
                    fenced_contexts: fenced.into_iter().map(ContextId::new).collect(),
                    revision: Number::from(revision),
                    created_at,
                    extensions: Map::new(),
                })
            },
        )
        .collect()
}

/// The space's receipt summaries, content-free by construction: no result, error,
/// lease or raw idempotency key is selected.
async fn receipt_rows(tx: &mut Tx<'_>, spec: &ExportSpec) -> AResult<Vec<ReceiptRow>> {
    let rows = sqlx::query_as::<_, (String, String, String, String, String, String)>(
        "SELECT receipt_id, requester_principal_id, op, payload_digest, status, created_at \
         FROM memory_receipts WHERE space_id = ? ORDER BY receipt_id",
    )
    .bind(spec.space_id.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(|_| storage_failure())?;
    Ok(rows
        .into_iter()
        .map(
            |(receipt_id, requester, operation, payload_digest, status, created_at)| ReceiptRow {
                receipt_id,
                requester,
                operation,
                payload_digest,
                status,
                created_at,
            },
        )
        .collect())
}

fn storage_failure() -> Reject {
    Reject::internal("storage failure before commit")
}

fn unreadable_row(what: &str) -> Reject {
    Reject::terminal(
        SpokeRejectCode::InternalError,
        format!("stored {what} is unreadable"),
    )
}

/// JCS byte length of one value, computed through the shared canonicalizer.
fn canonical_len(value: &Value) -> AResult<usize> {
    let length = spoke_operations::content_serialized_bytes(value);
    if length == usize::MAX {
        Err(Reject::invalid(
            "the export artifact cannot be canonicalized (RFC 8785)",
        ))
    } else {
        Ok(length)
    }
}
