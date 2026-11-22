//! §6.11/§6.13 outbound transfer: one borrowed-transaction snapshot projected
//! into the logical export bundle, sealed by its JCS digest, and partitioned
//! into element-atomic wire chunks.

mod activate;
mod bundle;
mod chunk;
mod exports;
mod stage;
#[cfg(test)]
mod tests;

use chrono::{DateTime, Utc};
use serde_json::Value;

use mindcore_base::ids::{SpaceId, SubjectId};
use mindcore_base::ir::{
    CandidateSelection, MemoryCandidateIr, MemoryProfileRow, MemoryRecordIr, MemoryReflectionRow,
    MemoryRepository, MemorySubjectRow, PlacementSelection, RecordSelection,
};
use mindcore_sqlite::SqliteMemoryRepository;

use crate::policy;
use crate::reject::{AResult, Reject};
use crate::Tx;

pub(crate) use activate::{ACTIVATE, DISCARD};
pub(crate) use exports::{CREATE, GET};
pub(crate) use stage::STAGE;

/// The five §7.29–§7.33 operations this crate dispatches. One list: the serving seam reports it
/// as the family it serves, and the manifest derivation asks whether every one of them is there.
pub const OPERATIONS: [&str; 5] = [CREATE, GET, STAGE, ACTIVATE, DISCARD];

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
        // §5.2/D-10: every authorized read withholds the subject while any of its
        // durable intents is unsettled, so the export carries nothing of it, its
        // subject row included. Counts therefore report the post-filter totals.
        if repository
            .has_unsettled_intent(tx, &spec.space_id, &subject.subject_id)
            .await?
        {
            continue;
        }
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
    // A receipt row carries no subject or placement, so only a whole-space export
    // can summarize the space's receipts.
    let receipts = if spec.subject_id.is_none() {
        repository.receipt_summaries(tx, &spec.space_id).await?
    } else {
        Vec::new()
    };

    let bundle = bundle::assemble(spec, &space, snapshots, &grants, &receipts)?;
    let digest = bundle["digest"]
        .as_str()
        .ok_or_else(|| Reject::internal("the export bundle carries no digest"))?
        .to_string();
    let chunks = chunk::partition(&bundle, &digest, policy::EXPORT_CHUNK_BYTES)?;
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
        None => Ok(repository.list_subjects(tx, &spec.space_id).await?),
    }
}

/// JCS byte length of one value, computed through the shared canonicalizer.
pub(super) fn canonical_len(value: &Value) -> AResult<usize> {
    let length = spoke_operations::content_serialized_bytes(value);
    if length == usize::MAX {
        Err(Reject::invalid(
            "the export artifact cannot be canonicalized (RFC 8785)",
        ))
    } else {
        Ok(length)
    }
}
