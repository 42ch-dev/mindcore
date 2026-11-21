//! Supported adoption of a shared knowledge entry into a personal collection.
//!
//! The helper takes the caller's two scoped ports: the source collection is the one `src` serves,
//! the destination is the one `dst` serves. Neither port is selected from the payload, so one
//! host-supplied pairing decides where a copy can land.
//!
//! The destination key is the source `entry_id`. A repeat call recognizes the copy by its Mindcore
//! adoption anchor and returns it instead of writing a second one, so the guarantee leans on the
//! destination port's atomic create-only CAS; without that CAS the slot can be claimed twice.

use serde_json::Value;

use mindcore_base::knowledge::{adoption_anchor_at_revision, adoption_anchor_parts};
use spoke_operations::{
    knowledge_entry_visible_to_viewpoint, spoke_reject, KnowledgeEntryPort, SpokeReject,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::knowledge_entry::{KnowledgeEntryModulesKey, KnowledgeEntryModulesValue};
use spoke_schemas::KnowledgeEntry;

/// Module namespace holding a carrier's cognition envelope.
const COGNITION_MODULE: &str = "cognition";

/// Detail a destination adapter attaches to the create refusal for a slot it holds superseded.
const SUPERSEDED_OCCUPANT_DETAIL: &str = "superseded_by";

/// What one adoption call did with the destination slot.
#[derive(Debug, Clone)]
pub enum AdoptionOutcome {
    /// This call claimed the slot and wrote the copy.
    Created(KnowledgeEntry),
    /// The slot already held this adoption; the stored copy is returned unchanged.
    AlreadyPresent(KnowledgeEntry),
}

/// Adopts the shared entry `entry_id` into the collection `dst` serves.
///
/// The copy carries an adoption anchor pinned to the source revision it was taken from and keeps
/// the source's knowledge metadata, but none of its cognition records. The source entry is only
/// ever read.
///
/// A missing, superseded or undisclosable source is reported as one generic
/// [`SpokeRejectCode::KnowledgeEntryNotFound`], repeat calls included: no anchor is ever written
/// for a source the caller cannot see now. An occupied slot is recognized only by this adoption's
/// own anchor, and only at the revision that anchor records: a copy adopted at another revision is
/// [`SpokeRejectCode::RevisionConflict`] naming the revision it was adopted at, even when the
/// source now sits at the requested one, so a stale copy is never served as the revision asked
/// for. Any other occupant, including a legacy anchor without a recorded revision or a
/// superseded row the live read hides, is [`SpokeRejectCode::KnowledgeEntryAlreadyExists`] and is
/// never overwritten or refreshed. An empty `entry_id` and a zero revision name no anchor and are
/// [`SpokeRejectCode::InvalidInput`].
pub async fn adopt_knowledge_entry(
    src: &dyn KnowledgeEntryPort,
    dst: &dyn KnowledgeEntryPort,
    entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome> {
    // Neither an empty id nor a zero revision can name an adoption anchor, so the call is refused
    // before it reads or writes: every anchor this helper writes round-trips through the parser.
    let Some(anchor) = adoption_anchor_at_revision(entry_id, expected_source_revision) else {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "an adoption anchors a non-empty entry id at a positive source revision",
            None,
        );
    };
    // The source decides existence, freshness and disclosure before the destination is read, so a
    // missing, superseded or undisclosable source answers one generic absence even on a repeat.
    let source = match src.get_knowledge_entry(entry_id).await {
        SpokeResult::Ok(source) if knowledge_entry_visible_to_viewpoint(&source, None) => source,
        SpokeResult::Ok(_) => return source_absent(),
        SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::KnowledgeEntryNotFound => {
            return source_absent()
        }
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let visible_source_revision = source.revision;

    // The slot is the exact key. Only this adoption's own anchor is recognizable; any other
    // occupant is a conflict this helper never overwrites.
    let mut copy = match dst.get_knowledge_entry(entry_id).await {
        SpokeResult::Ok(existing) => return classify(existing, entry_id, expected_source_revision),
        SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::KnowledgeEntryNotFound => {
            source
        }
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };

    if visible_source_revision != Some(expected_source_revision) {
        return source_revision_conflict(entry_id, expected_source_revision);
    }
    // The source DTO moves into the destination write: only the identity the destination assigns
    // and the provenance this call records differ from what was just read.
    copy.entry_id = entry_id.to_string();
    copy.revision = None;
    copy.created_at = None;
    copy.updated_at = None;
    empty_cognition_records(&mut copy);
    copy.source_anchor = Some(anchor);

    match dst.put_knowledge_entry(copy, None).await {
        SpokeResult::Ok(created) => SpokeResult::Ok(AdoptionOutcome::Created(created)),
        // Another writer claimed the slot between the read and the create. The stored copy answers
        // like a pre-existing one; an unreadable slot preserves the original conflict.
        SpokeResult::Reject(reject)
            if reject.code == SpokeRejectCode::KnowledgeEntryAlreadyExists =>
        {
            match dst.get_knowledge_entry(entry_id).await {
                SpokeResult::Ok(existing) => classify(existing, entry_id, expected_source_revision),
                SpokeResult::Reject(_) => SpokeResult::Reject(reject),
            }
        }
        SpokeResult::Reject(reject) if rejects_superseded_occupant(&reject) => occupied(entry_id),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    }
}

/// Answers one occupancy of the destination slot by the anchor rule.
///
/// The revision the anchor records is the copy's identity, never the source's current revision: a
/// request naming anything else is a stale copy, not the revision it asked for.
fn classify(
    existing: KnowledgeEntry,
    entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome> {
    match adopted_revision(&existing, entry_id) {
        Some(adopted) if expected_source_revision == adopted => {
            SpokeResult::Ok(AdoptionOutcome::AlreadyPresent(existing))
        }
        Some(adopted) => spoke_reject(
            SpokeRejectCode::RevisionConflict,
            format!("adopted copy of {entry_id} carries source revision {adopted}"),
            None,
        ),
        // An unrelated occupant, or a legacy anchor without a recorded revision, is never adopted
        // over and never refreshed.
        None => occupied(entry_id),
    }
}

/// The one occupancy refusal: the slot holds something this adoption does not own.
fn occupied<T>(entry_id: &str) -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::KnowledgeEntryAlreadyExists,
        format!("knowledge entry {entry_id} already exists"),
        None,
    )
}

/// Whether a destination refusal is its structured "this slot holds a superseded entry".
///
/// The pair is matched as a pair: an `InvalidInput` without the detail is a real validation
/// failure of the copy and propagates untouched.
fn rejects_superseded_occupant(reject: &SpokeReject) -> bool {
    reject.code == SpokeRejectCode::InvalidInput
        && reject.details.as_ref().is_some_and(|details| {
            details
                .get(SUPERSEDED_OCCUPANT_DETAIL)
                .is_some_and(Value::is_string)
        })
}

/// The revision a copy's adoption anchor records for `entry_id`, when it names that entry.
fn adopted_revision(entry: &KnowledgeEntry, entry_id: &str) -> Option<u64> {
    match adoption_anchor_parts(entry) {
        Some((source_id, adopted)) if source_id == entry_id => adopted,
        _ => None,
    }
}

/// Keeps the cognition envelope and its member presence, but no copied judgment rows.
fn empty_cognition_records(entry: &mut KnowledgeEntry) {
    let Ok(key) = KnowledgeEntryModulesKey::try_from(COGNITION_MODULE) else {
        return;
    };
    if let Some(KnowledgeEntryModulesValue::Object(module)) = entry.modules.get_mut(&key) {
        // Only a present array is cleared in place; any other shape stays for the destination to
        // validate rather than being masked as an empty history.
        if let Some(Value::Array(records)) = module.get_mut("records") {
            records.clear();
        }
    }
}

fn source_absent<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::KnowledgeEntryNotFound,
        "source knowledge entry not found",
        None,
    )
}

fn source_revision_conflict<T>(entry_id: &str, expected: u64) -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::RevisionConflict,
        format!("shared knowledge entry {entry_id} is not at revision {expected}"),
        None,
    )
}
