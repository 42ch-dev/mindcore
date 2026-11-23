//! Supported adoption of a shared knowledge entry into a personal collection.
//!
//! The helper takes the caller's two scoped ports: the source collection is the one `src` serves,
//! the destination is the one `dst` serves. Neither port is selected from the payload, so one
//! host-supplied pairing decides where a copy can land.
//!
//! The destination key is the source `entry_id`. A repeat call recognizes the copy by the protocol
//! adoption anchor it carries and returns it instead of writing a second one, so the guarantee
//! leans on the destination port's atomic create-only CAS; without that CAS the slot can be
//! claimed twice.

use serde_json::{Map, Value};

use mindcore_base::knowledge::{adoption_anchor, adoption_anchor_parts};
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

/// Detail naming the source revision the destination copy was actually adopted at.
const ADOPTED_REVISION_DETAIL: &str = "adopted_revision";

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
/// The copy carries an adoption anchor naming `source_scope_id`, the entry id and the source
/// revision it was taken from, and keeps the source's knowledge metadata, but none of its cognition
/// records. The source entry is only ever read.
///
/// A missing, superseded or undisclosable source is reported as one generic
/// [`SpokeRejectCode::KnowledgeEntryNotFound`], repeat calls included: no anchor is ever written
/// for a source the caller cannot see now. An occupied slot is recognized only by this adoption's
/// own anchor, and only at the revision that anchor records: a copy adopted at another revision is
/// [`SpokeRejectCode::RevisionConflict`] with `details.adopted_revision` naming the revision it was
/// adopted at, even when the source now sits at the requested one, so a stale copy is never served
/// as the revision asked for. Any other occupant — an anchor naming another scope or entry, or a
/// superseded row the live read hides — is [`SpokeRejectCode::KnowledgeEntryAlreadyExists`] and is
/// never overwritten or refreshed. An empty scope id, an empty `entry_id` and a zero revision name
/// no anchor and are [`SpokeRejectCode::InvalidInput`].
pub async fn adopt_knowledge_entry(
    src: &dyn KnowledgeEntryPort,
    dst: &dyn KnowledgeEntryPort,
    source_scope_id: &str,
    entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome> {
    adopt(
        src,
        dst,
        source_scope_id,
        entry_id,
        expected_source_revision,
    )
    .await
}

/// The same adoption for callers holding concrete ports.
///
/// Generic dispatch keeps each `&S`/`&D` in the returned future's state, so — unlike the
/// object-safe entry, whose `&dyn` borrow is not `Sync` — this future is `Send` when both ports
/// are; a served handler needs that to box it as `Send`.
pub async fn adopt_knowledge_entry_typed<S, D>(
    src: &S,
    dst: &D,
    source_scope_id: &str,
    entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome>
where
    S: KnowledgeEntryPort + Sync,
    D: KnowledgeEntryPort + Sync,
{
    adopt(
        src,
        dst,
        source_scope_id,
        entry_id,
        expected_source_revision,
    )
    .await
}

/// The one implementation of the adoption rules; both public entries dispatch here.
async fn adopt<S, D>(
    src: &S,
    dst: &D,
    source_scope_id: &str,
    entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome>
where
    S: KnowledgeEntryPort + ?Sized,
    D: KnowledgeEntryPort + ?Sized,
{
    let Some(anchor) = adoption_anchor(source_scope_id, entry_id, expected_source_revision) else {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "an adoption anchors a non-empty scope id and entry id at a positive source revision",
            None,
        );
    };
    let source = match src.get_knowledge_entry(entry_id).await {
        SpokeResult::Ok(source) if knowledge_entry_visible_to_viewpoint(&source, None) => source,
        SpokeResult::Ok(_) => return source_absent(),
        SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::KnowledgeEntryNotFound => {
            return source_absent()
        }
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let visible_source_revision = source.revision;

    let mut copy = match dst.get_knowledge_entry(entry_id).await {
        SpokeResult::Ok(existing) => {
            return classify(
                existing,
                source_scope_id,
                entry_id,
                expected_source_revision,
            )
        }
        SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::KnowledgeEntryNotFound => {
            source
        }
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };

    if visible_source_revision != Some(expected_source_revision) {
        return source_revision_conflict(entry_id, expected_source_revision);
    }
    copy.entry_id = entry_id.to_string();
    copy.revision = None;
    copy.created_at = None;
    copy.updated_at = None;
    empty_cognition_records(&mut copy);
    copy.source_anchor = None;
    copy.adoption_anchor = Some(anchor);

    match dst.put_knowledge_entry(copy, None).await {
        SpokeResult::Ok(created) => SpokeResult::Ok(AdoptionOutcome::Created(created)),
        // A create-only put may report the raced occupancy as the store's own refusal or as the
        // caller-OCC refusal a remote destination answers an occupied create with; both mean the
        // same slot claim, so both re-read and classify.
        SpokeResult::Reject(reject) if rejects_occupied_create(&reject) => {
            match dst.get_knowledge_entry(entry_id).await {
                SpokeResult::Ok(existing) => classify(
                    existing,
                    source_scope_id,
                    entry_id,
                    expected_source_revision,
                ),
                SpokeResult::Reject(_) => SpokeResult::Reject(reject),
            }
        }
        SpokeResult::Reject(reject) if rejects_superseded_occupant(&reject) => occupied(entry_id),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    }
}

/// Answers one occupancy of the destination slot by the anchor rule.
///
/// Only an anchor naming the same source triple belongs to this adoption. The revision the anchor
/// records is the copy's identity, never the source's current revision: a request naming anything
/// else is a stale copy, not the revision it asked for.
fn classify(
    existing: KnowledgeEntry,
    source_scope_id: &str,
    entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome> {
    match adopted_revision(&existing, source_scope_id, entry_id) {
        Some(adopted) if expected_source_revision == adopted => {
            SpokeResult::Ok(AdoptionOutcome::AlreadyPresent(existing))
        }
        Some(adopted) => {
            let mut details = Map::new();
            details.insert(ADOPTED_REVISION_DETAIL.to_string(), Value::from(adopted));
            spoke_reject(
                SpokeRejectCode::RevisionConflict,
                format!("adopted copy of {entry_id} carries source revision {adopted}"),
                Some(details),
            )
        }
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

/// Whether a destination refusal reports that a create-only put found the slot occupied.
///
/// The local store names it directly; a 0.18 `RemoteAdapter` destination refuses the create-only
/// CAS in the port's OCC vocabulary (`REVISION_CONFLICT`) before the provider ever sees the put.
/// The code is matched, never the message: a conflict raised for any other reason still re-reads
/// the slot, and a read that does not establish occupancy forwards the original refusal.
fn rejects_occupied_create(reject: &SpokeReject) -> bool {
    matches!(
        reject.code,
        SpokeRejectCode::KnowledgeEntryAlreadyExists | SpokeRejectCode::RevisionConflict
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

/// The revision a copy's adoption anchor records for this source, when it names it.
fn adopted_revision(entry: &KnowledgeEntry, source_scope_id: &str, entry_id: &str) -> Option<u64> {
    match adoption_anchor_parts(entry) {
        Some((scope_id, source_id, adopted))
            if scope_id == source_scope_id && source_id == entry_id =>
        {
            Some(adopted)
        }
        _ => None,
    }
}

/// Keeps the cognition envelope and its member presence, but no copied judgment rows.
fn empty_cognition_records(entry: &mut KnowledgeEntry) {
    let Ok(key) = KnowledgeEntryModulesKey::try_from(COGNITION_MODULE) else {
        return;
    };
    if let Some(KnowledgeEntryModulesValue::Object(module)) = entry.modules.get_mut(&key) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A port with no entries: every read and write is the generic absence.
    struct AbsentKnowledge;

    #[async_trait::async_trait]
    impl KnowledgeEntryPort for AbsentKnowledge {
        async fn get_knowledge_entry(&self, _entry_id: &str) -> SpokeResult<KnowledgeEntry> {
            source_absent()
        }

        async fn put_knowledge_entry(
            &self,
            _entry: KnowledgeEntry,
            _expected_base_revision: Option<u64>,
        ) -> SpokeResult<KnowledgeEntry> {
            source_absent()
        }
    }

    /// A served handler boxes this future as `Send`; the assertion fails to compile if the typed
    /// entry ever stops holding `Send` state (each `&S` is `Send` only because `S: Sync`).
    #[test]
    fn typed_entry_yields_a_send_future() {
        fn assert_send<T: Send>(_: T) {}

        assert_send(adopt_knowledge_entry_typed(
            &AbsentKnowledge,
            &AbsentKnowledge,
            "spc_shared",
            "ke_entry",
            1,
        ));
    }

    /// A source serving one entry at revision 2.
    struct SourceAtTwo(KnowledgeEntry);

    #[async_trait::async_trait]
    impl KnowledgeEntryPort for SourceAtTwo {
        async fn get_knowledge_entry(&self, _entry_id: &str) -> SpokeResult<KnowledgeEntry> {
            SpokeResult::Ok(self.0.clone())
        }

        async fn put_knowledge_entry(
            &self,
            _entry: KnowledgeEntry,
            _expected_base_revision: Option<u64>,
        ) -> SpokeResult<KnowledgeEntry> {
            panic!("the source is only ever read")
        }
    }

    /// A destination whose slot is empty on the first read but claimed by another adoption before
    /// this one writes: the create refusal then re-reads a copy at another revision.
    struct RacedDestination {
        entry_id: String,
        reads: std::sync::atomic::AtomicUsize,
        /// The source revision the racing adoption anchored its copy at.
        own_revision: u64,
        /// Whether the refusal uses the caller-OCC code a `RemoteAdapter` destination answers an
        /// occupied create-only put with, rather than the store's own occupancy code.
        remote: bool,
    }

    #[async_trait::async_trait]
    impl KnowledgeEntryPort for RacedDestination {
        async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
            if self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                return source_absent();
            }
            let mut copy = self.own_copy();
            copy.entry_id = entry_id.to_string();
            SpokeResult::Ok(copy)
        }

        async fn put_knowledge_entry(
            &self,
            _entry: KnowledgeEntry,
            _expected_base_revision: Option<u64>,
        ) -> SpokeResult<KnowledgeEntry> {
            if self.remote {
                let mut details = serde_json::Map::new();
                details.insert("entry_id".into(), Value::String(self.entry_id.clone()));
                return spoke_reject(
                    SpokeRejectCode::RevisionConflict,
                    format!("Entry already exists: {}", self.entry_id),
                    Some(details),
                );
            }
            spoke_reject(
                SpokeRejectCode::KnowledgeEntryAlreadyExists,
                format!("knowledge entry {} already exists", self.entry_id),
                None,
            )
        }
    }

    impl RacedDestination {
        fn at(own_revision: u64, remote: bool) -> Self {
            Self {
                entry_id: "ke_shared_gate".to_string(),
                reads: std::sync::atomic::AtomicUsize::new(0),
                own_revision,
                remote,
            }
        }

        /// The copy the racing adoption wrote, anchored at `own_revision`.
        fn own_copy(&self) -> KnowledgeEntry {
            let mut copy: KnowledgeEntry = serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "entry_id": self.entry_id,
                "entry_type": "location",
                "canonical_name": "the north gate",
                "status": "confirmed",
                "body": { "summary": "the north gate is shut after dusk" },
                "extensions": {},
                "modules": {},
            }))
            .expect("copy fixture parses");
            copy.adoption_anchor = adoption_anchor("spc_shared", &self.entry_id, self.own_revision);
            copy
        }
    }

    /// A source at revision 2, so a race lost to a copy at revision 1 is distinguishable from one
    /// lost to a copy at the requested revision.
    fn source_at_two() -> KnowledgeEntry {
        let mut source: KnowledgeEntry = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "entry_id": "ke_shared_gate",
            "entry_type": "location",
            "canonical_name": "the north gate",
            "status": "confirmed",
            "body": { "summary": "the north gate is shut after dusk" },
            "extensions": {},
            "modules": {},
        }))
        .expect("source fixture parses");
        source.revision = Some(2);
        source
    }

    #[tokio::test]
    async fn a_race_lost_to_a_copy_at_another_revision_carries_the_adopted_revision() {
        for remote in [false, true] {
            let dst = RacedDestination::at(1, remote);

            match adopt_knowledge_entry(
                &SourceAtTwo(source_at_two()),
                &dst,
                "spc_shared",
                "ke_shared_gate",
                2,
            )
            .await
            {
                SpokeResult::Reject(reject) => {
                    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
                    assert_eq!(
                        reject
                            .details
                            .as_ref()
                            .and_then(|details| details.get(ADOPTED_REVISION_DETAIL))
                            .and_then(Value::as_u64),
                        Some(1),
                        "the create-race classification names the revision the copy was adopted at \
                         (remote refusal: {remote})"
                    );
                }
                other => panic!(
                    "a lost create race must answer the conflict, got {other:?} \
                     (remote refusal: {remote})"
                ),
            }
        }
    }

    #[tokio::test]
    async fn a_race_lost_to_a_copy_at_the_requested_revision_is_already_present() {
        for remote in [false, true] {
            let dst = RacedDestination::at(2, remote);

            match adopt_knowledge_entry(
                &SourceAtTwo(source_at_two()),
                &dst,
                "spc_shared",
                "ke_shared_gate",
                2,
            )
            .await
            {
                SpokeResult::Ok(AdoptionOutcome::AlreadyPresent(copy)) => assert_eq!(
                    adoption_anchor_parts(&copy),
                    Some(("spc_shared", "ke_shared_gate", 2)),
                    "the same-triple winner is returned as the adopted copy (remote refusal: \
                     {remote})"
                ),
                other => panic!(
                    "a same-triple race winner must answer AlreadyPresent, got {other:?} \
                     (remote refusal: {remote})"
                ),
            }
        }
    }
}
