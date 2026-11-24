//! Supported adoption of a shared knowledge entry into the destination collection.
//!
//! The helper takes the caller's two scoped ports: the source collection is the one `src` serves,
//! the destination is the one `dst` serves. Neither port is selected from the payload, so one
//! host-supplied pairing decides where a copy can land.
//!
//! A served host wires `dst` to the one collection the knowledge face serves, and that collection
//! is shared by every peer admitted to the scope: it holds one slot per `destination_entry_id`, and
//! the first adoption of a given id claims it. A repeat answers `already_present` with that stored
//! copy only at the anchor revision the copy records — a copy adopted at another revision is
//! `RevisionConflict` naming `adopted_revision` — and a peer whose admitted selection hides the
//! copy is answered the occupancy refusal, not the copy. The helper itself is per-slot, not
//! per-caller: it takes no principal, so two callers naming one slot contend for the same row.
//!
//! A create-only destination answers occupied for an occupied id and success for a free one, so the
//! refusal itself reports slot occupancy. That disclosure is permitted and no wider than the
//! knowledge face the copy lands on: a create-only `KnowledgeEntryPort::put_knowledge_entry` over
//! the same face already distinguishes an occupied id from a free one while hiding the entry, so
//! adoption reports occupancy only — never the occupying entry's content, revision or visibility.
//!
//! The destination key is `destination_entry_id`, which the caller has already resolved: a served
//! call defaults it to the source's resolved terminal entry id, and the request field is only the
//! caller's override. A repeat call recognizes the copy by the protocol adoption anchor it carries
//! and returns it instead of writing a second one, so the guarantee leans on the destination port's
//! atomic create-only CAS; without that CAS the slot can be claimed twice.

use serde_json::{Map, Value};

use mindcore_base::knowledge::{
    adoption_anchor, adoption_anchor_parts, adoption_fault, SERVED_REVISION_BOUND,
};
use spoke_operations::{
    knowledge_entry_visible_to_viewpoint, spoke_reject, KnowledgeEntryPort, SpokeReject,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::knowledge_entry::KnowledgeEntryModulesKey;
use spoke_schemas::KnowledgeEntry;

/// Module namespace holding a carrier's cognition envelope.
const COGNITION_MODULE: &str = "cognition";

/// Presence-only detail a destination adapter attaches to the create refusal for a slot it holds
/// superseded; its value never names the successor.
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

/// Adopts the shared entry `source_entry_id` into the slot `destination_entry_id` of the collection
/// `dst` serves.
///
/// The copy carries an adoption anchor naming `source_scope_id`, the source entry id and the source
/// revision it was taken from, and keeps the source's knowledge metadata, but not its `cognition`
/// module. The source entry is only ever read.
///
/// A missing, superseded or undisclosable source is reported as one generic
/// [`SpokeRejectCode::NotFound`], repeat calls included: no anchor is ever written for a source the
/// caller cannot see now. An occupied slot is recognized only by this adoption's own anchor, and
/// only at the revision that anchor records: a copy adopted at another revision is
/// [`SpokeRejectCode::RevisionConflict`] with `details.adopted_revision` naming the revision it was
/// adopted at, even when the source now sits at the requested one, so a stale copy is never served
/// as the revision asked for. Any other occupant — an anchor naming another scope or entry, a
/// superseded row the live read hides, or an entry a served destination port's admitted selection
/// conceals — is [`SpokeRejectCode::RevisionConflict`] without details, and is never overwritten or
/// refreshed. An empty scope id, an empty `destination_entry_id` and a zero revision name no anchor
/// and are [`SpokeRejectCode::InvalidInput`].
///
/// An embedded helper caller asking for a copy whose matching stored anchor records a revision
/// above the served ceiling gets no conformant conflict to report, so it receives the fixed
/// [`SpokeRejectCode::InternalError`] from [`adoption_fault`] — message
/// `knowledge adoption could not complete`, `details.kind = "store"` — instead of a
/// [`SpokeRejectCode::RevisionConflict`].
pub async fn adopt_knowledge_entry(
    src: &dyn KnowledgeEntryPort,
    dst: &dyn KnowledgeEntryPort,
    source_scope_id: &str,
    source_entry_id: &str,
    destination_entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome> {
    adopt(
        src,
        dst,
        source_scope_id,
        source_entry_id,
        destination_entry_id,
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
    source_entry_id: &str,
    destination_entry_id: &str,
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
        source_entry_id,
        destination_entry_id,
        expected_source_revision,
    )
    .await
}

/// The one implementation of the adoption rules; both public entries dispatch here.
async fn adopt<S, D>(
    src: &S,
    dst: &D,
    source_scope_id: &str,
    source_entry_id: &str,
    destination_entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome>
where
    S: KnowledgeEntryPort + ?Sized,
    D: KnowledgeEntryPort + ?Sized,
{
    let Some(anchor) = adoption_anchor(source_scope_id, source_entry_id, expected_source_revision)
    else {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "an adoption anchors a non-empty scope id and entry id at a positive source revision",
            None,
        );
    };
    if destination_entry_id.is_empty() {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "an adoption names a non-empty destination entry id",
            None,
        );
    }
    let source = match src.get_knowledge_entry(source_entry_id).await {
        SpokeResult::Ok(source) if knowledge_entry_visible_to_viewpoint(&source, None) => source,
        SpokeResult::Ok(_) => return source_absent(),
        SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::KnowledgeEntryNotFound => {
            return source_absent()
        }
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let visible_source_revision = source.revision;

    let mut copy = match dst.get_knowledge_entry(destination_entry_id).await {
        SpokeResult::Ok(existing) => {
            return classify(
                existing,
                source_scope_id,
                source_entry_id,
                expected_source_revision,
            )
        }
        SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::KnowledgeEntryNotFound => {
            source
        }
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };

    if visible_source_revision != Some(expected_source_revision) {
        return source_revision_conflict(source_entry_id, expected_source_revision);
    }
    copy.entry_id = destination_entry_id.to_string();
    copy.revision = None;
    copy.created_at = None;
    copy.updated_at = None;
    copy.modules.remove(&cognition_module_key());
    copy.source_anchor = None;
    copy.adoption_anchor = Some(anchor);

    match dst.put_knowledge_entry(copy, None).await {
        SpokeResult::Ok(created) => SpokeResult::Ok(AdoptionOutcome::Created(created)),
        // A create-only put may report the raced occupancy as the store's own refusal or as the
        // caller-OCC refusal a remote destination answers an occupied create with; both mean the
        // same slot claim, so both re-read and classify.
        SpokeResult::Reject(reject) if rejects_occupied_create(&reject) => {
            match dst.get_knowledge_entry(destination_entry_id).await {
                SpokeResult::Ok(existing) => classify(
                    existing,
                    source_scope_id,
                    source_entry_id,
                    expected_source_revision,
                ),
                SpokeResult::Reject(refusal) => match refusal.code {
                    // The slot's occupant is not visible to this call: absence and occupancy are
                    // the same unanswered slot claim, so both answer the occupancy refusal.
                    SpokeRejectCode::KnowledgeEntryNotFound
                    | SpokeRejectCode::KnowledgeEntryAlreadyExists => {
                        occupied(destination_entry_id)
                    }
                    // A selection or revision refusal is the destination's own answer, and a
                    // storage fault stays one: forwarding it keeps the refusal hygiene honest.
                    _ => SpokeResult::Reject(refusal),
                },
            }
        }
        SpokeResult::Reject(reject) if rejects_superseded_occupant(&reject) => {
            occupied(destination_entry_id)
        }
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
    source_entry_id: &str,
    expected_source_revision: u64,
) -> SpokeResult<AdoptionOutcome> {
    let adopted = adopted_revision(&existing, source_scope_id, source_entry_id);
    // The anchor's revision is the only carrier of this value onto the wire, so a stored one above
    // the served ceiling has no conformant copy or refusal to report: refuse as a store fault
    // rather than interpolate it.
    if adopted.is_some_and(|adopted| adopted > SERVED_REVISION_BOUND) {
        return SpokeResult::Reject(adoption_fault("store"));
    }
    match adopted {
        Some(adopted) if expected_source_revision == adopted => {
            SpokeResult::Ok(AdoptionOutcome::AlreadyPresent(existing))
        }
        Some(adopted) => {
            let mut details = Map::new();
            details.insert(ADOPTED_REVISION_DETAIL.to_string(), Value::from(adopted));
            spoke_reject(
                SpokeRejectCode::RevisionConflict,
                format!("adopted copy of {source_entry_id} carries source revision {adopted}"),
                Some(details),
            )
        }
        None => occupied(&existing.entry_id),
    }
}

/// The one occupancy refusal: the slot holds something this adoption does not own.
fn occupied<T>(entry_id: &str) -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::RevisionConflict,
        format!("knowledge entry {entry_id} already exists"),
        None,
    )
}

/// Whether a destination refusal reports that a create-only put found the slot occupied.
///
/// The local store names it directly; a remote `RemoteAdapter` destination refuses the create-only
/// CAS in the port's OCC vocabulary (`REVISION_CONFLICT`) before the provider ever sees the put.
/// The code is matched, never the message: a conflict raised for any other reason still re-reads
/// the slot, and a re-read that reports absence or occupancy answers the occupancy, while its other
/// refusals forward as the destination reported them.
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
        && reject
            .details
            .as_ref()
            .is_some_and(|details| details.contains_key(SUPERSEDED_OCCUPANT_DETAIL))
}

/// The revision a copy's adoption anchor records for this source, when it names it.
fn adopted_revision(
    entry: &KnowledgeEntry,
    source_scope_id: &str,
    source_entry_id: &str,
) -> Option<u64> {
    match adoption_anchor_parts(entry) {
        Some((scope_id, source_id, adopted))
            if scope_id == source_scope_id && source_id == source_entry_id =>
        {
            Some(adopted)
        }
        _ => None,
    }
}

fn cognition_module_key() -> KnowledgeEntryModulesKey {
    KnowledgeEntryModulesKey::try_from(COGNITION_MODULE).expect("a valid module key")
}

fn source_absent<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::NotFound,
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

    /// A port answering the store's own absence code, which is the vocabulary a source read
    /// refuses in before the adoption restates it.
    fn absent_from_store<T>() -> SpokeResult<T> {
        spoke_reject(
            SpokeRejectCode::KnowledgeEntryNotFound,
            "knowledge entry not found",
            None,
        )
    }

    /// A port with no entries: every read and write is the generic absence.
    struct AbsentKnowledge;

    #[async_trait::async_trait]
    impl KnowledgeEntryPort for AbsentKnowledge {
        async fn get_knowledge_entry(&self, _entry_id: &str) -> SpokeResult<KnowledgeEntry> {
            absent_from_store()
        }

        async fn put_knowledge_entry(
            &self,
            _entry: KnowledgeEntry,
            _expected_base_revision: Option<u64>,
        ) -> SpokeResult<KnowledgeEntry> {
            absent_from_store()
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
                return absent_from_store();
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

    /// A destination that answers absence, then claims the slot on the put, then answers a fixed
    /// refusal on the re-read: the raced occupant is invisible one step later than the race.
    struct RacedReadRefusal {
        refusal: SpokeRejectCode,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl RacedReadRefusal {
        fn new(refusal: SpokeRejectCode) -> Self {
            Self {
                refusal,
                reads: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl KnowledgeEntryPort for RacedReadRefusal {
        async fn get_knowledge_entry(&self, _entry_id: &str) -> SpokeResult<KnowledgeEntry> {
            match self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 => absent_from_store(),
                _ => spoke_reject(self.refusal, "the re-read refusal", None),
            }
        }

        async fn put_knowledge_entry(
            &self,
            _entry: KnowledgeEntry,
            _expected_base_revision: Option<u64>,
        ) -> SpokeResult<KnowledgeEntry> {
            spoke_reject(
                SpokeRejectCode::KnowledgeEntryAlreadyExists,
                "knowledge entry ke_shared_gate already exists",
                None,
            )
        }
    }

    /// A re-read that refuses the caller's authority is the destination's own answer, not the put
    /// refusal it replaced: the caller must not read a permanent selection denial as a storage
    /// fault.
    #[tokio::test]
    async fn a_raced_slot_whose_reread_is_unauthorized_keeps_the_selection_refusal() {
        match adopt_knowledge_entry(
            &SourceAtTwo(source_at_two()),
            &RacedReadRefusal::new(SpokeRejectCode::NotAuthorized),
            "spc_shared",
            "ke_shared_gate",
            "ke_shared_gate",
            2,
        )
        .await
        {
            SpokeResult::Reject(reject) => {
                assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
                assert_eq!(reject.message, "the re-read refusal");
            }
            other => panic!("a re-read refusal must travel, got {other:?}"),
        }
    }

    /// A re-read that fails for storage reasons keeps its own code, so the runtime's refusal
    /// hygiene turns it into the fixed store fault rather than the put's occupancy refusal.
    #[tokio::test]
    async fn a_raced_slot_whose_reread_is_a_storage_fault_forwards_that_fault() {
        match adopt_knowledge_entry(
            &SourceAtTwo(source_at_two()),
            &RacedReadRefusal::new(SpokeRejectCode::InternalError),
            "spc_shared",
            "ke_shared_gate",
            "ke_shared_gate",
            2,
        )
        .await
        {
            SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InternalError),
            other => panic!("a storage fault must travel, got {other:?}"),
        }
    }

    /// A stored anchor at the served ceiling is still a conformant conflict; one above it has no
    /// conformant refusal to report, so the call answers the fixed store fault instead.
    #[tokio::test]
    async fn a_stored_adopted_revision_above_the_served_bound_is_a_store_fault() {
        match adopt_knowledge_entry(
            &SourceAtTwo(source_at_two()),
            &RacedDestination::at(SERVED_REVISION_BOUND, false),
            "spc_shared",
            "ke_shared_gate",
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
                    Some(SERVED_REVISION_BOUND)
                );
            }
            other => panic!("the ceiling itself stays a conflict, got {other:?}"),
        }

        for out_of_range in [SERVED_REVISION_BOUND + 1, u64::MAX] {
            match adopt_knowledge_entry(
                &SourceAtTwo(source_at_two()),
                &RacedDestination::at(out_of_range, false),
                "spc_shared",
                "ke_shared_gate",
                "ke_shared_gate",
                2,
            )
            .await
            {
                SpokeResult::Reject(reject) => {
                    assert_eq!(reject, adoption_fault("store"));
                    assert!(
                        !reject.message.contains(&out_of_range.to_string()),
                        "the out-of-range revision is never interpolated: {reject:?}"
                    );
                }
                other => panic!("an unservable stored revision must fault, got {other:?}"),
            }
        }
    }
}
