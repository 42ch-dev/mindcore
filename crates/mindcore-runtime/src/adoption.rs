//! The `spoke-knowledge-adoption` face: one shared source read for the whole surface, and the
//! destination read from the registry row of the Mind the caller was admitted to.
//!
//! The port owns the one `BEGIN IMMEDIATE` transaction that covers the grant check, the
//! destination resolution, the retirement-chain resolution, the destination read and the
//! create-only write, and commits only when a copy was written. The copy, repeat and occupancy
//! rules stay in the SPI helper; this module supplies the two borrowed-transaction port handles,
//! the registry and chain reads the transaction needs and the refusal hygiene.

use std::io::Write;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use serde_json::json;
use sqlx::{Sqlite, SqlitePool, Transaction};
use tokio::sync::Mutex;

use mindcore_base::ids::SpaceId;
use mindcore_base::knowledge::{
    adoption_fault as fixed_fault, KnowledgeScope, SERVED_REVISION_BOUND,
};
use mindcore_spi::{adopt_knowledge_entry_typed, AdoptionOutcome};
use mindcore_spoke::authorize_knowledge;
use mindcore_sqlite::minds::registered_collection;
use mindcore_sqlite::{load_retirement_entry, read_entry, retirement_successors_of, write_entry};
use spoke_operations::{
    filter_knowledge_entries_by_scope, resolve_retirement_chain, spoke_reject, AdoptionPort,
    KnowledgeEntryPort, RetirementChainEntry, RetirementTerminal, SpokeReject, SpokeRejectCode,
    SpokeResult,
};
use spoke_schemas::{
    KnowledgeAdoptRequest, KnowledgeAdoptResponse, KnowledgeEntry, Principal, Scope,
};

/// The one grant an adoption needs, and needs on one row: it writes a knowledge entry into the
/// Mind's collection and returns the copy, so it is a knowledge put and a get together.
const ADOPTION_CAPABILITIES: [&str; 2] = ["knowledge_write", "knowledge_read"];

/// The source side of the surface: the one collection adoption reads for every admitted Mind,
/// named on the wire by a host-owned id that lands in each copy's anchor.
///
/// It describes the source only. The destination of every call is the collection the admitted
/// Mind registered, resolved inside the call's transaction, so nothing here names a per-caller or
/// per-surface destination and no route map returns.
pub struct AdoptionRoute {
    /// The host-owned id naming this source on the wire, recorded in every adopted copy's anchor
    /// triple. Callers cannot supply it: this baseline forwards only the configured value.
    pub scope_id: String,
    pub source: KnowledgeScope,
}

/// The adoption port over one host store.
///
/// The destination is not held: each call reads the registry row of the Mind it was admitted to,
/// so a Mind created after the surface was assembled adopts from its first call and no
/// collection is fixed for a principal or for the surface.
pub(crate) struct AdoptionService {
    pool: SqlitePool,
    source: AdoptionRoute,
}

impl AdoptionService {
    pub(crate) fn new(pool: SqlitePool, source: AdoptionRoute) -> Self {
        Self { pool, source }
    }
}

#[async_trait]
impl AdoptionPort for AdoptionService {
    async fn adopt_knowledge_entry(
        &self,
        principal: &Principal,
        scope: &Scope,
        request: &KnowledgeAdoptRequest,
    ) -> SpokeResult<KnowledgeAdoptResponse> {
        // A source scope this surface does not serve is the same generic absence as a missing
        // source: the peer learns nothing about which scopes exist.
        if request.source_scope_id.as_str() != self.source.scope_id {
            return source_absent();
        }
        // Assembly already refuses a source scope that is a registered Mind, but a Mind created
        // afterwards can take that id, so the same decision is made here. It needs no statement.
        if self.source.scope_id == scope.scope_id.as_str() {
            return source_absent();
        }
        // Connect refuses an out-of-scope destination before the port runs; the port keeps the
        // same answer, so a direct caller cannot adopt outside the admitted scope either.
        if request.destination_scope_id.as_str() != scope.scope_id.as_str() {
            return not_authorized();
        }
        if request.source_revision == 0 {
            return spoke_reject(
                SpokeRejectCode::InvalidInput,
                "an adoption anchors a positive source revision",
                None,
            );
        }
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => return SpokeResult::Reject(adoption_fault("begin", error)),
        };
        let outcome = adopt_in(&mut transaction, &self.source, principal, scope, request).await;
        let outcome = match outcome {
            SpokeResult::Ok(outcome) => outcome,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(adoption_refusal(reject)),
        };
        // The response is built before the transaction settles: a copy Connect could not accept is
        // a fault of the same call, never a committed write reported as a transport failure.
        let created = matches!(&outcome, AdoptionOutcome::Created(_));
        let response = match adopted(outcome) {
            SpokeResult::Ok(response) => response,
            SpokeResult::Reject(reject) => {
                let _ = transaction.rollback().await;
                return SpokeResult::Reject(reject);
            }
        };
        // A repeat write is no write: the recorded copy is served and the reads leave nothing to
        // commit.
        let settled = match created {
            true => transaction.commit().await,
            false => transaction.rollback().await,
        };
        if let Err(error) = settled {
            return SpokeResult::Reject(adoption_fault("commit", error));
        }
        SpokeResult::Ok(response)
    }
}

/// The one transaction's work, in step order: the grant first, then the destination, then the
/// source's terminal, then the copy rules over the two scoped handles.
async fn adopt_in(
    transaction: &mut Transaction<'_, Sqlite>,
    source: &AdoptionRoute,
    principal: &Principal,
    admitted: &Scope,
    request: &KnowledgeAdoptRequest,
) -> SpokeResult<AdoptionOutcome> {
    // The grant check stands ahead of every read in the transaction, so a caller without it learns
    // nothing the knowledge face would not tell it. A grant and the copy share one snapshot.
    let space = SpaceId::new(admitted.scope_id.as_str());
    if let SpokeResult::Reject(reject) = authorize_knowledge(
        &mut *transaction,
        principal.principal_id.as_str(),
        &space,
        &ADOPTION_CAPABILITIES,
    )
    .await
    {
        return SpokeResult::Reject(reject);
    }
    // The destination is the admitted Mind's registry row. No row is the target-free refusal; a
    // collection that is the source's own is answered as a missing source, the decision assembly
    // cannot make for a Mind created after it.
    let destination = match registered_collection(&mut **transaction, &admitted.scope_id).await {
        SpokeResult::Ok(Some(collection)) => collection,
        SpokeResult::Ok(None) => return not_authorized(),
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    if destination == source.source.collection {
        return source_absent();
    }

    let gate = Mutex::new(transaction);
    let collection = source.source.collection.as_str();
    let scope_id = source.scope_id.as_str();

    let requested = {
        let mut guard = gate.lock().await;
        load_retirement_entry(
            &mut guard,
            collection,
            scope_id,
            request.source_entry_id.as_str(),
        )
        .await
    };
    let requested = match requested {
        SpokeResult::Ok(Some(requested)) => requested,
        SpokeResult::Ok(None) => return source_absent(),
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    // The resolver's closures answer `None` for both absence and a storage fault, so the fault
    // is recorded here and replaces the retirement refusal the resolver builds from that `None`.
    let fault: StdMutex<Option<SpokeReject>> = StdMutex::new(None);
    let resolved = resolve_retirement_chain(
        requested,
        |current: &RetirementChainEntry| {
            let gate = &gate;
            let fault = &fault;
            let entry_id = current.entry_id.clone();
            async move {
                let mut guard = gate.lock().await;
                chain_lookup(
                    retirement_successors_of(&mut guard, collection, &entry_id).await,
                    fault,
                )
            }
        },
        |entry_id: &str| {
            let gate = &gate;
            let fault = &fault;
            let entry_id = entry_id.to_string();
            async move {
                let mut guard = gate.lock().await;
                chain_lookup(
                    load_retirement_entry(&mut guard, collection, scope_id, &entry_id).await,
                    fault,
                )
            }
        },
    )
    .await;
    let terminal = match chain_outcome(resolved, fault.into_inner().expect("fault slot")) {
        SpokeResult::Ok(terminal) => terminal,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    // A live source must sit at the revision asked for; a retired one is never compared, because
    // the anchor records the terminal's own revision.
    if terminal.source_entry_id == request.source_entry_id.as_str()
        && terminal.source_revision != request.source_revision
    {
        return spoke_reject(
            SpokeRejectCode::RevisionConflict,
            format!(
                "shared knowledge entry {} is not at revision {}",
                request.source_entry_id.as_str(),
                request.source_revision
            ),
            None,
        );
    }

    let destination_id = request
        .destination_entry_id
        .as_deref()
        .map(String::as_str)
        .unwrap_or(terminal.source_entry_id.as_str());
    // The anchor records the terminal's revision, so a value the served wire cannot carry would
    // commit a copy whose response Connect must reject as a transport fault. Refuse before the
    // write instead: the caller's request was in range, the stored source is not servable.
    if terminal.source_revision > SERVED_REVISION_BOUND {
        return spoke_reject(
            SpokeRejectCode::InternalError,
            format!(
                "shared knowledge entry {} sits at revision {} outside the served range",
                terminal.source_entry_id, terminal.source_revision
            ),
            None,
        );
    }
    let src = AdoptionSource {
        collection,
        gate: &gate,
    };
    let dst = AdoptionDestination {
        collection: &destination,
        scope: admitted,
        gate: &gate,
    };
    adopt_knowledge_entry_typed(
        &src,
        &dst,
        &terminal.source_scope_id,
        &terminal.source_entry_id,
        destination_id,
        terminal.source_revision,
    )
    .await
}

/// The call's read half: the shared source.
///
/// The adoption only ever reads the source. `KnowledgeEntryPort` carries a write because the
/// destination needs one, so the source handle refuses that write before it touches the store.
struct AdoptionSource<'a, 'b, 'tx> {
    collection: &'a str,
    gate: &'b Mutex<&'b mut Transaction<'tx, Sqlite>>,
}

#[async_trait]
impl KnowledgeEntryPort for AdoptionSource<'_, '_, '_> {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        let mut gate = self.gate.lock().await;
        read_entry(&mut gate, self.collection, entry_id).await
    }

    async fn put_knowledge_entry(
        &self,
        _entry: KnowledgeEntry,
        _expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        spoke_reject(
            SpokeRejectCode::InternalError,
            "the adoption source is read-only",
            None,
        )
    }
}

/// The call's write half: the admitted Mind's registered collection, under the admitted
/// selection.
///
/// The registry's collection is the selector; the admitted `Scope` additionally refines which
/// entries of that collection this call may serve or write, so an entry the host admitted the
/// scope without is neither returned nor copied. An occupant outside that selection answers the
/// ordinary occupancy refusal, never a distinct "not authorized": the answer reports the slot as
/// taken either way, and never what the occupant holds.
struct AdoptionDestination<'a, 'b, 'tx> {
    collection: &'a str,
    scope: &'a Scope,
    gate: &'b Mutex<&'b mut Transaction<'tx, Sqlite>>,
}

#[async_trait]
impl KnowledgeEntryPort for AdoptionDestination<'_, '_, '_> {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        let mut gate = self.gate.lock().await;
        let entry = read_entry(&mut gate, self.collection, entry_id).await;
        match entry {
            // An occupant outside the admitted selection answers the ordinary occupancy refusal,
            // byte for byte the one a visible occupant gets: the answer reports the slot as taken
            // either way, and never what the occupant holds.
            SpokeResult::Ok(entry) => match admitted_destination(&entry, self.scope) {
                SpokeResult::Ok(true) => SpokeResult::Ok(entry),
                SpokeResult::Ok(false) => occupied(&entry.entry_id),
                SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
            },
            // `read_entry` is live-only, so a retired row arrives here as an absence. It is still a
            // taken slot: answering `NotFound` would let the SPI helper's revision gate report the
            // destination's retirement while a live occupant answers occupancy before that gate.
            SpokeResult::Reject(reject)
                if reject.code == SpokeRejectCode::KnowledgeEntryNotFound =>
            {
                match retirement_successors_of(&mut gate, self.collection, entry_id).await {
                    SpokeResult::Ok(Some(_)) => occupied(entry_id),
                    SpokeResult::Ok(None) => SpokeResult::Reject(reject),
                    SpokeResult::Reject(fault) => SpokeResult::Reject(fault),
                }
            }
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    async fn put_knowledge_entry(
        &self,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        // A copy outside the admitted selection is not a slot this call may claim; it answers the
        // same occupancy as an occupied slot, so a free-but-unusable slot is not distinguishable
        // from an occupied one either.
        match admitted_destination(&entry, self.scope) {
            SpokeResult::Ok(true) => {}
            SpokeResult::Ok(false) => return occupied(&entry.entry_id),
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        let mut gate = self.gate.lock().await;
        write_entry(&mut gate, self.collection, entry, expected_base_revision).await
    }
}

/// Whether one destination entry passes the admitted scope's selection: the same SPOKE predicate
/// the routed knowledge face applies to its own served reads and writes.
fn admitted_destination(entry: &KnowledgeEntry, scope: &Scope) -> SpokeResult<bool> {
    match filter_knowledge_entries_by_scope(std::slice::from_ref(entry), scope) {
        SpokeResult::Ok(visible) => SpokeResult::Ok(!visible.is_empty()),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    }
}

/// A storage fault under adoption.
///
/// The wire carries a fixed message and the failing stage: a raw sqlx or decoder diagnostic names
/// local tables, columns and stored payloads, so it goes to the host's stderr and never into the
/// signed refusal.
fn adoption_fault(stage: &str, diagnostic: impl std::fmt::Display) -> SpokeReject {
    let _ = writeln!(
        std::io::stderr(),
        "mindcore adoption {stage} fault: {diagnostic}"
    );
    fixed_fault(stage)
}

/// The contract's own refusals travel as they are; anything else a port reports is a storage or
/// stored-payload fault, and its text stays host-side.
fn adoption_refusal(reject: SpokeReject) -> SpokeReject {
    match reject.code {
        SpokeRejectCode::NotFound
        | SpokeRejectCode::NotAuthorized
        | SpokeRejectCode::RevisionConflict
        | SpokeRejectCode::SourceRetired
        | SpokeRejectCode::AdoptionAnchorImmutable
        | SpokeRejectCode::InvalidInput => reject,
        _ => adoption_fault("store", reject.message),
    }
}

/// The adopted copy as the response envelope.
///
/// Built before the transaction settles, so a copy the served wire cannot describe refuses the
/// call instead of committing and then failing as a transport fault.
fn adopted(outcome: AdoptionOutcome) -> SpokeResult<KnowledgeAdoptResponse> {
    let (word, entry) = match outcome {
        AdoptionOutcome::Created(entry) => ("adopted", entry),
        AdoptionOutcome::AlreadyPresent(entry) => ("already_present", entry),
    };
    if !served_entry_revision(&entry) {
        return SpokeResult::Reject(adoption_fault(
            "response",
            format!(
                "adopted copy {} carries a revision outside the served range",
                entry.entry_id
            ),
        ));
    }
    match serde_json::from_value(json!({ "outcome": word, "entry": entry })) {
        Ok(response) => SpokeResult::Ok(response),
        Err(error) => SpokeResult::Reject(adoption_fault("response", error)),
    }
}

/// Whether the served response's own revision member sits inside the served range. A created copy
/// is assigned by the store, but a destination slot that already held this adoption can carry a
/// stored revision above it.
fn served_entry_revision(entry: &KnowledgeEntry) -> bool {
    entry
        .revision
        .is_none_or(|revision| revision <= SERVED_REVISION_BOUND)
}

/// One chain lookup as the resolver's closures answer it: absence as `None`, and a storage fault
/// recorded in the slot before its own `None`, so the resolver's refusal is not the last word.
fn chain_lookup<T>(
    looked_up: SpokeResult<Option<T>>,
    fault: &StdMutex<Option<SpokeReject>>,
) -> Option<T> {
    match looked_up {
        SpokeResult::Ok(found) => found,
        SpokeResult::Reject(reject) => {
            *fault.lock().expect("fault slot") = Some(reject);
            None
        }
    }
}

/// The resolver's outcome with the recorded fault preferred over it: the resolver builds a
/// `SOURCE_RETIRED` from the `None` a fault produced, so the fault must win or a storage fault
/// would read as retirement.
fn chain_outcome(
    resolved: SpokeResult<RetirementTerminal>,
    fault: Option<SpokeReject>,
) -> SpokeResult<RetirementTerminal> {
    match (resolved, fault) {
        (_, Some(fault)) => SpokeResult::Reject(fault),
        (resolved, None) => resolved,
    }
}

fn source_absent<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::NotFound,
        "source knowledge entry not found",
        None,
    )
}

/// The ordinary occupancy answer: the slot is taken, and it reports nothing about its occupant
/// beyond occupancy. A visible occupant and one the admitted selection hides answer byte-identically
/// — the same disclosure a create-only `put_knowledge_entry` on this face already makes — so the
/// answer never reports the occupying entry's content, revision or visibility.
fn occupied<T>(entry_id: &str) -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::RevisionConflict,
        format!("knowledge entry {entry_id} already exists"),
        None,
    )
}

/// One answer for both refusals that are not a missing source: a destination scope that is not the
/// admitted Mind's, and a caller whose grant does not cover the call. It is the served faces' own
/// fixed refusal — `knowledge scope not authorized`, no `details`, naming no caller, capability or
/// scope — so the two cases are not distinguishable and neither discloses a target.
fn not_authorized<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::NotAuthorized,
        "knowledge scope not authorized",
        None,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU64;

    use serde_json::Map;
    use tempfile::TempDir;

    use mindcore_base::boundary::CallerId;
    use mindcore_base::ids::MindId;
    use mindcore_base::knowledge::ADOPTION_FAULT;

    use mindcore_sqlite::minds::CreateMind;
    use mindcore_sqlite::{read_carrier_header, MindStore};
    use spoke_schemas::knowledge_entry::{
        KnowledgeEntryBody, KnowledgeEntryCanonicalName as CanonicalName,
    };

    use super::*;

    const SHARED_SCOPE: &str = "spc_shared";
    const SOURCE_COLLECTION: &str = "shared";
    const DESTINATION_MIND: &str = "spc_destination";
    /// What `create` assigns a Mind of that id, asserted in [`service`] so a test never names a
    /// collection the registry would not.
    const DESTINATION_COLLECTION: &str = "mind:spc_destination";
    /// The creator, and therefore the holder of the Mind's one default grant.
    const OWNER: &str = "prn_owner";

    fn entry(entry_id: &str) -> KnowledgeEntry {
        KnowledgeEntry {
            adoption_anchor: None,
            body: KnowledgeEntryBody {
                summary: Some(format!("{entry_id} summary")),
                ..KnowledgeEntryBody::default()
            },
            canonical_name: CanonicalName::try_from(format!("{entry_id} name"))
                .expect("non-empty canonical name"),
            created_at: None,
            disclosure: None,
            entry_id: entry_id.to_string(),
            entry_type: "location".to_string(),
            extensions: HashMap::new(),
            modules: HashMap::new(),
            owner: None,
            revision: None,
            schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
            source_anchor: None,
            status: "confirmed".to_string(),
            updated_at: None,
        }
    }

    fn principal(principal_id: &str) -> Principal {
        serde_json::from_value(json!({ "principal_id": principal_id, "extensions": {} }))
            .expect("principal JSON matches the schema")
    }

    fn scope(scope_id: &str) -> Scope {
        serde_json::from_value(json!({ "scope_id": scope_id }))
            .expect("scope JSON matches the schema")
    }

    fn request(source_entry_id: &str, source_revision: u64) -> KnowledgeAdoptRequest {
        serde_json::from_value(json!({
            "source_scope_id": SHARED_SCOPE,
            "source_entry_id": source_entry_id,
            "source_revision": source_revision,
            "destination_scope_id": DESTINATION_MIND,
            "idempotency_key": format!("{source_entry_id}-{source_revision}"),
        }))
        .expect("request JSON matches the schema")
    }

    /// One store with the shared source's rows, one registered Mind through the production
    /// `create` — so its registry row and the owner's default grant are the ones a served Mind
    /// has — and the port over it. The service holds no destination: each call reads the row.
    async fn service() -> (TempDir, MindStore, AdoptionService) {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("adoption.db"))
            .await
            .expect("open store");
        let created = store
            .create_mind(&CreateMind {
                mind_id: Some(MindId::new(DESTINATION_MIND)),
                display_name: None,
                owner: CallerId::new(OWNER),
            })
            .await;
        let mind = ok(created, "create the admitted Mind");
        assert_eq!(mind.knowledge.collection, DESTINATION_COLLECTION);
        seed_source(&store).await;
        let service = AdoptionService::new(
            store.pool(),
            AdoptionRoute {
                scope_id: SHARED_SCOPE.to_string(),
                source: KnowledgeScope::new(SOURCE_COLLECTION),
            },
        );
        (dir, store, service)
    }

    async fn seed_source(store: &MindStore) {
        let shared = store.knowledge(KnowledgeScope::new(SOURCE_COLLECTION));
        for entry_id in ["ke_source", "ke_retired", "ke_terminal"] {
            let written = shared.put_knowledge_entry(entry(entry_id), None).await;
            assert!(
                matches!(written, SpokeResult::Ok(_)),
                "seed shared/{entry_id}: {written:?}"
            );
        }
    }

    /// One space-wide grant, as a host issues one to a peer it admits into `space`.
    async fn grant(store: &MindStore, space: &str, principal_id: &str, capabilities: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, '2026-11-27T00:00:00.000Z')",
        )
        .bind(format!("grt_{principal_id}_{space}"))
        .bind(space)
        .bind(principal_id)
        .bind(json!(capabilities).to_string())
        .execute(&store.pool())
        .await
        .expect("seed grant");
    }

    /// One memory space with no `minds` row: a space a host created and admits a peer to, without
    /// registering it as a Mind. Only the grant check stands between such a caller and the
    /// registry read.
    async fn unregistered_space(store: &MindStore, space: &str) {
        sqlx::query(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) \
             VALUES (?, NULL, 'host_1', 1, 'active', 1, '2026-11-27T00:00:00.000Z')",
        )
        .bind(space)
        .execute(&store.pool())
        .await
        .expect("seed space");
    }

    /// Every row of `collection`, so a refused call can be shown to have written nothing.
    async fn collection_rows(store: &MindStore, collection: &str) -> Vec<(String, i64)> {
        sqlx::query_as(
            "SELECT entry_id, revision FROM knowledge_entries WHERE collection = ? \
             ORDER BY entry_id",
        )
        .bind(collection)
        .fetch_all(&store.pool())
        .await
        .expect("read the collection")
    }

    fn ok<T>(result: SpokeResult<T>, context: &str) -> T {
        match result {
            SpokeResult::Ok(value) => value,
            SpokeResult::Reject(reject) => {
                panic!("{context}: unexpectedly rejected with {reject:?}")
            }
        }
    }

    fn refused<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
        match result {
            SpokeResult::Reject(reject) => reject,
            SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        }
    }

    /// The slot's live row still sits at `revision` and carries `marker`: a refused adoption wrote
    /// nothing, so it neither advanced the revision nor wrote a retirement marker. Reading the live
    /// row rather than a historical revision is what makes that a statement about current state.
    async fn slot_unchanged(
        destination: &mindcore_sqlite::ScopedKnowledgeStore,
        revision: Option<u64>,
        marker: Option<&str>,
        context: &str,
    ) {
        let row = ok(
            destination.get_entry_including_superseded("ke_slot").await,
            "read the destination row",
        );
        assert_eq!(
            (row.entry.revision, row.superseded_by),
            (revision, marker.map(str::to_string)),
            "{context}"
        );
    }

    /// The adoption destination's own read of one slot, at the port boundary the SPI helper calls.
    /// A test reaches it directly because the served face's earlier refusals can intercept a call
    /// before this read runs, which would leave the read itself unpinned.
    async fn destination_read(store: &MindStore, admitted: &Scope, entry_id: &str) -> SpokeReject {
        let mut transaction = store
            .pool()
            .begin()
            .await
            .expect("begin the destination read");
        let gate = Mutex::new(&mut transaction);
        // The port borrows the gate's `&mut transaction`, so the read runs inside this block: its
        // end is what releases the borrow before the rollback below.
        let answer = {
            let port = AdoptionDestination {
                collection: DESTINATION_COLLECTION,
                scope: admitted,
                gate: &gate,
            };
            port.get_knowledge_entry(entry_id).await
        };
        transaction.rollback().await.expect("roll back the read");
        match answer {
            SpokeResult::Reject(reject) => reject,
            SpokeResult::Ok(entry) => panic!("{entry_id}: unexpectedly served {entry:?}"),
        }
    }

    /// The first call writes the copy the anchor names, and the repeat serves that stored copy
    /// instead of writing a second one.
    #[tokio::test]
    async fn an_admitted_principal_adopts_once_and_repeats_from_the_stored_anchor() {
        let (_dir, store, service) = service().await;
        let owner = principal(OWNER);
        let admitted = scope(DESTINATION_MIND);

        let created = ok(
            service
                .adopt_knowledge_entry(&owner, &admitted, &request("ke_source", 1))
                .await,
            "adopt",
        );
        let created = serde_json::to_value(&created).expect("response serializes");
        assert_eq!(created["outcome"], "adopted");
        assert_eq!(created["entry"]["entry_id"], "ke_source");
        assert_eq!(
            created["entry"]["adoption_anchor"]["source_scope_id"],
            SHARED_SCOPE
        );
        assert_eq!(
            created["entry"]["adoption_anchor"]["source_entry_id"],
            "ke_source"
        );
        assert_eq!(created["entry"]["adoption_anchor"]["source_revision"], 1);

        let repeat = ok(
            service
                .adopt_knowledge_entry(&owner, &admitted, &request("ke_source", 1))
                .await,
            "repeat",
        );
        let repeat = serde_json::to_value(&repeat).expect("response serializes");
        assert_eq!(repeat["outcome"], "already_present");
        assert_eq!(repeat["entry"]["revision"], created["entry"]["revision"]);

        let stale = refused(
            service
                .adopt_knowledge_entry(&owner, &admitted, &request("ke_source", 2))
                .await,
            "a stale source revision",
        );
        assert_eq!(stale.code, SpokeRejectCode::RevisionConflict);
        assert!(stale.details.is_none(), "{stale:?}");

        let absent = refused(
            service
                .adopt_knowledge_entry(&owner, &admitted, &request("ke_missing", 1))
                .await,
            "a source the shared collection does not hold",
        );
        assert_eq!(absent.code, SpokeRejectCode::NotFound);
        assert!(absent.details.is_none(), "{absent:?}");

        let mut elsewhere = request("ke_source", 1);
        elsewhere.source_scope_id = "spc_other".parse().expect("scope id");
        let foreign = refused(
            service
                .adopt_knowledge_entry(&owner, &admitted, &elsewhere)
                .await,
            "another source scope",
        );
        assert_eq!(foreign.code, SpokeRejectCode::NotFound);
        assert_eq!(foreign.message, absent.message);

        let unauthorized = refused(
            service
                .adopt_knowledge_entry(&owner, &scope("spc_elsewhere"), &request("ke_source", 1))
                .await,
            "another destination scope",
        );
        assert_eq!(unauthorized.code, SpokeRejectCode::NotAuthorized);
        assert!(unauthorized.details.is_none(), "{unauthorized:?}");

        // The destination is the admitted Mind's registered collection, and a peer the host
        // grants there writes into it, which is where `knowledge.get` reads the copy back.
        grant(
            &store,
            DESTINATION_MIND,
            "prn_second",
            &["knowledge_write", "knowledge_read"],
        )
        .await;
        let second = ok(
            service
                .adopt_knowledge_entry(
                    &principal("prn_second"),
                    &admitted,
                    &request("ke_terminal", 1),
                )
                .await,
            "a second admitted principal",
        );
        assert_eq!(
            serde_json::to_value(&second).expect("response serializes")["entry"]["entry_id"],
            "ke_terminal"
        );
        let stored = ok(
            store
                .knowledge(KnowledgeScope::new(DESTINATION_COLLECTION))
                .get_knowledge_entry("ke_terminal")
                .await,
            "read the second principal's copy from the Mind's registered collection",
        );
        assert_eq!(stored.entry_id, "ke_terminal");

        let zero = refused(
            service
                .adopt_knowledge_entry(&owner, &admitted, &request("ke_source", 0))
                .await,
            "a zero source revision",
        );
        assert_eq!(zero.code, SpokeRejectCode::InvalidInput);
    }

    /// A Mind the registry does not name is the target-free refusal: the call learns nothing about
    /// whether that scope exists, and it is the same answer a missing grant gets.
    ///
    /// The caller here holds a live grant covering both capabilities in a space with no `minds`
    /// row, so the grant check passes and the call reaches `registered_collection`'s `None`. The
    /// assertion therefore fails if that branch is broken or removed: a fallback collection would
    /// adopt the source and answer `adopted`, and a drop of the branch would panic.
    #[tokio::test]
    async fn a_mind_with_no_registry_row_answers_the_target_free_refusal() {
        let (_dir, store, service) = service().await;
        const UNREGISTERED: &str = "spc_unregistered";
        unregistered_space(&store, UNREGISTERED).await;
        grant(
            &store,
            UNREGISTERED,
            "prn_homeless",
            &["knowledge_write", "knowledge_read"],
        )
        .await;
        let mut unregistered = request("ke_source", 1);
        unregistered.destination_scope_id = UNREGISTERED.parse().expect("scope id");

        let source_before = collection_rows(&store, SOURCE_COLLECTION).await;
        let destination_before = collection_rows(&store, DESTINATION_COLLECTION).await;
        let refused_answer = refused(
            service
                .adopt_knowledge_entry(
                    &principal("prn_homeless"),
                    &scope(UNREGISTERED),
                    &unregistered,
                )
                .await,
            "a granted caller in a space with no registry row",
        );
        assert_eq!(refused_answer.code, SpokeRejectCode::NotAuthorized);
        assert!(refused_answer.details.is_none(), "{refused_answer:?}");
        assert_eq!(
            collection_rows(&store, SOURCE_COLLECTION).await,
            source_before,
            "an unregistered Mind writes nothing into the source"
        );
        assert_eq!(
            collection_rows(&store, DESTINATION_COLLECTION).await,
            destination_before,
            "an unregistered Mind writes into no registered collection either"
        );

        // One answer for both cases of the widened rule: the granted owner naming a scope that is
        // not its Mind answers the same bytes.
        let mismatched = refused(
            service
                .adopt_knowledge_entry(
                    &principal(OWNER),
                    &scope("spc_elsewhere"),
                    &request("ke_source", 1),
                )
                .await,
            "a destination scope that is not the admitted Mind's",
        );
        assert_eq!(mismatched, refused_answer);
    }

    /// A destination collection that is the source's own is answered as a missing source: assembly
    /// cannot make that decision for a Mind created after it, so the call does, and the caller
    /// learns nothing about which collection is configured.
    #[tokio::test]
    async fn a_destination_collection_equal_to_the_source_is_not_found() {
        let (_dir, store, _service) = service().await;
        // The same store, with the surface configured to read the collection the Mind registered.
        let colliding = AdoptionService::new(
            store.pool(),
            AdoptionRoute {
                scope_id: SHARED_SCOPE.to_string(),
                source: KnowledgeScope::new(DESTINATION_COLLECTION),
            },
        );

        let refused_answer = refused(
            colliding
                .adopt_knowledge_entry(
                    &principal(OWNER),
                    &scope(DESTINATION_MIND),
                    &request("ke_source", 1),
                )
                .await,
            "a source that is the admitted Mind's own collection",
        );
        assert_eq!(refused_answer.code, SpokeRejectCode::NotFound);
        assert!(refused_answer.details.is_none(), "{refused_answer:?}");
        assert_eq!(
            refused_answer,
            refused(
                colliding
                    .adopt_knowledge_entry(
                        &principal(OWNER),
                        &scope(DESTINATION_MIND),
                        &request("ke_missing", 1),
                    )
                    .await,
                "a source the collection does not hold"
            ),
            "both are the one generic absence"
        );
    }

    /// A configured source scope id that is a registered Mind's own id would let one Mind's write
    /// become another's source, so the call refuses it as a missing source. Assembly refuses the
    /// same shape for the Minds it can see; this is the call-time half for a Mind created after.
    #[tokio::test]
    async fn a_source_scope_naming_the_admitted_mind_is_not_found() {
        let (_dir, store, _service) = service().await;
        let colliding = AdoptionService::new(
            store.pool(),
            AdoptionRoute {
                scope_id: DESTINATION_MIND.to_string(),
                source: KnowledgeScope::new(SOURCE_COLLECTION),
            },
        );
        let mut own_scope = request("ke_source", 1);
        own_scope.source_scope_id = DESTINATION_MIND.parse().expect("scope id");

        let refused_answer = refused(
            colliding
                .adopt_knowledge_entry(&principal(OWNER), &scope(DESTINATION_MIND), &own_scope)
                .await,
            "a source scope that is the admitted Mind's id",
        );
        assert_eq!(refused_answer.code, SpokeRejectCode::NotFound);
        assert!(refused_answer.details.is_none(), "{refused_answer:?}");
    }

    /// The grant check also stands ahead of the registry lookup, which the source-side test above
    /// cannot show: a registry read that ran first would answer a valid row and leave the refusal
    /// unchanged.
    ///
    /// The control makes the registry read's answer visible in the outcome. This service's source
    /// is the registered Mind's own collection, so the row that resolves at
    /// `registered_collection` is exactly the collision the port answers as a missing source. An
    /// unauthorized caller is refused on its grant — `NOT_AUTHORIZED` — and would instead be
    /// answered the generic `NOT_FOUND` if the lookup moved ahead of the check. The authorized
    /// creator reaching that same `NOT_FOUND` proves the discriminating answer is live, so the
    /// unauthorized assertion cannot pass by being unreachable.
    #[tokio::test]
    async fn the_grant_check_precedes_the_registry_lookup_whose_row_names_the_source() {
        let (_dir, store, _service) = service().await;
        let resolved = ok(
            mindcore_sqlite::minds::registered_collection(&store.pool(), DESTINATION_MIND).await,
            "read the registry row the wrong order would reach",
        );
        assert_eq!(
            resolved.as_deref(),
            Some(DESTINATION_COLLECTION),
            "the row is there to be read, so the control is not vacuous"
        );
        let colliding = AdoptionService::new(
            store.pool(),
            AdoptionRoute {
                scope_id: SHARED_SCOPE.to_string(),
                source: KnowledgeScope::new(DESTINATION_COLLECTION),
            },
        );
        let destination_before = collection_rows(&store, DESTINATION_COLLECTION).await;

        let unauthorized = refused(
            colliding
                .adopt_knowledge_entry(
                    &principal("prn_stranger"),
                    &scope(DESTINATION_MIND),
                    &request("ke_source", 1),
                )
                .await,
            "an unauthorized caller into a Mind whose collection is the source",
        );
        assert_eq!(
            unauthorized.code,
            SpokeRejectCode::NotAuthorized,
            "the grant is decided before the registry row is read: {unauthorized:?}"
        );
        assert!(unauthorized.details.is_none(), "{unauthorized:?}");
        assert_eq!(
            collection_rows(&store, DESTINATION_COLLECTION).await,
            destination_before,
            "a refused adoption writes nothing"
        );

        let authorized = refused(
            colliding
                .adopt_knowledge_entry(
                    &principal(OWNER),
                    &scope(DESTINATION_MIND),
                    &request("ke_source", 1),
                )
                .await,
            "the granted creator meeting the same collision",
        );
        assert_eq!(
            authorized.code,
            SpokeRejectCode::NotFound,
            "what a registry-first order would have answered: {authorized:?}"
        );
        assert_ne!(
            unauthorized, authorized,
            "the two orders are distinguishable, so the unauthorized answer pins the order"
        );
    }

    /// The grant check stands ahead of every read: the source holds an undecodable payload, so a
    /// call that read it would answer the fixed store fault. A principal with no grant answers the
    /// refusal instead, and the destination collection is untouched.
    #[tokio::test]
    async fn the_grant_check_refuses_before_the_first_read() {
        let (_dir, store, service) = service().await;
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES ('shared', 'ke_broken', 1, '{ not an entry', \
                     '2026-11-21T00:00:00Z', '2026-11-21T00:00:00Z')",
        )
        .execute(&store.pool())
        .await
        .expect("seed an undecodable stored payload");
        let before = collection_rows(&store, DESTINATION_COLLECTION).await;

        let without_grant = refused(
            service
                .adopt_knowledge_entry(
                    &principal("prn_stranger"),
                    &scope(DESTINATION_MIND),
                    &request("ke_broken", 1),
                )
                .await,
            "a principal with no grant in the Mind's space",
        );
        assert_eq!(without_grant.code, SpokeRejectCode::NotAuthorized);
        assert!(without_grant.details.is_none(), "{without_grant:?}");
        assert_eq!(
            collection_rows(&store, DESTINATION_COLLECTION).await,
            before,
            "a refused adoption writes nothing"
        );

        // The creator's default grant reaches the read, which answers the fault: the two answers
        // prove the check stands ahead of it rather than behind a passing one.
        let reached = refused(
            service
                .adopt_knowledge_entry(
                    &principal(OWNER),
                    &scope(DESTINATION_MIND),
                    &request("ke_broken", 1),
                )
                .await,
            "the granted creator reaching the read",
        );
        assert_eq!(reached.code, SpokeRejectCode::InternalError);
        assert_eq!(reached.message, ADOPTION_FAULT);
    }

    /// A retired source resolves to its live terminal: the copy is written under the terminal and
    /// its anchor names that terminal's triple, whatever revision the request named.
    #[tokio::test]
    async fn a_retired_source_adopts_from_its_live_terminal() {
        let (_dir, store, service) = service().await;
        ok(
            store
                .knowledge(KnowledgeScope::new(SOURCE_COLLECTION))
                .supersede_entry("ke_retired", 1, "ke_terminal")
                .await,
            "retire the source",
        );

        let adopted = ok(
            service
                .adopt_knowledge_entry(
                    &principal(OWNER),
                    &scope(DESTINATION_MIND),
                    &request("ke_retired", 1),
                )
                .await,
            "adopt a retired source",
        );
        let adopted = serde_json::to_value(&adopted).expect("response serializes");
        assert_eq!(adopted["outcome"], "adopted");
        assert_eq!(
            adopted["entry"]["entry_id"], "ke_terminal",
            "the copy takes the terminal's id"
        );
        let anchor = &adopted["entry"]["adoption_anchor"];
        assert_eq!(anchor["source_entry_id"], "ke_terminal");
        assert_eq!(anchor["source_scope_id"], SHARED_SCOPE);
        assert_eq!(anchor["source_revision"], 1);
    }

    /// A storage fault on the requested entry is `INTERNAL_ERROR` with the fixed message, never
    /// the `NOT_FOUND` its absence answers.
    #[tokio::test]
    async fn an_initial_retirement_fault_is_internal_error() {
        let (_dir, store, service) = service().await;
        let seed = store.pool();
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES ('shared', 'ke_broken', 1, '{ not an entry', \
                     '2026-11-21T00:00:00Z', '2026-11-21T00:00:00Z')",
        )
        .execute(&seed)
        .await
        .expect("seed an undecodable stored payload");

        let refused = refused(
            service
                .adopt_knowledge_entry(
                    &principal(OWNER),
                    &scope(DESTINATION_MIND),
                    &request("ke_broken", 1),
                )
                .await,
            "an undecodable source",
        );
        assert_eq!(refused.code, SpokeRejectCode::InternalError);
        assert_eq!(refused.message, ADOPTION_FAULT);
        assert_eq!(
            refused
                .details
                .as_ref()
                .and_then(|details| details.get("kind")),
            Some(&json!("store"))
        );
        assert!(
            !refused.message.contains("payload"),
            "the diagnostic stays host-side: {refused:?}"
        );
    }

    /// A fault on a mid-chain hop is not retirement: the chain cannot be resolved, so the call
    /// answers `INTERNAL_ERROR` rather than `SOURCE_RETIRED`.
    #[tokio::test]
    async fn a_mid_chain_retirement_fault_is_internal_error() {
        let (_dir, store, service) = service().await;
        let seed = store.pool();
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES ('shared', 'ke_broken', 1, '{ not an entry', \
                     '2026-11-21T00:00:00Z', '2026-11-21T00:00:00Z')",
        )
        .execute(&seed)
        .await
        .expect("seed an undecodable stored payload");
        sqlx::query(
            "UPDATE knowledge_entries SET superseded_by = 'ke_broken' \
             WHERE collection = 'shared' AND entry_id = 'ke_source'",
        )
        .execute(&seed)
        .await
        .expect("retire the source into the undecodable hop");

        let refused = refused(
            service
                .adopt_knowledge_entry(
                    &principal(OWNER),
                    &scope(DESTINATION_MIND),
                    &request("ke_source", 1),
                )
                .await,
            "a mid-chain undecodable hop",
        );
        assert_eq!(refused.code, SpokeRejectCode::InternalError);
        assert_eq!(refused.message, ADOPTION_FAULT);
        assert_eq!(
            refused
                .details
                .as_ref()
                .and_then(|details| details.get("kind")),
            Some(&json!("store"))
        );
    }

    /// A recorded fault wins over the `SOURCE_RETIRED` the resolver builds from the `None` that
    /// fault produced; without a fault the resolver's own outcome stands. The successor lookup and
    /// the mid-chain load share this interpretation, so both load sites are covered here.
    #[test]
    fn a_recorded_chain_fault_replaces_the_resolvers_retirement_refusal() {
        let retired = SpokeReject {
            code: SpokeRejectCode::SourceRetired,
            message: "retirement chain is broken: the successor entry is missing".to_string(),
            details: Some(Map::from_iter([(
                "retirement_chain".to_string(),
                json!(["ke_retired"]),
            )])),
        };
        let fault = SpokeReject {
            code: SpokeRejectCode::InternalError,
            message: "knowledge retirement marker could not be read: no such table".to_string(),
            details: None,
        };

        let replaced = refused(
            chain_outcome(SpokeResult::Reject(retired.clone()), Some(fault.clone())),
            "a fault under a retirement refusal",
        );
        assert_eq!(
            replaced, fault,
            "the fault is preferred, not the retirement"
        );
        assert_ne!(
            replaced.code,
            SpokeRejectCode::SourceRetired,
            "a storage fault never reads as retirement"
        );

        let preserved = refused(
            chain_outcome(SpokeResult::Reject(retired.clone()), None),
            "a retirement refusal with no recorded fault",
        );
        assert_eq!(
            preserved, retired,
            "genuine absence or retirement keeps its refusal"
        );

        let terminal = RetirementTerminal {
            source_scope_id: SHARED_SCOPE.to_string(),
            source_entry_id: "ke_terminal".to_string(),
            source_revision: 1,
        };
        let resolved = ok(
            chain_outcome(SpokeResult::Ok(terminal), None),
            "a resolved chain",
        );
        assert_eq!(resolved.source_entry_id, "ke_terminal");
    }

    /// A lookup answers the resolver with absence and records its fault in the shared slot, so the
    /// interpretation above has something to prefer.
    #[test]
    fn a_failed_chain_lookup_records_its_fault_and_answers_absence() {
        let fault = StdMutex::new(None);
        let successors: SpokeResult<Option<Vec<String>>> = spoke_reject(
            SpokeRejectCode::InternalError,
            "knowledge retirement marker could not be read: no such table",
            None,
        );
        assert!(
            chain_lookup(successors, &fault).is_none(),
            "a fault reads as absence to the resolver"
        );
        assert_eq!(
            fault.lock().expect("fault slot").as_ref().map(|f| f.code),
            Some(SpokeRejectCode::InternalError)
        );

        let quiet = StdMutex::new(None);
        assert_eq!(
            chain_lookup(
                SpokeResult::Ok(Some(vec!["ke_terminal".to_string()])),
                &quiet
            ),
            Some(vec!["ke_terminal".to_string()])
        );
        assert!(
            quiet.lock().expect("fault slot").is_none(),
            "a good read records nothing"
        );
        assert!(chain_lookup::<String>(SpokeResult::Ok(None), &quiet).is_none());
        assert!(
            quiet.lock().expect("fault slot").is_none(),
            "absence records nothing"
        );
    }

    /// The adoption only reads its source: the write its port must expose refuses before it
    /// touches the store, and the row keeps the revision it was seeded at.
    #[tokio::test]
    async fn the_adoption_source_handle_refuses_a_write() {
        let (_dir, _store, service) = service().await;
        let mut transaction = service
            .pool
            .begin()
            .await
            .expect("begin the call's transaction");
        let gate = Mutex::new(&mut transaction);
        let src = AdoptionSource {
            collection: SOURCE_COLLECTION,
            gate: &gate,
        };

        let refused = refused(
            src.put_knowledge_entry(entry("ke_source"), None).await,
            "a write through the source handle",
        );
        assert_eq!(refused.code, SpokeRejectCode::InternalError);
        assert_eq!(refused.message, "the adoption source is read-only");

        let header = ok(
            read_carrier_header(&mut *transaction, SOURCE_COLLECTION, "ke_source").await,
            "the shared source keeps its row",
        );
        assert_eq!(header.revision, 1);
        transaction.rollback().await.expect("roll back the read");
    }

    /// An occupied destination slot answers one occupancy whatever the admitted selection makes of
    /// the occupant: a hidden occupant's refusal is byte-identical to a visible one's, so the answer
    /// reports occupancy and never what the occupant holds. A free slot the selection cannot claim
    /// answers the same refusal and writes nothing.
    #[tokio::test]
    async fn a_hidden_occupant_answers_the_same_occupancy_as_a_visible_one() {
        let (_dir, store, service) = service().await;
        let owner = principal(OWNER);
        // An occupant neither anchor names: the SPI answers the ordinary occupancy for it.
        ok(
            store
                .knowledge(KnowledgeScope::new(DESTINATION_COLLECTION))
                .put_knowledge_entry(entry("ke_source"), None)
                .await,
            "seed the destination occupant",
        );
        // The same admitted id, once with an entry-id selection that never holds the occupant.
        let hidden: Scope = serde_json::from_value(json!({
            "scope_id": DESTINATION_MIND,
            "entry_ids": ["ke_somebody_else"],
        }))
        .expect("scope JSON matches the schema");

        let shown = refused(
            service
                .adopt_knowledge_entry(&owner, &scope(DESTINATION_MIND), &request("ke_source", 1))
                .await,
            "a visible occupant",
        );
        let concealed = refused(
            service
                .adopt_knowledge_entry(&owner, &hidden, &request("ke_source", 1))
                .await,
            "a hidden occupant",
        );
        assert_eq!(shown.code, SpokeRejectCode::RevisionConflict);
        assert!(shown.details.is_none(), "{shown:?}");
        assert_eq!(
            concealed, shown,
            "a hidden occupant must not be distinguishable from a visible one"
        );

        // A free slot the selection cannot claim is not adoptable either: same refusal, no row.
        let claimed = refused(
            service
                .adopt_knowledge_entry(&owner, &hidden, &request("ke_terminal", 1))
                .await,
            "a free slot outside the admitted selection",
        );
        assert_eq!(
            claimed.code,
            SpokeRejectCode::RevisionConflict,
            "an unusable free slot answers the occupancy: {claimed:?}"
        );
        assert!(claimed.details.is_none(), "{claimed:?}");
        assert!(
            matches!(
                store
                    .knowledge(KnowledgeScope::new(DESTINATION_COLLECTION))
                    .get_knowledge_entry("ke_terminal")
                    .await,
                SpokeResult::Reject(_)
            ),
            "the unusable slot stays free"
        );

        // The same slot under an admitting selection still creates.
        ok(
            service
                .adopt_knowledge_entry(&owner, &scope(DESTINATION_MIND), &request("ke_terminal", 1))
                .await,
            "the admitting selection claims the free slot",
        );
    }

    /// One destination slot answers the same occupancy whether its occupant is live and admitted,
    /// live and hidden by the admission, or retired. The retired row resolves inside the destination
    /// read rather than through a create-only put, so only this comparison shows both states agree.
    #[tokio::test]
    async fn a_retired_destination_occupant_answers_the_same_occupancy_as_a_live_one() {
        let (_dir, store, service) = service().await;
        let owner = principal(OWNER);
        let destination = store.knowledge(KnowledgeScope::new(DESTINATION_COLLECTION));
        for entry_id in ["ke_slot", "ke_successor"] {
            ok(
                destination.put_knowledge_entry(entry(entry_id), None).await,
                "seed the destination occupant",
            );
        }
        // The same admitted id, once with an entry-id selection that never holds the occupant.
        let hidden: Scope = serde_json::from_value(json!({
            "scope_id": DESTINATION_MIND,
            "entry_ids": ["ke_somebody_else"],
        }))
        .expect("scope JSON matches the schema");
        let mut claim = request("ke_source", 1);
        claim.destination_entry_id = Some("ke_slot".parse().expect("non-empty destination id"));

        let live = refused(
            service
                .adopt_knowledge_entry(&owner, &scope(DESTINATION_MIND), &claim)
                .await,
            "a live occupant",
        );
        slot_unchanged(
            &destination,
            Some(1),
            None,
            "a refused claim of a live slot",
        )
        .await;

        let concealed = refused(
            service.adopt_knowledge_entry(&owner, &hidden, &claim).await,
            "a live occupant the selection hides",
        );
        slot_unchanged(
            &destination,
            Some(1),
            None,
            "a refused claim of a hidden live slot",
        )
        .await;

        // The read the SPI helper reaches after a live occupant answers occupancy. A retired row
        // must answer it identically: reporting an absence here is what lets a revision gate
        // report the destination's retirement while the slot is in fact taken.
        let read_live = destination_read(&store, &hidden, "ke_slot").await;

        let retired = ok(
            destination
                .supersede_entry("ke_slot", 1, "ke_successor")
                .await,
            "retire the occupant",
        );
        let retired_shown = refused(
            service
                .adopt_knowledge_entry(&owner, &scope(DESTINATION_MIND), &claim)
                .await,
            "a retired occupant",
        );
        slot_unchanged(
            &destination,
            Some(retired),
            Some("ke_successor"),
            "a refused claim of a retired slot",
        )
        .await;

        let retired_concealed = refused(
            service.adopt_knowledge_entry(&owner, &hidden, &claim).await,
            "a retired occupant the selection hides",
        );
        slot_unchanged(
            &destination,
            Some(retired),
            Some("ke_successor"),
            "a refused claim of a retired hidden slot",
        )
        .await;

        // An unreachable destination read is what the SPI helper turns into a revision-gate answer,
        // so the port itself must report the retired row as taken, byte for byte as it reports a
        // live one.
        let read_retired = destination_read(&store, &hidden, "ke_slot").await;
        assert_eq!(
            read_retired, read_live,
            "a retired row must read as occupancy, never as an absence"
        );
        assert_eq!(read_retired.code, SpokeRejectCode::RevisionConflict);
        assert!(read_retired.details.is_none(), "{read_retired:?}");

        assert_eq!(live.code, SpokeRejectCode::RevisionConflict);
        assert!(live.details.is_none(), "{live:?}");
        for (state, reject) in [
            ("hidden", &concealed),
            ("retired", &retired_shown),
            ("retired and hidden", &retired_concealed),
        ] {
            assert_eq!(
                reject, &live,
                "a {state} occupant must not be distinguishable from a live visible one"
            );
        }
    }
}
