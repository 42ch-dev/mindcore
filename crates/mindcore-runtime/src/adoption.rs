//! The `spoke-knowledge-adoption` face: one shared source read for the whole surface, and the
//! knowledge face's own collection as the destination.
//!
//! The port owns the one `BEGIN IMMEDIATE` transaction that covers the scope checks, the
//! retirement-chain resolution, the destination read and the create-only write, and commits only
//! when a copy was written. The copy, repeat and occupancy rules stay in the SPI helper; this
//! module supplies the two borrowed-transaction port handles, the store reads the chain needs and
//! the refusal hygiene.

use std::io::Write;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use serde_json::json;
use sqlx::{Sqlite, SqlitePool, Transaction};
use tokio::sync::Mutex;

use mindcore_base::knowledge::{
    adoption_fault as fixed_fault, KnowledgeScope, SERVED_REVISION_BOUND,
};
use mindcore_spi::{adopt_knowledge_entry_typed, AdoptionOutcome};
use mindcore_sqlite::{load_retirement_entry, read_entry, retirement_successors_of, write_entry};
use spoke_operations::{
    filter_knowledge_entries_by_scope, resolve_retirement_chain, spoke_reject, AdoptionPort,
    KnowledgeEntryPort, RetirementChainEntry, RetirementTerminal, SpokeReject, SpokeRejectCode,
    SpokeResult,
};
use spoke_schemas::{
    KnowledgeAdoptRequest, KnowledgeAdoptResponse, KnowledgeEntry, Principal, Scope,
};

use crate::AdoptionRoute;

/// The adoption port over one host store.
///
/// `destination` is the one collection the knowledge face itself serves, shared by every peer the
/// host admits to that scope: the first adoption claims a slot, later adopters of the same source
/// are served that stored copy, and every admitted peer reads it through the face's
/// `knowledge.get`/`list`. It is not the workflow route map: that map selects a *workflow*
/// collection per principal, and the knowledge face takes no per-principal collection.
pub(crate) struct AdoptionService {
    pool: SqlitePool,
    destination: KnowledgeScope,
    source: AdoptionRoute,
}

impl AdoptionService {
    pub(crate) fn new(
        pool: SqlitePool,
        destination: KnowledgeScope,
        source: AdoptionRoute,
    ) -> Self {
        Self {
            pool,
            destination,
            source,
        }
    }
}

#[async_trait]
impl AdoptionPort for AdoptionService {
    async fn adopt_knowledge_entry(
        &self,
        _principal: &Principal,
        scope: &Scope,
        request: &KnowledgeAdoptRequest,
    ) -> SpokeResult<KnowledgeAdoptResponse> {
        // A source scope this surface does not serve is the same generic absence as a missing
        // source: the peer learns nothing about which scopes exist.
        if request.source_scope_id.as_str() != self.source.scope_id {
            return source_absent();
        }
        // Connect refuses an out-of-scope destination before the port runs; the port keeps the
        // same answer, so a direct caller cannot adopt outside the admitted scope either.
        if request.destination_scope_id.as_str() != scope.scope_id.as_str() {
            return not_authorized("adoption destination scope is outside the admitted context");
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
        let outcome = adopt_in(
            &mut transaction,
            &self.source,
            &self.destination,
            scope,
            request,
        )
        .await;
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

/// The one transaction's work: resolve the requested source to its terminal, then apply the
/// adoption rules over the two scoped handles.
async fn adopt_in(
    transaction: &mut Transaction<'_, Sqlite>,
    source: &AdoptionRoute,
    destination: &KnowledgeScope,
    admitted: &Scope,
    request: &KnowledgeAdoptRequest,
) -> SpokeResult<AdoptionOutcome> {
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
        collection: destination.collection.as_str(),
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

/// The call's write half: the knowledge face's own collection, under the admitted selection.
///
/// The host's configured collection is the selector; the admitted `Scope` additionally refines
/// which entries of that collection this call may serve or write, so an entry the host admitted the
/// scope without is neither returned nor copied. An occupant outside that selection answers the
/// ordinary occupancy refusal, never a distinct "not authorized": the caller learns that the slot is
/// taken, not that an entry it cannot see exists there.
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
            // byte for byte the one a visible occupant gets: the caller learns occupied or free,
            // never whether an entry it cannot see holds the slot.
            SpokeResult::Ok(entry) => match admitted_destination(&entry, self.scope) {
                SpokeResult::Ok(true) => SpokeResult::Ok(entry),
                SpokeResult::Ok(false) => occupied(&entry.entry_id),
                SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
            },
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

/// The ordinary occupancy answer: the slot is taken, and the caller learns nothing else about its
/// occupant. A visible occupant and one the admitted selection hides answer byte-identically, so a
/// guessed destination id never reports whether an entry outside the caller's reach exists.
fn occupied<T>(entry_id: &str) -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::RevisionConflict,
        format!("knowledge entry {entry_id} already exists"),
        None,
    )
}

fn not_authorized<T>(message: &str) -> SpokeResult<T> {
    spoke_reject(SpokeRejectCode::NotAuthorized, message, None)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU64;

    use serde_json::{json, Map};
    use tempfile::TempDir;

    use mindcore_base::knowledge::ADOPTION_FAULT;

    use mindcore_sqlite::{read_carrier_header, MindStore};
    use spoke_schemas::knowledge_entry::{
        KnowledgeEntryBody, KnowledgeEntryCanonicalName as CanonicalName,
    };

    use super::*;

    const SHARED_SCOPE: &str = "spc_shared";
    const DESTINATION_SCOPE: &str = "spc_destination";
    const DESTINATION_COLLECTION: &str = "wren";

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
            "destination_scope_id": DESTINATION_SCOPE,
            "idempotency_key": format!("{source_entry_id}-{source_revision}"),
        }))
        .expect("request JSON matches the schema")
    }

    /// One store with the shared source's rows and the port over it: the destination is the
    /// knowledge face's own collection, the same one it serves to every admitted peer.
    async fn service() -> (TempDir, MindStore, AdoptionService) {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("adoption.db"))
            .await
            .expect("open store");
        let shared = store.knowledge(KnowledgeScope::new("shared"));
        for entry_id in ["ke_source", "ke_retired", "ke_terminal"] {
            let written = shared.put_knowledge_entry(entry(entry_id), None).await;
            assert!(
                matches!(written, SpokeResult::Ok(_)),
                "seed shared/{entry_id}: {written:?}"
            );
        }
        let service = AdoptionService::new(
            store.pool(),
            KnowledgeScope::new(DESTINATION_COLLECTION),
            AdoptionRoute {
                scope_id: SHARED_SCOPE.to_string(),
                source: KnowledgeScope::new("shared"),
            },
        );
        (dir, store, service)
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

    /// The first call writes the copy the anchor names, and the repeat serves that stored copy
    /// instead of writing a second one.
    #[tokio::test]
    async fn an_admitted_principal_adopts_once_and_repeats_from_the_stored_anchor() {
        let (_dir, store, service) = service().await;
        let owner = principal("prn_owner");
        let admitted = scope(DESTINATION_SCOPE);

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

        // No route map gates adoption: a second admitted principal adopts into the same collection
        // the knowledge face serves, which is where `knowledge.get` reads it back.
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
            "read the second principal's copy from the knowledge face's collection",
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

    /// A retired source resolves to its live terminal: the copy is written under the terminal and
    /// its anchor names that terminal's triple, whatever revision the request named.
    #[tokio::test]
    async fn a_retired_source_adopts_from_its_live_terminal() {
        let (_dir, store, service) = service().await;
        ok(
            store
                .knowledge(KnowledgeScope::new("shared"))
                .supersede_entry("ke_retired", 1, "ke_terminal")
                .await,
            "retire the source",
        );

        let adopted = ok(
            service
                .adopt_knowledge_entry(
                    &principal("prn_owner"),
                    &scope(DESTINATION_SCOPE),
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
                    &principal("prn_owner"),
                    &scope(DESTINATION_SCOPE),
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
                    &principal("prn_owner"),
                    &scope(DESTINATION_SCOPE),
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
            collection: "shared",
            gate: &gate,
        };

        let refused = refused(
            src.put_knowledge_entry(entry("ke_source"), None).await,
            "a write through the source handle",
        );
        assert_eq!(refused.code, SpokeRejectCode::InternalError);
        assert_eq!(refused.message, "the adoption source is read-only");

        let header = ok(
            read_carrier_header(&mut *transaction, "shared", "ke_source").await,
            "the shared source keeps its row",
        );
        assert_eq!(header.revision, 1);
        transaction.rollback().await.expect("roll back the read");
    }

    /// An occupied destination slot answers one occupancy whatever the admitted selection makes of
    /// the occupant: a hidden occupant's refusal is byte-identical to a visible one's, so a guessed
    /// destination id never reports whether an entry the caller cannot see exists. A free slot the
    /// selection cannot claim answers the same refusal and writes nothing.
    #[tokio::test]
    async fn a_hidden_occupant_answers_the_same_occupancy_as_a_visible_one() {
        let (_dir, store, service) = service().await;
        let owner = principal("prn_owner");
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
            "scope_id": DESTINATION_SCOPE,
            "entry_ids": ["ke_somebody_else"],
        }))
        .expect("scope JSON matches the schema");

        let shown = refused(
            service
                .adopt_knowledge_entry(&owner, &scope(DESTINATION_SCOPE), &request("ke_source", 1))
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
                .adopt_knowledge_entry(
                    &owner,
                    &scope(DESTINATION_SCOPE),
                    &request("ke_terminal", 1),
                )
                .await,
            "the admitting selection claims the free slot",
        );
    }
}
