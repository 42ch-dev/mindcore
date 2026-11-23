//! The `spoke-knowledge-adoption` face: one shared source read for the whole surface, and each
//! admitted principal's routed collection as the destination.
//!
//! The port owns the one `BEGIN IMMEDIATE` transaction that covers the scope checks, the
//! retirement-chain resolution, the destination read and the create-only write, and commits only
//! when a copy was written. The copy, repeat and occupancy rules stay in the SPI helper; this
//! module supplies the two borrowed-transaction port handles, the store reads the chain needs and
//! the refusal hygiene.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sqlx::{Sqlite, SqlitePool, Transaction};
use tokio::sync::Mutex;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{adopt_knowledge_entry_typed, AdoptionOutcome};
use mindcore_sqlite::{load_retirement_entry, read_entry, retirement_successors_of, write_entry};
use spoke_operations::{
    resolve_retirement_chain, spoke_reject, AdoptionPort, KnowledgeEntryPort, RetirementChainEntry,
    SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{
    KnowledgeAdoptRequest, KnowledgeAdoptResponse, KnowledgeEntry, Principal, Scope,
};

use crate::AdoptionRoute;

/// The fixed message a storage fault carries onto the wire; the diagnostic stays host-side.
const ADOPTION_FAULT: &str = "knowledge adoption could not complete";

/// The adoption port over one host store.
///
/// `routes` is the one validated route map the workflow binding also holds: a principal resolves
/// to one destination collection for both faces, so an adopted copy lands where that principal's
/// knowledge face reads and writes.
pub(crate) struct AdoptionService {
    pool: SqlitePool,
    routes: Arc<BTreeMap<String, KnowledgeScope>>,
    source: AdoptionRoute,
}

impl AdoptionService {
    pub(crate) fn new(
        pool: SqlitePool,
        routes: Arc<BTreeMap<String, KnowledgeScope>>,
        source: AdoptionRoute,
    ) -> Self {
        Self {
            pool,
            routes,
            source,
        }
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
        let Some(destination) = self.routes.get(principal.principal_id.as_str()) else {
            return not_authorized("principal has no workflow knowledge route");
        };

        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => return SpokeResult::Reject(adoption_fault("begin", error)),
        };
        let outcome = adopt_in(&mut transaction, &self.source, destination, request).await;
        let outcome = match outcome {
            SpokeResult::Ok(outcome) => outcome,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(adoption_refusal(reject)),
        };
        // A repeat write is no write: the recorded copy is served and the reads leave nothing to
        // commit.
        let settled = match &outcome {
            AdoptionOutcome::Created(_) => transaction.commit().await,
            AdoptionOutcome::AlreadyPresent(_) => transaction.rollback().await,
        };
        if let Err(error) = settled {
            return SpokeResult::Reject(adoption_fault("commit", error));
        }
        adopted(outcome)
    }
}

/// The one transaction's work: resolve the requested source to its terminal, then apply the
/// adoption rules over the two scoped handles.
async fn adopt_in(
    transaction: &mut Transaction<'_, Sqlite>,
    source: &AdoptionRoute,
    destination: &KnowledgeScope,
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
    let terminal = match resolve_retirement_chain(
        requested,
        |current: &RetirementChainEntry| {
            let gate = &gate;
            let fault = &fault;
            let entry_id = current.entry_id.clone();
            async move {
                let mut guard = gate.lock().await;
                match retirement_successors_of(&mut guard, collection, &entry_id).await {
                    SpokeResult::Ok(successors) => successors,
                    SpokeResult::Reject(reject) => {
                        *fault.lock().expect("fault slot") = Some(reject);
                        None
                    }
                }
            }
        },
        |entry_id: &str| {
            let gate = &gate;
            let fault = &fault;
            let entry_id = entry_id.to_string();
            async move {
                let mut guard = gate.lock().await;
                match load_retirement_entry(&mut guard, collection, scope_id, &entry_id).await {
                    SpokeResult::Ok(entry) => entry,
                    SpokeResult::Reject(reject) => {
                        *fault.lock().expect("fault slot") = Some(reject);
                        None
                    }
                }
            }
        },
    )
    .await
    {
        SpokeResult::Ok(terminal) => terminal,
        SpokeResult::Reject(reject) => {
            return match fault.into_inner().expect("fault slot") {
                Some(fault) => SpokeResult::Reject(fault),
                None => SpokeResult::Reject(reject),
            };
        }
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
    let src = AdoptionSource {
        collection,
        gate: &gate,
    };
    let dst = AdoptionDestination {
        collection: destination.collection.as_str(),
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

/// The call's write half: the caller's routed destination.
struct AdoptionDestination<'a, 'b, 'tx> {
    collection: &'a str,
    gate: &'b Mutex<&'b mut Transaction<'tx, Sqlite>>,
}

#[async_trait]
impl KnowledgeEntryPort for AdoptionDestination<'_, '_, '_> {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        let mut gate = self.gate.lock().await;
        read_entry(&mut gate, self.collection, entry_id).await
    }

    async fn put_knowledge_entry(
        &self,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        let mut gate = self.gate.lock().await;
        write_entry(&mut gate, self.collection, entry, expected_base_revision).await
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
    SpokeReject {
        code: SpokeRejectCode::InternalError,
        message: ADOPTION_FAULT.into(),
        details: Some(Map::from_iter([(
            "kind".to_string(),
            Value::String(stage.to_string()),
        )])),
    }
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
fn adopted(outcome: AdoptionOutcome) -> SpokeResult<KnowledgeAdoptResponse> {
    let (word, entry) = match outcome {
        AdoptionOutcome::Created(entry) => ("adopted", entry),
        AdoptionOutcome::AlreadyPresent(entry) => ("already_present", entry),
    };
    match serde_json::from_value(json!({ "outcome": word, "entry": entry })) {
        Ok(response) => SpokeResult::Ok(response),
        Err(error) => SpokeResult::Reject(adoption_fault("response", error)),
    }
}

fn source_absent<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::NotFound,
        "source knowledge entry not found",
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

    use serde_json::json;
    use tempfile::TempDir;

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

    /// One store with the shared source's rows and the port over it: the owner is routed to
    /// `wren`, a stranger is admitted by nobody.
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
            Arc::new(BTreeMap::from([(
                "prn_owner".to_string(),
                KnowledgeScope::new(DESTINATION_COLLECTION),
            )])),
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
    async fn a_routed_principal_adopts_once_and_repeats_from_the_stored_anchor() {
        let (_dir, _store, service) = service().await;
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

        let unrouted = refused(
            service
                .adopt_knowledge_entry(
                    &principal("prn_stranger"),
                    &admitted,
                    &request("ke_source", 1),
                )
                .await,
            "a principal with no route",
        );
        assert_eq!(unrouted.code, SpokeRejectCode::NotAuthorized);
        assert!(
            !unrouted.message.contains(DESTINATION_COLLECTION),
            "the refusal names no destination collection: {unrouted:?}"
        );

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
}
