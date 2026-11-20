//! Knowledge-only Connect serving proof against SPOKE `spoke-connect =0.17.0`
//! (`remote-adapter` feature).
//!
//! Both peers advertise exactly `["spoke-knowledge"]` in their signed hellos
//! over an in-process `loopback_transport_pair`. The responder attaches the
//! production SQLite-backed [`RoutedKnowledgeStore`] through
//! `RemoteServePortsComposite::knowledge_only`, so the three scoped ops reach
//! the same store the runtime serves later.
//!
//! The host admission maps the authenticated peer to a typed `Scope`; the
//! configured `scope_id` selects the store's one collection. A baseline-only
//! session is denied on the wire, a peer outside its admission or a list that
//! names another scope is refused before storage, and an entry in another
//! collection is indistinguishable from a missing one. A declared viewpoint
//! needs the negotiated `ke-ownership` flag and stays bounded by the admission
//! and the disclosure predicate, and an unsupported scope selection is refused
//! instead of ignored.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use serde_json::{json, Value};
use tempfile::TempDir;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_sqlite::MindStore;
use spoke_connect::core::derive_peer_id_from_ed25519_pubkey;
use spoke_connect::remote::{
    connect_remote_adapter, connect_responder, loopback_transport_pair, ConnectResponder,
    ConnectResponderOptions, KnowledgeAdmission, RemoteAdapter, RemoteAdapterOptions,
    RemoteAdapterState, RemoteIdentity, RemoteKnowledgeFace, RemoteServePorts,
    RemoteServePortsComposite,
};
use spoke_operations::{
    spoke_reject, KnowledgeEntryPort, KnowledgeQueryPort, ScopedKnowledgePort, SpokeReject,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryDisclosure, KnowledgeEntryOwner,
};
use spoke_schemas::{HostCapabilityManifest, KnowledgeEntry, Scope};

const SCOPE_ID: &str = "spoke-scope-001";
const OTHER_SCOPE_ID: &str = "spoke-scope-002";
const COLLECTION: &str = "wren";
const OTHER_COLLECTION: &str = "studio";
const OWNER_VIEWPOINT: &str = "kb_mira";
const FOREIGN_VIEWPOINT: &str = "kb_rival";

/// One store with the served collection plus a second collection the route
/// never reaches, and an owner-private entry only its own viewpoint sees.
struct Fixture {
    _dir: TempDir,
    store: MindStore,
}

impl Fixture {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("knowledge_connect.db"))
            .await
            .expect("open store");
        let wren = store.knowledge(KnowledgeScope::new(COLLECTION));
        for (entry_id, summary) in [("ke_alpha", "alpha shared"), ("ke_beta", "beta shared")] {
            expect_ok(
                wren.put_knowledge_entry(entry(entry_id, summary), None)
                    .await,
                "seed served entry",
            );
        }
        expect_ok(
            wren.put_knowledge_entry(private_entry("ke_mira_private", "mira private"), None)
                .await,
            "seed owner-private entry",
        );
        let studio = store.knowledge(KnowledgeScope::new(OTHER_COLLECTION));
        expect_ok(
            studio
                .put_knowledge_entry(entry("ke_elsewhere", "studio only"), None)
                .await,
            "seed other-collection entry",
        );
        Self { _dir: dir, store }
    }
}

fn entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    KnowledgeEntry {
        body: KnowledgeEntryBody {
            summary: Some(summary.to_string()),
            ..KnowledgeEntryBody::default()
        },
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
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

fn private_entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    let mut entry = entry(entry_id, summary);
    entry.disclosure =
        Some(KnowledgeEntryDisclosure::from_str("owner-private").expect("non-empty disclosure"));
    entry.owner = Some(KnowledgeEntryOwner::try_from(OWNER_VIEWPOINT).expect("non-empty owner"));
    entry
}

fn scope_of(scope_id: &str) -> Scope {
    serde_json::from_value(json!({ "scope_id": scope_id })).expect("scope JSON matches the schema")
}

fn viewpoint_scope(scope_id: &str, viewpoint: &str) -> Scope {
    serde_json::from_value(json!({ "scope_id": scope_id, "viewpoint": viewpoint }))
        .expect("scope JSON matches the schema")
}

/// The served `scope_id` plus one selection the knowledge face does not serve.
fn scope_with(selection: Value) -> Scope {
    let mut scope = json!({ "scope_id": SCOPE_ID });
    scope
        .as_object_mut()
        .expect("scope object")
        .extend(selection.as_object().expect("selection object").clone());
    serde_json::from_value(scope).expect("scope JSON matches the schema")
}

fn knowledge_manifest(host_id: &str, capabilities: &[&str]) -> HostCapabilityManifest {
    serde_json::from_value(json!({
        "schema_version": 1,
        "host_id": host_id,
        "roles": ["data-store"],
        "capabilities": capabilities,
        "namespaces": ["toy_world"],
        "extensions": {},
    }))
    .expect("manifest JSON matches the schema")
}

/// Admission context: only the fixture peer maps to the fixture scope.
fn admission_of(adapter_peer_id: &str, admitted: Scope) -> KnowledgeAdmission {
    let adapter_peer_id = adapter_peer_id.to_string();
    Arc::new(move |peer_id: &str| {
        if peer_id == adapter_peer_id {
            SpokeResult::Ok(admitted.clone())
        } else {
            refuse(peer_id)
        }
    })
}

fn refusing_admission() -> KnowledgeAdmission {
    Arc::new(refuse)
}

fn refuse(peer_id: &str) -> SpokeResult<Scope> {
    spoke_reject(
        SpokeRejectCode::NotAuthorized,
        format!("peer {peer_id} holds no knowledge admission"),
        None,
    )
}

struct KnowledgeOnlySession {
    adapter: Arc<RemoteAdapter>,
    responder: Arc<ConnectResponder>,
    adapter_peer_id: String,
    responder_peer_id: String,
}

/// Establish a real signed loopback session: Ed25519 throwaway identities,
/// signed hellos bound to the peer nonce, signed session snapshot, and a
/// fail-closed allowlist on both ends. Both manifests advertise exactly
/// `capabilities`, and the responder attaches a knowledge face only.
async fn connect_knowledge_session(
    fixture: &Fixture,
    admission: KnowledgeAdmission,
    capabilities: &[&str],
) -> KnowledgeOnlySession {
    let pair = loopback_transport_pair();

    let adapter_seed = [0x42u8; 32];
    let responder_seed = [0xA7u8; 32];
    let adapter_pubkey = SigningKey::from_bytes(&adapter_seed)
        .verifying_key()
        .to_bytes();
    let responder_pubkey = SigningKey::from_bytes(&responder_seed)
        .verifying_key()
        .to_bytes();
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(&adapter_pubkey);
    let responder_peer_id = derive_peer_id_from_ed25519_pubkey(&responder_pubkey);

    let face = Arc::new(RemoteKnowledgeFace {
        store: Arc::new(
            fixture
                .store
                .served_knowledge(SCOPE_ID, KnowledgeScope::new(COLLECTION)),
        ),
        admission,
    });
    let ports: Arc<dyn RemoteServePorts + Send + Sync> =
        Arc::new(RemoteServePortsComposite::knowledge_only(face));

    let responder = connect_responder(ConnectResponderOptions {
        transport: Arc::new(pair.server),
        identity: RemoteIdentity {
            seed: responder_seed,
        },
        manifest: knowledge_manifest("connect-knowledge-only-responder", capabilities),
        allowlist: vec![adapter_peer_id.clone()],
        peer_keys: HashMap::from([(adapter_peer_id.clone(), adapter_pubkey)]),
        ports: Some(ports),
        invoke_timeout_ms: Some(10_000),
    })
    .await;

    let adapter = connect_remote_adapter(RemoteAdapterOptions {
        transport: Arc::new(pair.client),
        local_identity: RemoteIdentity { seed: adapter_seed },
        local_manifest: knowledge_manifest("connect-knowledge-only-adapter", capabilities),
        remote_pubkey: responder_pubkey,
        allowlist: vec![responder_peer_id.clone()],
        invoke_timeout_ms: Some(10_000),
        capability_token: None,
    })
    .await
    .expect("signed Knowledge-only loopback handshake must succeed");

    KnowledgeOnlySession {
        adapter,
        responder,
        adapter_peer_id,
        responder_peer_id,
    }
}

impl Drop for KnowledgeOnlySession {
    fn drop(&mut self) {
        self.adapter.close();
        self.responder.close();
    }
}

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        SpokeResult::Reject(reject) => reject,
    }
}

fn assert_op_unsupported<T: std::fmt::Debug>(result: SpokeResult<T>, op: &str) {
    let reject = expect_reject(result, op);
    assert_eq!(
        reject.code,
        SpokeRejectCode::CapabilityPortMissing,
        "unnegotiated knowledge op {op} must map to the capability-port-missing reject"
    );
    let details = reject.details.expect("dispatch deny carries details");
    assert_eq!(
        details.get("wire_code").and_then(Value::as_str),
        Some("op_unsupported"),
        "pinned raw reject code for the unnegotiated knowledge op {op}"
    );
    assert!(
        reject.message.contains(op),
        "reject message names the refused op: {}",
        reject.message
    );
}

/// Signed Knowledge-only negotiation: both sides observe the peer manifest
/// advertising exactly `["spoke-knowledge"]` — no baseline, no memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knowledge_only_signed_negotiation() {
    let fixture = Fixture::open().await;
    let session = connect_knowledge_session(
        &fixture,
        admission_of("pending", scope_of(SCOPE_ID)),
        &["spoke-knowledge"],
    )
    .await;

    assert_eq!(session.adapter.state(), RemoteAdapterState::Established);
    assert_eq!(
        session.adapter.remote_peer_id().as_deref(),
        Some(session.responder_peer_id.as_str())
    );
    let remote_manifest = session
        .adapter
        .remote_manifest()
        .expect("responder manifest present after handshake");
    assert_eq!(remote_manifest.capabilities, vec!["spoke-knowledge"]);
    assert!(!remote_manifest
        .capabilities
        .iter()
        .any(|capability| capability == "spoke-baseline"));

    assert_eq!(
        session.responder.remote_peer_id().as_deref(),
        Some(session.adapter_peer_id.as_str())
    );
    let dialer_manifest = session
        .responder
        .remote_manifest()
        .expect("dialer manifest present after handshake");
    assert_eq!(dialer_manifest.capabilities, vec!["spoke-knowledge"]);
}

/// The scoped knowledge face serves all three ops: get, a conditional put
/// (create then compare-and-swap), and the scoped list. A stale revision keeps
/// `StoredRevisionStale` with the stored revision and is not a silent
/// overwrite; the owner-private entry stays out of the plain list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knowledge_only_session_serves_get_conditional_put_and_scoped_list() {
    let fixture = Fixture::open().await;
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(
        &SigningKey::from_bytes(&[0x42u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let session = connect_knowledge_session(
        &fixture,
        admission_of(&adapter_peer_id, scope_of(SCOPE_ID)),
        &["spoke-knowledge"],
    )
    .await;

    let fetched = expect_ok(
        KnowledgeEntryPort::get_knowledge_entry(&*session.adapter, "ke_alpha").await,
        "scoped get",
    );
    assert_eq!(fetched.entry_id, "ke_alpha");
    assert_eq!(fetched.revision, Some(1));

    let created = expect_ok(
        KnowledgeEntryPort::put_knowledge_entry(
            &*session.adapter,
            entry("ke_gamma", "gamma created"),
            None,
        )
        .await,
        "scoped create",
    );
    assert_eq!(created.revision, Some(1));

    let updated = expect_ok(
        KnowledgeEntryPort::put_knowledge_entry(
            &*session.adapter,
            entry("ke_gamma", "gamma revised"),
            Some(1),
        )
        .await,
        "conditional update",
    );
    assert_eq!(updated.revision, Some(2));

    let stale = expect_reject(
        KnowledgeEntryPort::put_knowledge_entry(
            &*session.adapter,
            entry("ke_gamma", "silent overwrite"),
            Some(1),
        )
        .await,
        "stale conditional update",
    );
    assert_eq!(stale.code, SpokeRejectCode::StoredRevisionStale);
    let details = stale.details.expect("stale reject carries the revisions");
    assert_eq!(
        details
            .get("expected_base_revision")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        details.get("stored_revision").and_then(Value::as_u64),
        Some(2),
        "the stale refusal preserves the stored revision"
    );

    let listed = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(&*session.adapter, &scope_of(SCOPE_ID)).await,
        "scoped list",
    );
    assert_eq!(
        listed
            .iter()
            .map(|entry| entry.entry_id.as_str())
            .collect::<Vec<_>>(),
        vec!["ke_alpha", "ke_beta", "ke_gamma"],
        "the list is the served collection in entry_id order, owner-private withheld"
    );
    let gamma = listed
        .iter()
        .find(|entry| entry.entry_id == "ke_gamma")
        .expect("the updated entry is listed");
    assert_eq!(gamma.body.summary.as_deref(), Some("gamma revised"));
}

/// A session that negotiates `spoke-baseline` only is denied every knowledge
/// op with the wire `op_unsupported`, exposed as `CapabilityPortMissing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_only_session_is_denied_for_every_knowledge_op() {
    let fixture = Fixture::open().await;
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(
        &SigningKey::from_bytes(&[0x42u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let session = connect_knowledge_session(
        &fixture,
        admission_of(&adapter_peer_id, scope_of(SCOPE_ID)),
        &["spoke-baseline"],
    )
    .await;

    assert_op_unsupported(
        KnowledgeEntryPort::get_knowledge_entry(&*session.adapter, "ke_alpha").await,
        "port.knowledge.get",
    );
    assert_op_unsupported(
        KnowledgeEntryPort::put_knowledge_entry(
            &*session.adapter,
            entry("ke_created", "must not be written"),
            None,
        )
        .await,
        "port.knowledge.put",
    );
    assert_op_unsupported(
        KnowledgeQueryPort::list_knowledge_entries(&*session.adapter, &scope_of(SCOPE_ID)).await,
        "port.scope.list_knowledge_entries",
    );
}

/// A list naming another scope and a peer the admission refuses both answer
/// `NotAuthorized` with no target details, before the store is reached: the
/// unadmitted get names an entry that exists and still learns nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_scope_list_and_unadmitted_peer_are_not_authorized() {
    let fixture = Fixture::open().await;
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(
        &SigningKey::from_bytes(&[0x42u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let admitted = connect_knowledge_session(
        &fixture,
        admission_of(&adapter_peer_id, scope_of(SCOPE_ID)),
        &["spoke-knowledge"],
    )
    .await;

    let widened = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(&*admitted.adapter, &scope_of(OTHER_SCOPE_ID))
            .await,
        "cross-scope list",
    );
    assert_eq!(widened.code, SpokeRejectCode::NotAuthorized);
    assert!(
        widened.details.is_none(),
        "the cross-scope refusal carries no target details: {widened:?}"
    );

    let unadmitted =
        connect_knowledge_session(&fixture, refusing_admission(), &["spoke-knowledge"]).await;
    let denied = expect_reject(
        KnowledgeEntryPort::get_knowledge_entry(&*unadmitted.adapter, "ke_alpha").await,
        "unadmitted get",
    );
    assert_eq!(
        denied.code,
        SpokeRejectCode::NotAuthorized,
        "an existing id answers NotAuthorized, not the entry and not a missing-id refusal"
    );
    assert!(denied.details.is_none());
    let denied_list = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(&*unadmitted.adapter, &scope_of(SCOPE_ID)).await,
        "unadmitted list",
    );
    assert_eq!(denied_list.code, SpokeRejectCode::NotAuthorized);
    assert!(denied_list.details.is_none());
}

/// An id that exists only in another collection answers exactly like a
/// missing id: one `KnowledgeEntryNotFound`, no details, no collection name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn entry_in_another_collection_is_indistinguishable_from_missing() {
    let fixture = Fixture::open().await;
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(
        &SigningKey::from_bytes(&[0x42u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let session = connect_knowledge_session(
        &fixture,
        admission_of(&adapter_peer_id, scope_of(SCOPE_ID)),
        &["spoke-knowledge"],
    )
    .await;

    let foreign = expect_reject(
        KnowledgeEntryPort::get_knowledge_entry(&*session.adapter, "ke_elsewhere").await,
        "get of an entry in another collection",
    );
    let missing = expect_reject(
        KnowledgeEntryPort::get_knowledge_entry(&*session.adapter, "ke_absent").await,
        "get of a missing entry",
    );

    assert_eq!(foreign.code, SpokeRejectCode::KnowledgeEntryNotFound);
    assert_eq!(missing.code, foreign.code);
    assert_eq!(foreign.details, missing.details);
    assert!(!foreign.message.contains(OTHER_COLLECTION));
    assert_eq!(
        foreign.message,
        "no knowledge entry ke_elsewhere in the admitted scope"
    );
    assert_eq!(
        missing.message,
        "no knowledge entry ke_absent in the admitted scope"
    );
}

/// The store applies SPOKE's owner-private disclosure: only the owner's
/// viewpoint reads or lists the private entry, and a foreign viewpoint cannot
/// overwrite it while learning no revision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_private_entry_stays_hidden_from_a_foreign_viewpoint() {
    let fixture = Fixture::open().await;
    let served = fixture
        .store
        .served_knowledge(SCOPE_ID, KnowledgeScope::new(COLLECTION));
    let owner = viewpoint_scope(SCOPE_ID, OWNER_VIEWPOINT);
    let foreign = viewpoint_scope(SCOPE_ID, FOREIGN_VIEWPOINT);

    let visible = expect_ok(
        served
            .get_scoped_knowledge_entry(&owner, "ke_mira_private")
            .await,
        "owner viewpoint get",
    );
    assert_eq!(visible.revision, Some(1));

    let hidden = expect_reject(
        served
            .get_scoped_knowledge_entry(&foreign, "ke_mira_private")
            .await,
        "foreign viewpoint get",
    );
    assert_eq!(hidden.code, SpokeRejectCode::KnowledgeEntryNotFound);

    let owner_list = expect_ok(
        served.list_scoped_knowledge_entries(&owner).await,
        "owner viewpoint list",
    );
    assert!(owner_list
        .iter()
        .any(|entry| entry.entry_id == "ke_mira_private"));
    let foreign_list = expect_ok(
        served.list_scoped_knowledge_entries(&foreign).await,
        "foreign viewpoint list",
    );
    assert!(!foreign_list
        .iter()
        .any(|entry| entry.entry_id == "ke_mira_private"));

    let overwrite = expect_reject(
        served
            .put_scoped_knowledge_entry(&foreign, entry("ke_mira_private", "hijacked"), Some(1))
            .await,
        "foreign viewpoint put of a hidden existing row",
    );
    assert_eq!(
        overwrite.code,
        SpokeRejectCode::NotAuthorized,
        "a hidden row is never overwritten and leaks no CAS detail"
    );
    let after = expect_ok(
        served
            .get_scoped_knowledge_entry(&owner, "ke_mira_private")
            .await,
        "owner viewpoint get after the refused overwrite",
    );
    assert_eq!(after.body.summary.as_deref(), Some("mira private"));
    assert_eq!(after.revision, Some(1));
}

/// A refused put names neither the served collection nor whether the id exists:
/// a conditional put of an id that is absent — including one that exists only
/// in another collection — answers exactly like a stale visible row, and a
/// create-only collision names no collection either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_put_names_no_collection() {
    let fixture = Fixture::open().await;
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(
        &SigningKey::from_bytes(&[0x42u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let session = connect_knowledge_session(
        &fixture,
        admission_of(&adapter_peer_id, scope_of(SCOPE_ID)),
        &["spoke-knowledge"],
    )
    .await;

    let foreign = expect_reject(
        KnowledgeEntryPort::put_knowledge_entry(
            &*session.adapter,
            entry("ke_elsewhere", "hidden elsewhere"),
            Some(1),
        )
        .await,
        "conditional put of an id that exists only in another collection",
    );
    let absent = expect_reject(
        KnowledgeEntryPort::put_knowledge_entry(
            &*session.adapter,
            entry("ke_absent", "never written"),
            Some(1),
        )
        .await,
        "conditional put of an absent id",
    );
    assert_eq!(foreign.code, SpokeRejectCode::StoredRevisionStale);
    assert_eq!(absent.code, foreign.code);
    assert_eq!(
        foreign.message, "knowledge entry ke_elsewhere does not hold the expected revision",
        "the stale refusal names no collection"
    );
    assert_eq!(
        absent.message, "knowledge entry ke_absent does not hold the expected revision",
        "an absent id reads exactly like a foreign-only one"
    );
    assert_eq!(
        absent.details, foreign.details,
        "both report the same revisions"
    );
    let details = foreign
        .details
        .clone()
        .expect("stale reject carries revisions");
    assert_eq!(
        details
            .get("expected_base_revision")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        details.get("stored_revision"),
        Some(&Value::Null),
        "a row outside the served collection has no stored revision"
    );

    let collision = expect_reject(
        KnowledgeEntryPort::put_knowledge_entry(
            &*session.adapter,
            entry("ke_alpha", "second create"),
            None,
        )
        .await,
        "create-only put of a visible id",
    );
    assert_eq!(collision.code, SpokeRejectCode::KnowledgeEntryAlreadyExists);
    assert_eq!(
        collision.message, "knowledge entry ke_alpha already exists",
        "the collision refusal names no collection"
    );
    for reject in [&foreign, &absent, &collision] {
        assert!(
            !reject.message.contains("collection"),
            "a refused put never names the served collection: {reject:?}"
        );
    }

    let listed = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(&*session.adapter, &scope_of(SCOPE_ID)).await,
        "list after the refused puts",
    );
    assert_eq!(
        listed
            .iter()
            .map(|entry| entry.entry_id.as_str())
            .collect::<Vec<_>>(),
        vec!["ke_alpha", "ke_beta"],
        "no refused put left a row behind"
    );
    let elsewhere = expect_ok(
        fixture
            .store
            .knowledge(KnowledgeScope::new(OTHER_COLLECTION))
            .get_knowledge_entry("ke_elsewhere")
            .await,
        "the other collection still holds its own entry",
    );
    assert_eq!(elsewhere.body.summary.as_deref(), Some("studio only"));
}

/// A declared viewpoint is served only under the negotiated `ke-ownership`
/// flag: without it the owned list is denied on the wire, while the same list
/// without a viewpoint is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_viewpoint_requires_negotiated_ownership() {
    let fixture = Fixture::open().await;
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(
        &SigningKey::from_bytes(&[0x42u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let session = connect_knowledge_session(
        &fixture,
        admission_of(&adapter_peer_id, scope_of(SCOPE_ID)),
        &["spoke-knowledge"],
    )
    .await;

    expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(&*session.adapter, &scope_of(SCOPE_ID)).await,
        "viewpoint-free list under spoke-knowledge alone",
    );
    let denied = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(
            &*session.adapter,
            &viewpoint_scope(SCOPE_ID, OWNER_VIEWPOINT),
        )
        .await,
        "viewpoint list without the negotiated ownership flag",
    );
    assert_eq!(
        denied.code,
        SpokeRejectCode::CapabilityPortMissing,
        "the frozen ownership conjunct answers the op_unsupported wire deny"
    );
    assert!(
        denied.message.contains("ke-ownership"),
        "the refusal names the missing capability: {}",
        denied.message
    );
}

/// A negotiated viewpoint is honoured only for the admitted one and still
/// cannot widen the query: another scope and another viewpoint are refused, and
/// disclosure keeps the owner-private entry away from a foreign viewpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negotiated_viewpoint_cannot_bypass_admission_or_disclosure() {
    let fixture = Fixture::open().await;
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(
        &SigningKey::from_bytes(&[0x42u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let ownership = &["spoke-knowledge", "ke-ownership"];

    let owner = connect_knowledge_session(
        &fixture,
        admission_of(&adapter_peer_id, viewpoint_scope(SCOPE_ID, OWNER_VIEWPOINT)),
        ownership,
    )
    .await;
    let owner_list = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(
            &*owner.adapter,
            &viewpoint_scope(SCOPE_ID, OWNER_VIEWPOINT),
        )
        .await,
        "owner viewpoint list",
    );
    assert!(
        owner_list
            .iter()
            .any(|entry| entry.entry_id == "ke_mira_private"),
        "the negotiated viewpoint is honoured for the owner"
    );

    let widened = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(
            &*owner.adapter,
            &viewpoint_scope(OTHER_SCOPE_ID, OWNER_VIEWPOINT),
        )
        .await,
        "cross-scope viewpoint list",
    );
    assert_eq!(widened.code, SpokeRejectCode::NotAuthorized);
    assert!(
        widened.details.is_none(),
        "the refusal carries no target details"
    );

    let claimed = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(
            &*owner.adapter,
            &viewpoint_scope(SCOPE_ID, FOREIGN_VIEWPOINT),
        )
        .await,
        "viewpoint the admission does not grant",
    );
    assert_eq!(
        claimed.code,
        SpokeRejectCode::NotAuthorized,
        "the negotiated capability lets the request declare a viewpoint, never pick one"
    );

    let foreign = connect_knowledge_session(
        &fixture,
        admission_of(
            &adapter_peer_id,
            viewpoint_scope(SCOPE_ID, FOREIGN_VIEWPOINT),
        ),
        ownership,
    )
    .await;
    let foreign_list = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(
            &*foreign.adapter,
            &viewpoint_scope(SCOPE_ID, FOREIGN_VIEWPOINT),
        )
        .await,
        "foreign viewpoint list",
    );
    assert!(
        !foreign_list
            .iter()
            .any(|entry| entry.entry_id == "ke_mira_private"),
        "a negotiated viewpoint still obeys the disclosure predicate"
    );
    let hidden = expect_reject(
        KnowledgeEntryPort::get_knowledge_entry(&*foreign.adapter, "ke_mira_private").await,
        "foreign viewpoint get",
    );
    assert_eq!(hidden.code, SpokeRejectCode::KnowledgeEntryNotFound);
}

/// Every op refuses a scope carrying a selection the knowledge face does not
/// serve, instead of silently ignoring it and answering for the bare scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_scope_selections_are_refused_by_the_served_store() {
    let fixture = Fixture::open().await;
    let served = fixture
        .store
        .served_knowledge(SCOPE_ID, KnowledgeScope::new(COLLECTION));
    let selections = [
        (
            "cognition",
            json!({ "cognition": { "view": "recorded_at", "filter": {} } }),
        ),
        (
            "timeline events",
            json!({ "timeline_event_ids": ["tl_one"] }),
        ),
        ("timeline scale", json!({ "timeline_scale": "day" })),
        ("fork", json!({ "fork_id": "fork_one" })),
        ("extensions", json!({ "extensions": { "toy_world": {} } })),
    ];

    for (label, selection) in selections {
        let scope = scope_with(selection);
        let refusals = [
            expect_reject(
                served.get_scoped_knowledge_entry(&scope, "ke_alpha").await,
                "get under an unsupported selection",
            ),
            expect_reject(
                served
                    .put_scoped_knowledge_entry(
                        &scope,
                        entry("ke_delta", "must not be written"),
                        None,
                    )
                    .await,
                "put under an unsupported selection",
            ),
            expect_reject(
                served.list_scoped_knowledge_entries(&scope).await,
                "list under an unsupported selection",
            ),
        ];
        for reject in refusals {
            assert_eq!(
                reject.code,
                SpokeRejectCode::InvalidInput,
                "an unsupported {label} selection is refused, not ignored"
            );
            assert!(
                reject
                    .message
                    .contains("serves no cognition, timeline, fork or extension selection"),
                "the refusal names the unsupported selection: {reject:?}"
            );
            assert!(reject.details.is_none(), "the refusal carries no details");
        }
    }

    assert_eq!(
        expect_reject(
            served
                .get_scoped_knowledge_entry(&scope_of(SCOPE_ID), "ke_delta")
                .await,
            "no refused put wrote a row",
        )
        .code,
        SpokeRejectCode::KnowledgeEntryNotFound
    );
}
