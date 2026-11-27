//! The Node addon over the in-process facade: one store handle per client, the registry entry
//! points, and [`settle`], the one place a `SpokeReject` becomes `code`, `message` and `details`.
//!
//! Every export answers a result object instead of rejecting through napi: a napi rejection
//! carries a message and neither the facade's code nor its details. `index.js` is the one place
//! that object becomes a value or an error.
//!
//! The addon opens no socket and declares no wire shape: each export calls an existing public
//! function of `mindcore-runtime`. The one transaction it opens itself is `ensureSubject`'s, the
//! host duty a memory write needs before `capture`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use napi_derive::napi;
use serde::Serialize;
use serde_json::{json, Map, Value};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::MindId;
use mindcore_runtime::minds::{self, CreateMind, Mind as MindRow, MindStatus};
use mindcore_runtime::{journal_dir, Mind};
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::MindStore;
use spoke_operations::{spoke_reject, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::Principal;

/// What `openClient` is given: the one store file this client serves, the caller every call
/// admits, and the journal root `review` publishes under (`review` refuses without one).
#[napi(object)]
pub struct ClientOptions {
    pub store_path: String,
    pub caller_id: String,
    pub journal_dir: Option<String>,
}

/// What `createMind` is given: the registry's own identity and label, both optional because the
/// registry answers a fresh id and a Mind may go unnamed.
#[napi(object)]
pub struct CreateMindOptions {
    pub id: Option<String>,
    pub display_name: Option<String>,
}

/// One registry row as JavaScript sees it: camelCase, and the status as its own name. The row
/// carries no serde derive, so this is the projection the binding owns.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MindInfo {
    id: String,
    display_name: Option<String>,
    status: &'static str,
    knowledge_collection: String,
    carrier_entry_id: String,
    created_at: String,
}

impl From<&MindRow> for MindInfo {
    fn from(row: &MindRow) -> Self {
        Self {
            id: row.id.as_str().to_string(),
            display_name: row.display_name.clone(),
            status: match row.status {
                MindStatus::Active => "active",
                MindStatus::Frozen => "frozen",
            },
            knowledge_collection: row.knowledge.collection.clone(),
            carrier_entry_id: row.carrier_entry_id.clone(),
            created_at: row.created_at.clone(),
        }
    }
}

/// A refusal's `code`, `message` and `details` — the three members every failure carries, in the
/// form both result objects take.
fn refusal_parts(reject: &SpokeReject) -> (String, String, Option<Value>) {
    (
        reject.code.as_str().to_string(),
        reject.message.clone(),
        reject.details.clone().map(Value::Object),
    )
}

/// A refusal as its result object; `details` is absent when the refusal carries none.
fn refusal(reject: &SpokeReject) -> Value {
    let (code, message, details) = refusal_parts(reject);
    let mut object = Map::new();
    object.insert("ok".to_string(), Value::Bool(false));
    object.insert("code".to_string(), Value::String(code));
    object.insert("message".to_string(), Value::String(message));
    if let Some(details) = details {
        object.insert("details".to_string(), details);
    }
    Value::Object(object)
}

/// One facade answer as its result object: `{ ok: true, value }`, or the refusal above.
///
/// An answer that does not serialize is this addon's own defect and is reported as one, never
/// answered as a refusal the facade made.
fn settle<T: Serialize>(result: SpokeResult<T>) -> Value {
    match result {
        SpokeResult::Ok(value) => match serde_json::to_value(&value) {
            Ok(value) => json!({ "ok": true, "value": value }),
            Err(error) => refusal(&SpokeReject {
                code: SpokeRejectCode::InternalError,
                message: format!("the facade's answer does not serialize: {error}"),
                details: None,
            }),
        },
        SpokeResult::Reject(reject) => refusal(&reject),
    }
}

/// The result object of an export whose value is a handle rather than a wire object; a handle
/// cannot travel inside `serde_json::Value`, so this is `settle`'s shape for the two handles.
/// Only the addon builds this object, so no JavaScript value is ever read back into it.
#[napi(object, object_from_js = false)]
pub struct ClientResult {
    pub ok: bool,
    pub value: Option<MindcoreClient>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub details: Option<Value>,
}

impl ClientResult {
    fn answered(client: MindcoreClient) -> Self {
        Self {
            ok: true,
            value: Some(client),
            code: None,
            message: None,
            details: None,
        }
    }

    fn refused(reject: &SpokeReject) -> Self {
        let (code, message, details) = refusal_parts(reject);
        Self {
            ok: false,
            value: None,
            code: Some(code),
            message: Some(message),
            details,
        }
    }
}

/// The result object of an export whose value is an opened Mind; see [`ClientResult`].
#[napi(object, object_from_js = false)]
pub struct MindResult {
    pub ok: bool,
    pub value: Option<MindView>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub details: Option<Value>,
}

impl MindResult {
    fn answered(view: MindView) -> Self {
        Self {
            ok: true,
            value: Some(view),
            code: None,
            message: None,
            details: None,
        }
    }

    fn refused(reject: &SpokeReject) -> Self {
        let (code, message, details) = refusal_parts(reject);
        Self {
            ok: false,
            value: None,
            code: Some(code),
            message: Some(message),
            details,
        }
    }
}

/// One opened Mind. It has no JavaScript constructor: `createMind` and `getMind` are the only
/// ways to one, and the operations over it are this handle's exported functions.
#[napi]
pub struct MindView {
    /// The facade handle. A private field is not a JavaScript member, so no getter is emitted for
    /// it and the handle never crosses the boundary by itself.
    // temporary: its readers arrive with Task 2 of `.mstar/plans/mind-native-binding.md`.
    #[allow(dead_code)]
    mind: Mind,
}

/// One client: the store handle every call shares, the caller it admits and the journal root its
/// Minds publish under. It has no JavaScript constructor either — `openClient` is the one way.
#[napi]
pub struct MindcoreClient {
    store: Arc<MindStore>,
    caller: CallerId,
    journal_root: Option<PathBuf>,
}

impl MindcoreClient {
    /// A Mind opened for this client publishes into the journal root it was given, and into no
    /// other medium: without a root the handle keeps no publisher and `review` refuses.
    fn publishing(&self, mind: Mind) -> Mind {
        match &self.journal_root {
            Some(root) => mind.with_journal_publisher(journal_dir(root.clone())),
            None => mind,
        }
    }

    async fn open_mind(&self, mind_id: &MindId) -> SpokeResult<MindView> {
        match Mind::open(&self.store, &self.caller, mind_id).await {
            SpokeResult::Ok(mind) => SpokeResult::Ok(MindView {
                mind: self.publishing(mind),
            }),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    /// The client's caller as the adapter's wire `Principal`: the authenticated identity the host
    /// supplied at open, never payload-asserted.
    fn principal(&self) -> SpokeResult<Principal> {
        match serde_json::from_value(json!({
            "principal_id": self.caller.as_str(),
            "extensions": {},
        })) {
            Ok(principal) => SpokeResult::Ok(principal),
            Err(error) => spoke_reject(
                SpokeRejectCode::InvalidInput,
                format!("the caller id is not a principal: {error}"),
                None,
            ),
        }
    }
}

/// Open the one store file this client serves.
///
/// `journalDir`, when given, is created and its entry made durable here: that is the host duty
/// `journalDir` requires, because the publisher serves an existing root and never creates it.
#[napi]
pub async fn open_client(options: ClientOptions) -> ClientResult {
    if let Some(dir) = &options.journal_dir {
        if let Err(error) = durable_root(Path::new(dir)) {
            return ClientResult::refused(&SpokeReject {
                code: SpokeRejectCode::InternalError,
                message: format!("the journal root {dir} is not durable: {error}"),
                details: None,
            });
        }
    }
    match MindStore::open(Path::new(&options.store_path)).await {
        Ok(store) => ClientResult::answered(MindcoreClient {
            store: Arc::new(store),
            caller: CallerId::new(options.caller_id),
            journal_root: options.journal_dir.map(PathBuf::from),
        }),
        Err(error) => ClientResult::refused(&SpokeReject {
            code: SpokeRejectCode::InternalError,
            message: format!(
                "the store {} cannot be opened: {error}",
                options.store_path
            ),
            details: None,
        }),
    }
}

/// Create a Mind owned by this client's caller, and open it: the registry writes the row in its
/// own transaction and the handle then reads it back under the caller's live grants.
#[napi]
pub async fn client_create_mind(
    client: &MindcoreClient,
    options: Option<CreateMindOptions>,
) -> MindResult {
    let options = options.unwrap_or(CreateMindOptions {
        id: None,
        display_name: None,
    });
    let request = CreateMind {
        mind_id: options.id.map(MindId::new),
        display_name: options.display_name,
        owner: client.caller.clone(),
    };
    let created = match minds::create(&client.store, &request).await {
        SpokeResult::Ok(row) => row,
        SpokeResult::Reject(reject) => return MindResult::refused(&reject),
    };
    match client.open_mind(&created.id).await {
        SpokeResult::Ok(view) => MindResult::answered(view),
        SpokeResult::Reject(reject) => MindResult::refused(&reject),
    }
}

/// Every Mind this client's caller holds a live grant in, in `id` order.
#[napi]
pub async fn client_list_minds(client: &MindcoreClient) -> Value {
    settle(
        match minds::list(&client.store, &client.caller).await {
            SpokeResult::Ok(rows) => {
                SpokeResult::Ok(rows.iter().map(MindInfo::from).collect::<Vec<_>>())
            }
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        },
    )
}

/// One Mind this client's caller may see; any other id answers the registry's own `NotFound`.
#[napi]
pub async fn client_get_mind(client: &MindcoreClient, id: String) -> MindResult {
    match client.open_mind(&MindId::new(id)).await {
        SpokeResult::Ok(view) => MindResult::answered(view),
        SpokeResult::Reject(reject) => MindResult::refused(&reject),
    }
}

/// Provision the subject row a memory write needs, the duty an embedding host performs before
/// `capture`: one transaction of the client's pool, committed.
#[napi]
pub async fn client_ensure_subject(
    client: &MindcoreClient,
    mind_id: String,
    subject_id: String,
) -> Value {
    let principal = match client.principal() {
        SpokeResult::Ok(principal) => principal,
        SpokeResult::Reject(reject) => return settle::<()>(SpokeResult::Reject(reject)),
    };
    let mut tx = match client.store.pool().begin().await {
        Ok(tx) => tx,
        Err(error) => {
            return settle::<()>(spoke_reject(
                SpokeRejectCode::InternalError,
                format!("the subject write cannot start: {error}"),
                None,
            ))
        }
    };
    let adapter = MemoryAdapter::new(false);
    let stage = adapter
        .ensure_subject(
            &mut tx,
            &principal,
            json!({ "space_id": mind_id, "subject_id": subject_id }),
        )
        .await;
    let settled = match stage {
        SpokeResult::Ok(MemoryStage::Ready(_)) => match tx.commit().await {
            Ok(()) => SpokeResult::Ok(()),
            Err(error) => SpokeResult::Reject(SpokeReject {
                code: SpokeRejectCode::InternalError,
                message: format!("the subject write does not commit: {error}"),
                details: None,
            }),
        },
        SpokeResult::Ok(stage) => SpokeResult::Reject(SpokeReject {
            code: SpokeRejectCode::InternalError,
            message: format!("the adapter answered an unexpected stage: {stage:?}"),
            details: None,
        }),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    };
    settle(settled)
}

/// The host duty a journal root requires: the root exists, and the entry naming it is durable
/// before the first publication writes into it.
///
/// One level at a time, each level's entry synced before the next is created: the publisher only
/// ever syncs beneath the root, so every entry created here — not just the root's own — is owed
/// its durability by the caller.
fn durable_root(root: &Path) -> std::io::Result<()> {
    if root.exists() {
        return if root.is_dir() {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "exists and is not a directory",
            ))
        };
    }
    // An existing ancestor is where creation starts; a missing one is created first, so its entry
    // is synced by that level's own call.
    if let Some(parent) = root.parent().filter(|p| !p.as_os_str().is_empty()) {
        durable_root(parent)?;
    }
    std::fs::create_dir(root)?;
    // `Path::new("journal").parent()` is the empty path, which names the current directory; only
    // `None` — the filesystem root — has no directory holding its entry.
    match root.parent() {
        None => Ok(()),
        Some(parent) if parent.as_os_str().is_empty() => std::fs::File::open(".")?.sync_all(),
        Some(parent) => std::fs::File::open(parent)?.sync_all(),
    }
}
