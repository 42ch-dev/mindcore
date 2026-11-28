//! The Node addon over the in-process facade: one store handle per client, the registry entry
//! points, and [`settle`], the one place a `SpokeReject` becomes `code`, `message` and `details`.
//!
//! Every export answers a result object — `{ ok: true, value }` or
//! `{ ok: false, code, message, details? }` — instead of rejecting through napi, whose rejection
//! carries a message and neither the facade's code nor its details. `index.js` is the one place
//! that object becomes a value or an error.
//!
//! Two failures are outside that contract, and deliberately so. A facade outcome and a guard this
//! addon runs over a JavaScript value (a revision that is not a whole number, an `occurredAt` that
//! is not RFC 3339, a wire object that does not decode) both arrive as a result object. But an
//! argument that does not match the declared `#[napi(object)]` shape at all — a missing `maxItems`,
//! a `content` that is not an object — is refused by napi's own argument validation before any
//! export body runs, and reaches JavaScript as a plain `TypeError` with no `code` and no `details`.
//! A caller that switches on `code` must therefore treat "not a `MindcoreError`" as the
//! argument-shape case.
//!
//! The addon opens no socket and declares no wire shape: each export calls an existing public
//! function of `mindcore-runtime`. The one transaction it opens itself is `ensureSubject`'s, the
//! host duty a memory write needs before `capture`; it begins immediate like every other writer.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use napi_derive::napi;
use serde::Serialize;
use serde_json::{json, Map, Value};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::MindId;
use mindcore_runtime::minds::{self, CreateMind, Mind as MindRow, MindStatus};
use mindcore_runtime::{journal_dir, Capture, Mind, Recall};
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::MindStore;
use spoke_operations::{spoke_reject, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::memory_candidates_review_request::Decision as ReviewDecision;
use spoke_schemas::{
    KnowledgeEntry, MemoryCandidatesReviewResponse, MemoryCaptureResponse, MemoryRecallResponse,
    Principal,
};

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

/// One memory capture as JavaScript supplies it: the wire request's own members, with `content`
/// and `selection` crossing as the plain objects the wire carries. `occurredAt` is RFC 3339.
#[napi(object)]
pub struct CaptureInput {
    pub subject: String,
    pub context: Option<String>,
    pub content: Value,
    pub occurred_at: Option<String>,
    pub kind_hint: Option<String>,
    pub idempotency_key: String,
}

/// One memory recall as JavaScript supplies it. Both bounds are `f64` for the same reason a
/// revision is: a napi `u32` reads through JavaScript's `uint32` conversion, so `-1` would arrive
/// as `4294967295` and `2^32` as `0` — a caller's mistake silently becoming a different budget.
#[napi(object)]
pub struct RecallInput {
    pub subject: String,
    pub selection: Value,
    pub max_items: f64,
    pub max_bytes: Option<f64>,
    pub query: Option<String>,
    pub cursor: Option<String>,
}

/// One registry row as JavaScript sees it: camelCase, and the status as its own name. The row
/// carries no serde derive, so this is the projection the binding owns.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[napi(object, object_from_js = false)]
pub struct MindInfo {
    pub id: String,
    pub display_name: Option<String>,
    pub status: &'static str,
    pub knowledge_collection: String,
    pub carrier_entry_id: String,
    pub created_at: String,
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

/// The code of a failure this addon raises for its own defect. No facade refusal carries it, so a
/// caller that reads `code` can never mistake one for the other.
const BINDING_DEFECT: &str = "BINDING_DEFECT";

/// One facade answer as its result object: `{ ok: true, value }`, or the refusal above.
///
/// An answer that does not serialize is this addon's own defect and is answered as one, under
/// [`BINDING_DEFECT`], never as a refusal the facade made. A refusal's `details` cannot reach
/// this path: it is already a `Value` map, which `to_value` cannot fail on.
fn settle<T: Serialize>(result: SpokeResult<T>) -> Value {
    match result {
        SpokeResult::Ok(value) => match serde_json::to_value(&value) {
            Ok(value) => json!({ "ok": true, "value": value }),
            Err(error) => json!({
                "ok": false,
                "code": BINDING_DEFECT,
                "message": format!(
                    "the binding could not serialize {}: {error}",
                    std::any::type_name::<T>()
                ),
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
    mind: Mind,
}

/// One client: the store handle every call shares, the caller it admits and the journal root its
/// Minds publish under. It has no JavaScript constructor either — `openClient` is the one way.
///
/// There is no `close`: a client lives as long as its host holds it, and the store pool is closed
/// when the client is garbage-collected and the process exits. A host that must release a database
/// file drops every client and view over it; nothing here can do that on the host's behalf, because
/// the store is shared with the views it handed out.
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

    /// The handle over one Mind. A seam the tests use to make the post-commit open fail: the create
    /// path has already committed a row by the time this runs, and that distinction is what the
    /// caller is told.
    async fn opened_mind(&self, mind_id: &MindId) -> SpokeResult<MindView> {
        #[cfg(test)]
        if take_injected_open_failure(mind_id) {
            return spoke_reject(
                SpokeRejectCode::InternalError,
                "the handle could not be read back",
                None,
            );
        }
        match Mind::open(&self.store, &self.caller, mind_id).await {
            SpokeResult::Ok(mind) => SpokeResult::Ok(MindView {
                mind: self.publishing(mind),
            }),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    async fn open_mind(&self, mind_id: &MindId) -> SpokeResult<MindView> {
        self.opened_mind(mind_id).await
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

/// One path the caller named, and the io failure it produced, as the two members a path refusal
/// carries: the member's own name and the failure's kind.
fn path_details(member: &str, path: &Path, kind: std::io::ErrorKind) -> Option<Map<String, Value>> {
    let mut details = Map::new();
    details.insert(
        member.to_string(),
        Value::String(path.display().to_string()),
    );
    details.insert("kind".to_string(), Value::String(kind.to_string()));
    Some(details)
}

/// Which of the two failures an io error on a caller-named path is.
///
/// A path that cannot be used as named — a component that is a regular file, a path that is a
/// directory where a file belongs, an entry that already exists as the wrong type — is the caller's
/// mistake: `INVALID_INPUT`. A walk that could not run because the environment refused it — no
/// permission, no space, an fsync that fails, a path that cannot be resolved — is an environment
/// fault: `INTERNAL_ERROR`. The addon never reports the second as the first: a caller who fixes
/// their spelling and retries would otherwise be told, again, that their input is wrong.
fn path_fault(member: &str, path: &Path, error: &std::io::Error) -> SpokeReject {
    let caller_mistake = matches!(
        error.kind(),
        std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::AlreadyExists
    );
    SpokeReject {
        code: if caller_mistake {
            SpokeRejectCode::InvalidInput
        } else {
            SpokeRejectCode::InternalError
        },
        message: if caller_mistake {
            format!("{member} {} cannot be used: {error}", path.display())
        } else {
            format!("{member} {} could not be prepared: {error}", path.display())
        },
        details: path_details(member, path, error.kind()),
    }
}

/// A store path that cannot be the file the caller asked for, as the refusal it is.
///
/// The store file is this client's own, and its name is the caller's to spell: a directory at that
/// name, a parent that refuses writes, a read-only file — every one of these is fixed by naming a
/// different path, so every one answers `INVALID_INPUT` with `storePath` and the io kind. This is
/// deliberately not [`path_fault`]'s split: the journal walk touches a directory tree whose refusals
/// can be environmental, while nothing about a store file is outside the caller's control.
fn store_path_refusal(path: &Path, error: &std::io::Error) -> SpokeReject {
    SpokeReject {
        code: SpokeRejectCode::InvalidInput,
        message: format!("storePath {} cannot be used: {error}", path.display()),
        details: path_details("storePath", path, error.kind()),
    }
}

/// What a store that failed to open actually reported, as the refusal's two members.
///
/// The kind is the failure's own: the io error's `ErrorKind` when sqlx hands one through, otherwise
/// the pool, migration or database error's name, and for a database error its SQLite code — the one
/// fact a host can act on. `"other"` is never invented in place of a real answer.
fn store_open_details(path: &Path, error: &sqlx::Error) -> Option<Map<String, Value>> {
    let kind = match error {
        sqlx::Error::Io(io) => io.kind().to_string(),
        sqlx::Error::Configuration(_) => "configuration".to_string(),
        sqlx::Error::InvalidArgument(_) => "invalid argument".to_string(),
        sqlx::Error::Database(database) => database
            .code()
            .map_or_else(|| "database".to_string(), |code| format!("database {code}")),
        sqlx::Error::Migrate(_) => "migration".to_string(),
        sqlx::Error::PoolTimedOut => "pool timed out".to_string(),
        sqlx::Error::PoolClosed => "pool closed".to_string(),
        sqlx::Error::Protocol(_) => "protocol".to_string(),
        sqlx::Error::RowNotFound => "row not found".to_string(),
        other => other.to_string(),
    };
    let mut details = Map::new();
    details.insert(
        "storePath".to_string(),
        Value::String(path.display().to_string()),
    );
    details.insert("kind".to_string(), Value::String(kind));
    Some(details)
}

/// The io failure that makes `path` unusable as a store file, if any.
///
/// Two probes, both synchronous and cheap. The first follows any symlink and refuses a name that
/// resolves to something that is not a file — a directory, a device — which SQLite would answer
/// with a generic "unable to open database file" naming neither the path nor the reason. The second opens the path for writing and closes it
/// again, which is exactly what building the pool does: a parent that refuses writes, or a
/// read-only file, is the caller's own path and is reported as such before any pool exists. The
/// empty file this probe creates is a database SQLite initializes normally.
///
/// Whatever survives both probes and still fails is not about the name: a database another process
/// holds locked, a migration that refuses the file, a full disk. That is the environment's.
fn store_path_fault(path: &Path) -> Option<std::io::Error> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Some(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                "a store path must be a file",
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Some(error),
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .err()
}

/// Open the one store file this client serves.
///
/// `journalDir`, when given, is created, resolved and its whole chain made durable here: that is
/// the host duty `journalDir` requires, because the publisher serves an existing root and never
/// creates it. The **resolved absolute** path is what the client retains, so every later
/// publication lands under the root the caller named at open even after the process changes its
/// working directory — a relative root means one directory then, and that directory afterwards.
///
/// The walk is synchronous and does real file I/O: it runs on the blocking pool rather than on the
/// async worker, so an `open` over a slow or network filesystem cannot stall every other call on
/// the addon's runtime.
///
/// Both caller-supplied paths are classified by [`path_fault`] and [`store_path_refusal`]: a name that
/// cannot be a directory or a file is `INVALID_INPUT`, an environment that refuses the work is
/// `INTERNAL_ERROR`. A walk that did not run at all (a join failure) is the addon's own
/// `INTERNAL_ERROR`.
#[napi]
pub async fn open_client(options: ClientOptions) -> ClientResult {
    let journal_root = match &options.journal_dir {
        Some(dir) => {
            let configured = PathBuf::from(dir);
            if configured.as_os_str().is_empty() {
                return ClientResult::refused(&SpokeReject {
                    code: SpokeRejectCode::InvalidInput,
                    message:
                        "journalDir is empty: name the directory `review` should publish under"
                            .to_string(),
                    details: path_details(
                        "journalDir",
                        &configured,
                        std::io::ErrorKind::InvalidInput,
                    ),
                });
            }
            let durable = configured.clone();
            let walked = napi::tokio::task::spawn_blocking(move || durable_root(&durable)).await;
            match walked {
                Ok(Ok(resolved)) => Some(resolved),
                Ok(Err(error)) => {
                    return ClientResult::refused(&path_fault("journalDir", &configured, &error))
                }
                Err(error) => {
                    return ClientResult::refused(&SpokeReject {
                        code: SpokeRejectCode::InternalError,
                        message: format!(
                            "the journal root {} could not be prepared: {error}",
                            configured.display()
                        ),
                        details: None,
                    })
                }
            }
        }
        None => None,
    };
    let configured = PathBuf::from(&options.store_path);
    let store_path = match absolute(&configured) {
        Ok(resolved) => resolved,
        Err(error) => return ClientResult::refused(&store_path_refusal(&configured, &error)),
    };
    if let Some(error) = store_path_fault(&store_path) {
        return ClientResult::refused(&store_path_refusal(&store_path, &error));
    }
    match MindStore::open(&store_path).await {
        Ok(store) => ClientResult::answered(MindcoreClient {
            store: Arc::new(store),
            caller: CallerId::new(options.caller_id),
            journal_root,
        }),
        Err(error) => ClientResult::refused(&SpokeReject {
            code: SpokeRejectCode::InternalError,
            message: format!(
                "storePath {} could not be opened: {error}",
                store_path.display()
            ),
            details: store_open_details(&store_path, &error),
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
    match client.opened_mind(&created.id).await {
        SpokeResult::Ok(view) => MindResult::answered(view),
        // see .agents/adr/implemented/architecture/2026-11-28-create-outcome-uncertain-after-commit.md
        SpokeResult::Reject(reject) => MindResult::refused(&SpokeReject {
            code: SpokeRejectCode::OutcomeUncertain,
            message: format!(
                "the Mind {} was created, but opening its handle failed: {}",
                created.id.as_str(),
                reject.message
            ),
            details: Some(Map::from_iter([
                (
                    "mindId".to_string(),
                    Value::String(created.id.as_str().to_string()),
                ),
                (
                    "code".to_string(),
                    Value::String(reject.code.as_str().to_string()),
                ),
            ])),
        }),
    }
}

/// Every Mind this client's caller holds a live grant in, in `id` order.
#[napi]
pub async fn client_list_minds(client: &MindcoreClient) -> Value {
    settle(match minds::list(&client.store, &client.caller).await {
        SpokeResult::Ok(rows) => {
            SpokeResult::Ok(rows.iter().map(MindInfo::from).collect::<Vec<_>>())
        }
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    })
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
    let mut tx = match client.store.pool().begin_with("BEGIN IMMEDIATE").await {
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

/// The derived judgment of one Mind as JavaScript sees it: the carrier revision the read served,
/// its current-view records and the `MindState` projection, camelCase. `Cognition` carries no
/// serde derive and its revision is a `u64`, so this is the projection the binding owns; the
/// number stays a JSON number, which is what `index.d.ts` declares.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CognitionInfo {
    carrier_revision: u64,
    records: Vec<Value>,
    mind_state: Value,
}

impl From<mindcore_spoke::Cognition> for CognitionInfo {
    fn from(cognition: mindcore_spoke::Cognition) -> Self {
        Self {
            carrier_revision: cognition.carrier_revision,
            records: cognition.records,
            mind_state: cognition.mind_state,
        }
    }
}

/// The addon's own refusal for an argument it cannot render into the facade's plain arguments.
fn invalid_input<T>(message: impl Into<String>) -> SpokeResult<T> {
    spoke_reject(SpokeRejectCode::InvalidInput, message, None)
}

/// A JavaScript `number` as the `u64` revision it names.
///
/// Only a finite, whole, non-negative value below `2^64` is one: `as u64` would truncate a
/// fraction, wrap a sign or saturate a magnitude, and a revision nobody could have stored is
/// the caller's mistake to hear about, not a revision to read.
fn whole_number(value: f64, member: &str) -> SpokeResult<u64> {
    const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
    if value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value < TWO_POW_64 {
        SpokeResult::Ok(value as u64)
    } else {
        invalid_input(format!(
            "{member} must be a whole non-negative number below 2^64, not {value}"
        ))
    }
}

/// An optional revision, converted by [`whole_number`]; a fractional, negative or infinite
/// member is refused before any facade call.
fn optional_revision(value: Option<f64>, member: &str) -> SpokeResult<Option<u64>> {
    match value {
        Some(value) => match whole_number(value, member) {
            SpokeResult::Ok(revision) => SpokeResult::Ok(Some(revision)),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        },
        None => SpokeResult::Ok(None),
    }
}

/// A JavaScript `number` as the `u32` recall budget it names.
///
/// [`whole_number`] first, so nothing wraps: `-1` must not become `4294967295` through JavaScript's
/// `uint32` conversion, and `2^32` must not become `0`. A value above `u32::MAX` is refused rather
/// than saturated, naming the member and the value.
fn whole_budget(value: f64, member: &str) -> SpokeResult<u32> {
    match whole_number(value, member) {
        SpokeResult::Ok(number) => match u32::try_from(number) {
            Ok(budget) => SpokeResult::Ok(budget),
            Err(_) => invalid_input(format!(
                "{member} must be a whole non-negative number at most {}, not {value}",
                u32::MAX
            )),
        },
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    }
}

/// An optional recall budget, converted by [`whole_budget`].
fn optional_budget(value: Option<f64>, member: &str) -> SpokeResult<Option<u32>> {
    match value {
        Some(value) => match whole_budget(value, member) {
            SpokeResult::Ok(budget) => SpokeResult::Ok(Some(budget)),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        },
        None => SpokeResult::Ok(None),
    }
}

/// An optional RFC 3339 `occurredAt` as the instant the capture carries.
fn occurred_at(value: Option<&str>) -> SpokeResult<Option<DateTime<Utc>>> {
    match value {
        None => SpokeResult::Ok(None),
        Some(text) => match DateTime::parse_from_rfc3339(text) {
            Ok(at) => SpokeResult::Ok(Some(at.with_timezone(&Utc))),
            Err(error) => invalid_input(format!(
                "occurredAt must be an RFC 3339 datetime, not {text}: {error}"
            )),
        },
    }
}

/// The opened Mind's registry row as it was at open, plus the carrier entry its judgment lives
/// in. A synchronous read: it calls nothing and holds no transaction.
#[napi]
pub fn mind_view_info(view: &MindView) -> MindInfo {
    MindInfo::from(view.mind.row())
}

/// Read one entry of this Mind's collection, at its current revision or at `revision`.
#[napi]
pub async fn mind_knowledge_get(view: &MindView, entry_id: String, revision: Option<f64>) -> Value {
    let revision = match optional_revision(revision, "revision") {
        SpokeResult::Ok(revision) => revision,
        SpokeResult::Reject(reject) => {
            return settle::<KnowledgeEntry>(SpokeResult::Reject(reject))
        }
    };
    settle(view.mind.knowledge_get(&entry_id, revision).await)
}

/// One knowledge entry as JavaScript supplied it, decoded into its published type. Anything the
/// wire type refuses to decode is the caller's `INVALID_INPUT`, naming the member.
fn knowledge_entry(value: Value) -> SpokeResult<KnowledgeEntry> {
    match serde_json::from_value(value) {
        Ok(entry) => SpokeResult::Ok(entry),
        Err(error) => invalid_input(format!("entry is not a knowledge entry: {error}")),
    }
}

/// One review decision as JavaScript supplied it: the wire type's `decision` member, tagged on
/// `action`.
fn review_decision(value: Value) -> SpokeResult<ReviewDecision> {
    match serde_json::from_value(value) {
        Ok(decision) => SpokeResult::Ok(decision),
        Err(error) => invalid_input(format!("decision is not a review decision: {error}")),
    }
}

/// Write one entry of this Mind's collection, against the revision the caller read it at.
#[napi]
pub async fn mind_knowledge_put(
    view: &MindView,
    entry: Value,
    expected_base_revision: Option<f64>,
) -> Value {
    let expected = match optional_revision(expected_base_revision, "expectedBaseRevision") {
        SpokeResult::Ok(expected) => expected,
        SpokeResult::Reject(reject) => {
            return settle::<KnowledgeEntry>(SpokeResult::Reject(reject))
        }
    };
    let entry = match knowledge_entry(entry) {
        SpokeResult::Ok(entry) => entry,
        SpokeResult::Reject(reject) => {
            return settle::<KnowledgeEntry>(SpokeResult::Reject(reject))
        }
    };
    settle(view.mind.knowledge_put(entry, expected).await)
}

/// Every entry of this Mind's collection, bounded by the store's own 512-entry cap.
#[napi]
pub async fn mind_knowledge_list(view: &MindView) -> Value {
    settle(view.mind.knowledge_list().await)
}

/// Capture one memory into this Mind.
#[napi]
pub async fn mind_capture(view: &MindView, input: CaptureInput) -> Value {
    let occurred_at = match occurred_at(input.occurred_at.as_deref()) {
        SpokeResult::Ok(occurred_at) => occurred_at,
        SpokeResult::Reject(reject) => {
            return settle::<MemoryCaptureResponse>(SpokeResult::Reject(reject))
        }
    };
    let capture = Capture {
        subject: &input.subject,
        context: input.context.as_deref(),
        content: &input.content,
        occurred_at,
        kind_hint: input.kind_hint.as_deref(),
        idempotency_key: &input.idempotency_key,
    };
    settle(view.mind.capture(&capture).await)
}

/// Review one pending candidate and answer the settled receipt.
#[napi]
pub async fn mind_review(
    view: &MindView,
    candidate_id: String,
    decision: Value,
    idempotency_key: String,
) -> Value {
    let decision = match review_decision(decision) {
        SpokeResult::Ok(decision) => decision,
        SpokeResult::Reject(reject) => {
            return settle::<MemoryCandidatesReviewResponse>(SpokeResult::Reject(reject))
        }
    };
    settle(
        view.mind
            .review(&candidate_id, &decision, &idempotency_key)
            .await,
    )
}

/// Recall this Mind's live records.
#[napi]
pub async fn mind_recall(view: &MindView, input: RecallInput) -> Value {
    let max_items = match whole_budget(input.max_items, "maxItems") {
        SpokeResult::Ok(max_items) => max_items,
        SpokeResult::Reject(reject) => {
            return settle::<MemoryRecallResponse>(SpokeResult::Reject(reject))
        }
    };
    let max_bytes = match optional_budget(input.max_bytes, "maxBytes") {
        SpokeResult::Ok(max_bytes) => max_bytes,
        SpokeResult::Reject(reject) => {
            return settle::<MemoryRecallResponse>(SpokeResult::Reject(reject))
        }
    };
    let recall = Recall {
        subject: &input.subject,
        selection: &input.selection,
        max_items: u64::from(max_items),
        max_bytes: max_bytes.map(u64::from),
        query: input.query.as_deref(),
        cursor: input.cursor.as_deref(),
    };
    settle(view.mind.recall(&recall).await)
}

/// Read this Mind's judgment, or `null` for a Mind that has judged nothing.
#[napi]
pub async fn mind_cognition_read(view: &MindView) -> Value {
    let settled = match view.mind.cognition_read().await {
        SpokeResult::Ok(cognition) => SpokeResult::Ok(cognition.map(CognitionInfo::from)),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    };
    settle(settled)
}

/// The host duty a journal root requires: the root exists, and every entry on the way to it is
/// durable before the first publication writes into it. Answers the absolute path it resolved to,
/// which is the root the caller must retain.
///
/// Two walks, both idempotent — not first-creation-only, because nothing records which entry a
/// failed start left unproven, so a restart redoes both:
///
/// 1. The configured spelling, from the filesystem root down to `root`: each level is created when
///    missing, refused under its own name when it is not a directory, and the entry naming it in
///    the directory that holds it is flushed.
/// 2. The resolved chain, from `root`'s canonical form up to the filesystem root: the entry naming
///    each level in its own holding directory is flushed as well.
///
/// The second walk is what makes a symlink safe at any depth. `/existing/link` resolving to
/// `/other/new/real` puts `new` — the directory holding `real`'s entry — outside every name the
/// configured walk visited, so walk 1 alone can report success while a crash loses the entry a
/// successful publication depended on. A path with no symlink resolves to itself, and the second
/// walk then repeats only entries the first already flushed.
///
/// An entry is flushed at most once per call: the two walks overlap on every ancestor of the
/// configured root, and a directory `fsync` is idempotent anyway. A level another process created
/// between the look and the `create_dir` is not a failure — `AlreadyExists` is the same outcome,
/// and the entry still gets its sync. A resolution failure after creation is a journal fault
/// naming the path: nothing can be published under a root whose real name is unknown.
fn durable_root(root: &Path) -> std::io::Result<PathBuf> {
    let mut synced: HashSet<PathBuf> = HashSet::new();
    let mut chain: Vec<&Path> = Vec::new();
    let mut cursor = Some(root);
    while let Some(level) = cursor {
        chain.push(level);
        cursor = level
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
    }
    for level in chain.iter().rev() {
        match std::fs::metadata(level) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(not_a_directory(level)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                #[cfg(test)]
                create_concurrently(level);
                match std::fs::create_dir(level) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !std::fs::metadata(level).is_ok_and(|m| m.is_dir()) {
                            return Err(not_a_directory(level));
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
        if let Some(parent) = holding_directory(level) {
            sync_once(parent, &mut synced)?;
        }
    }
    let resolved = std::fs::canonicalize(root).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!(
                "the journal root {} cannot be resolved after it was created: {error}",
                root.display()
            ),
        )
    })?;
    let mut resolved_chain: Vec<&Path> = Vec::new();
    let mut cursor = Some(resolved.as_path());
    while let Some(level) = cursor {
        resolved_chain.push(level);
        cursor = level.parent();
    }
    for level in resolved_chain {
        if let Some(parent) = holding_directory(level) {
            sync_once(parent, &mut synced)?;
        }
    }
    Ok(resolved)
}

/// [`sync_parent`], skipped for a directory this call has already flushed.
///
/// The key is the directory's **resolved** path, not the spelling it arrived as: a relative root's
/// configured walk names the current directory `.` while the resolved walk names the same
/// directory absolutely, and both must count as one flush. A directory this call cannot resolve is
/// keyed by its spelling — it still gets exactly one flush.
fn sync_once(parent: &Path, synced: &mut HashSet<PathBuf>) -> std::io::Result<()> {
    let key = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    if synced.insert(key) {
        sync_parent(parent)?;
    }
    Ok(())
}

/// `path` as an absolute path, resolved once against the directory that is current now.
///
/// The deepest existing ancestor is canonicalized and the components below it are appended, so a
/// relative `storePath` names one file for the life of the process: sqlx resolves a path when it
/// builds a connection, so a later working-directory change would otherwise reconnect to a second
/// database. A path whose every ancestor exists resolves exactly; a path none of whose ancestors
/// exist cannot happen here, because the filesystem root always exists.
fn absolute(path: &Path) -> std::io::Result<PathBuf> {
    let mut missing: Vec<&std::ffi::OsStr> = Vec::new();
    let mut cursor = path;
    loop {
        match std::fs::canonicalize(cursor) {
            Ok(mut resolved) => {
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match cursor.file_name() {
                    Some(name) => missing.push(name),
                    None => return Err(error),
                }
                match cursor.parent() {
                    Some(parent) if parent.as_os_str().is_empty() => cursor = Path::new("."),
                    Some(parent) => cursor = parent,
                    None => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
    }
}

/// The directory holding `level`'s entry.
///
/// `Path::new("journal").parent()` is the empty path, which names the current directory; only
/// `None` — the filesystem root — has no directory holding its entry.
fn holding_directory(level: &Path) -> Option<&Path> {
    match level.parent() {
        None => None,
        Some(parent) if parent.as_os_str().is_empty() => Some(Path::new(".")),
        Some(parent) => Some(parent),
    }
}

/// The refusal of a path component that is not a directory, naming it: accepting it would publish
/// into whatever the path really names.
fn not_a_directory(level: &Path) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{} exists and is not a directory", level.display()),
    )
}

fn sync_parent(parent: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    record_sync(parent);
    #[cfg(test)]
    if take_injected_sync_failure(parent) {
        return Err(std::io::Error::other(format!(
            "{}: injected sync fault",
            parent.display()
        )));
    }
    std::fs::File::open(parent)?.sync_all()
}

/// A test seam: the next sync of this exact directory fails, and every attempt is counted. An
/// `fsync` fault cannot be built from a test's side, so this is the only way to pin that a failed
/// sync is retried by the next start rather than skipped because the level now exists.
#[cfg(test)]
static INJECT_SYNC_FAILURE: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
static SYNC_ATTEMPTS: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn record_sync(parent: &Path) {
    SYNC_ATTEMPTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(parent.to_path_buf());
}

#[cfg(test)]
fn take_injected_sync_failure(parent: &Path) -> bool {
    let mut pending = INJECT_SYNC_FAILURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match pending.iter().position(|path| path == parent) {
        Some(index) => {
            pending.remove(index);
            true
        }
        None => false,
    }
}

/// A test seam: the next `Mind::open` fails, and every attempt is counted. A post-commit open
/// failure is the one window no facade path can enter from a test — the registry row is already
/// durable by the time the handle is read back.
#[cfg(test)]
static OPEN_FAILURE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
fn take_injected_open_failure(_mind_id: &MindId) -> bool {
    OPEN_FAILURE.swap(false, std::sync::atomic::Ordering::SeqCst)
}

/// A test seam: the next create of this exact level lands as another process's. `true` plants a
/// directory (the benign race), `false` a regular file (the refusal). Without it the window
/// between this walk's check and its `create_dir` cannot be entered from a test.
#[cfg(test)]
static CREATE_CONCURRENTLY: std::sync::Mutex<Vec<(PathBuf, bool)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn create_concurrently(level: &Path) {
    let planted = {
        let mut pending = CREATE_CONCURRENTLY
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending
            .iter()
            .position(|(path, _)| path == level)
            .map(|index| pending.remove(index).1)
    };
    match planted {
        Some(true) => {
            let _ = std::fs::create_dir(level);
        }
        Some(false) => {
            let _ = std::fs::File::create(level);
        }
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{
        absolute, client_create_mind, durable_root, knowledge_entry, occurred_at, optional_budget,
        optional_revision, path_fault, review_decision, settle, store_open_details,
        store_path_fault, store_path_refusal, whole_budget, whole_number, MindcoreClient,
        BINDING_DEFECT, CREATE_CONCURRENTLY, INJECT_SYNC_FAILURE, OPEN_FAILURE, SYNC_ATTEMPTS,
    };
    use std::path::{Path, PathBuf};
    use std::sync::Arc;


    /// The working directory is process-wide, so the tests that set it hold this and no other test
    /// touches it.
    static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The scratch directory as the walk sees it: the tests live under `/var/…` on macOS, which is
    /// a symlink to `/private/var`, so a raw temporary path is not a path any parent is keyed by.
    /// A level the walk will create does not resolve yet, so it is the canonical parent plus its
    /// own name.
    fn canonical(path: &Path) -> PathBuf {
        path.canonicalize()
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
    }

    fn syncs_of(path: &Path) -> usize {
        SYNC_ATTEMPTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|attempt| attempt.as_path() == path)
            .count()
    }

    /// An answer the addon itself cannot serialize is its own defect, never a facade refusal: the
    /// code names it, and no facade path can produce that code. `serde_json::Value` cannot hold a
    /// failing `Serialize`, so the failing value comes from a test type whose impl errors.
    #[test]
    fn an_answer_that_does_not_serialize_is_a_binding_defect() {
        use serde::ser::Error as _;
        use serde::Serialize;
        use spoke_operations::SpokeResult;

        struct Unserializable;

        impl Serialize for Unserializable {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(S::Error::custom("this answer cannot be serialized"))
            }
        }

        let answer = settle(SpokeResult::Ok(Unserializable));

        assert_eq!(answer["ok"], serde_json::json!(false), "{answer}");
        assert_eq!(
            answer["code"],
            serde_json::json!(BINDING_DEFECT),
            "{answer}"
        );
        assert_ne!(
            answer["code"],
            serde_json::json!("INTERNAL_ERROR"),
            "a binding defect is not a facade refusal: {answer}"
        );
        assert!(
            answer["message"]
                .as_str()
                .is_some_and(|message| message.contains("Unserializable")),
            "the message names what could not be serialized: {answer}"
        );
    }

    /// One conversion's refusal as its code and message; the value type differs per conversion.
    fn refusal_of<T>(
        convert: impl Fn() -> spoke_operations::SpokeResult<T>,
    ) -> (spoke_operations::SpokeRejectCode, String) {
        match convert() {
            spoke_operations::SpokeResult::Reject(reject) => (reject.code, reject.message),
            spoke_operations::SpokeResult::Ok(_) => panic!("the conversion accepted a bad member"),
        }
    }

    /// Every argument that is not of the type its export declares is the caller's own
    /// `INVALID_INPUT`, refused before any facade call, with the member named. A revision is
    /// covered below; these are the other three conversions the exports perform.
    #[test]
    fn a_member_of_the_wrong_shape_is_invalid_input_naming_the_member() {
        use serde_json::json;
        use spoke_operations::{SpokeRejectCode, SpokeResult};

        let refusals = [
            (
                "occurredAt",
                refusal_of(|| occurred_at(Some("not-a-timestamp"))),
            ),
            (
                "entry",
                refusal_of(|| knowledge_entry(json!({ "no_entry_id": true }))),
            ),
            (
                "decision",
                refusal_of(|| review_decision(json!({ "action": "maybe" }))),
            ),
        ];
        for (member, (code, message)) in refusals {
            assert_eq!(code, SpokeRejectCode::InvalidInput, "{member}: {message}");
            assert!(
                message.contains(member),
                "the refusal names {member}: {message}"
            );
        }

        assert!(matches!(
            occurred_at(Some("2026-01-01T00:30:00Z")),
            SpokeResult::Ok(Some(_))
        ));
        assert!(matches!(occurred_at(None), SpokeResult::Ok(None)));
        assert!(matches!(
            knowledge_entry(json!({
                "body": {}, "canonical_name": "c", "entry_id": "e", "entry_type": "t",
                "extensions": {}, "schema_version": 1, "status": "confirmed",
            })),
            SpokeResult::Ok(_)
        ));
        assert!(matches!(
            review_decision(json!({ "action": "promote" })),
            SpokeResult::Ok(_)
        ));
    }

    /// A recall budget arrives as a JavaScript `number` and is read through no `uint32` conversion:
    /// `-1` must not become `4294967295` and `2^32` must not become `0`. Only a whole, non-negative
    /// value at most `u32::MAX` is a budget; the rest name the member and the value.
    #[test]
    fn recall_budgets_are_whole_and_fit_in_u32() {
        use spoke_operations::{SpokeRejectCode, SpokeResult};

        for (value, expected) in [(0.0, 0u32), (1.0, 1), ((u32::MAX) as f64, u32::MAX)] {
            match whole_budget(value, "maxItems") {
                SpokeResult::Ok(budget) => assert_eq!(budget, expected, "{value}"),
                SpokeResult::Reject(reject) => panic!("{value} refused: {}", reject.message),
            }
        }
        for value in [
            -1.0,
            0.5,
            f64::NAN,
            f64::INFINITY,
            4_294_967_296.0,
            f64::MAX,
        ] {
            match whole_budget(value, "maxItems") {
                SpokeResult::Reject(reject) => {
                    assert_eq!(reject.code, SpokeRejectCode::InvalidInput, "{value}");
                    assert!(
                        reject.message.contains("maxItems"),
                        "the refusal names the member: {value}, {}",
                        reject.message
                    );
                    assert!(
                        reject.message.contains(&value.to_string()),
                        "the refusal names the value: {value}, {}",
                        reject.message
                    );
                }
                SpokeResult::Ok(budget) => panic!("{value} became {budget}"),
            }
        }
        assert!(matches!(
            optional_budget(None, "maxBytes"),
            SpokeResult::Ok(None)
        ));
        assert!(matches!(
            optional_budget(Some(1.0), "maxBytes"),
            SpokeResult::Ok(Some(1))
        ));
        assert!(matches!(
            optional_budget(Some(-1.0), "maxBytes"),
            SpokeResult::Reject(_)
        ));
    }

    /// Two failures read very differently to a caller: a name that cannot be what it must be is
    /// their spelling, and they can fix it; a filesystem that refuses the work is the environment,
    /// and retrying the same spelling changes nothing. Everything names the path and the kind.
    #[cfg(unix)]
    #[test]
    fn a_path_fault_is_the_callers_or_the_environments_by_kind() {
        use spoke_operations::SpokeRejectCode;

        let file = path_fault(
            "journalDir",
            Path::new("/tmp/journal"),
            &std::io::Error::from(std::io::ErrorKind::NotADirectory),
        );
        assert_eq!(file.code, SpokeRejectCode::InvalidInput);
        assert!(
            file.message.contains("/tmp/journal"),
            "the refusal names the path: {}",
            file.message
        );
        let details = file.details.expect("a path refusal carries its members");
        assert_eq!(details["journalDir"], serde_json::json!("/tmp/journal"));
        assert!(details.contains_key("kind"));

        let denied = path_fault(
            "journalDir",
            Path::new("/tmp/journal"),
            &std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );
        assert_eq!(
            denied.code,
            SpokeRejectCode::InternalError,
            "a permission fault is not the caller's spelling"
        );
        assert!(denied.message.contains("/tmp/journal"));

        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let sealed = scratch_path.join("sealed");
        std::fs::create_dir(&sealed).expect("the sealed directory");
        let root = sealed.join("journal");
        let mut permissions = std::fs::metadata(&sealed).expect("metadata").permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o555);
        std::fs::set_permissions(&sealed, permissions).expect("seal the directory");
        let walked = durable_root(&root);
        let mut permissions = std::fs::metadata(&sealed).expect("metadata").permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
        std::fs::set_permissions(&sealed, permissions).expect("unseal the directory");

        match walked {
            Err(error) => assert_eq!(
                path_fault("journalDir", &root, &error).code,
                SpokeRejectCode::InternalError,
                "an unwritable ancestor is an environment fault: {error}"
            ),
            Ok(_) => eprintln!("skipping the walk half: this process may write into a sealed dir"),
        }
    }

    /// A store name that is a directory is refused before SQLite sees it; a name that cannot be
    /// written is refused by the write probe. Both name `storePath`.
    #[cfg(unix)]
    #[test]
    fn a_store_path_that_is_not_a_file_is_refused_before_the_pool() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());

        let directory = scratch_path.join("a-directory");
        std::fs::create_dir(&directory).expect("the directory");
        let fault = store_path_fault(&directory).expect("a directory is not a store file");
        let refusal = store_path_refusal(&directory, &fault);
        assert_eq!(
            refusal.code,
            spoke_operations::SpokeRejectCode::InvalidInput
        );

        let sealed = scratch_path.join("sealed");
        std::fs::create_dir(&sealed).expect("the sealed directory");
        let store = sealed.join("mind.db");
        let mut permissions = std::fs::metadata(&sealed).expect("metadata").permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o555);
        std::fs::set_permissions(&sealed, permissions).expect("seal the directory");
        let fault = store_path_fault(&store);
        let mut permissions = std::fs::metadata(&sealed).expect("metadata").permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
        std::fs::set_permissions(&sealed, permissions).expect("unseal the directory");
        match fault {
            Some(error) => {
                assert_eq!(
                    store_path_refusal(&store, &error).code,
                    spoke_operations::SpokeRejectCode::InvalidInput,
                    "an unwritable parent is the caller's path: {error}"
                );
            }
            None => eprintln!("skipping the probe half: this process may write into a sealed dir"),
        }

        let fresh = scratch_path.join("fresh.db");
        assert!(store_path_fault(&fresh).is_none());
        assert!(fresh.is_file(), "the probe creates the file it will use");
    }

    /// A symlinked store path is the host's own arrangement: the probe follows the link and judges
    /// what it names, so a link to a file is a file and a link to a directory is refused with the
    /// kind its target has. Reading the link's own type instead would refuse every symlinked store.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_store_path_is_judged_by_what_it_names() {
        use std::os::unix::fs::symlink;

        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());

        let real = scratch_path.join("real.db");
        std::fs::File::create(&real).expect("the store file");
        let link = scratch_path.join("linked.db");
        symlink(&real, &link).expect("the store symlink");
        assert!(
            store_path_fault(&link).is_none(),
            "a link to a regular file is a usable store path"
        );

        let directory = scratch_path.join("a-directory");
        std::fs::create_dir(&directory).expect("the directory");
        let linked_directory = scratch_path.join("linked-directory");
        symlink(&directory, &linked_directory).expect("the directory symlink");
        let fault =
            store_path_fault(&linked_directory).expect("a link to a directory is not a store file");
        let refusal = store_path_refusal(&linked_directory, &fault);
        assert_eq!(
            refusal.code,
            spoke_operations::SpokeRejectCode::InvalidInput
        );
        assert!(
            refusal
                .details
                .as_ref()
                .is_some_and(|details| details["kind"] == serde_json::json!("is a directory")),
            "the refusal carries the kind the target has: {:?}",
            refusal.details
        );
        assert_eq!(
            refusal.details.expect("details")["storePath"],
            serde_json::json!(linked_directory.display().to_string())
        );
    }

    /// A relative `storePath` is resolved once, against the directory that is current then.
    /// Resolving it per connection — which is what sqlx does with the string it was handed — would
    /// make a later working-directory change reconnect to a second database.
    #[test]
    fn a_relative_store_path_resolves_once_against_its_open_directory() {
        let _guard = CWD.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let first = tempfile::TempDir::new().expect("a scratch directory");
        let second = tempfile::TempDir::new().expect("another scratch directory");
        let first_path = canonical(first.path());
        let second_path = canonical(second.path());
        let previous = std::env::current_dir().expect("a current directory");

        std::env::set_current_dir(&first_path).expect("enter the first directory");
        let resolved = absolute(Path::new("mind.db"));
        std::env::set_current_dir(&second_path).expect("enter the second directory");
        let also_resolved = absolute(Path::new("mind.db"));
        std::env::set_current_dir(&previous).expect("leave the scratch directories");

        assert_eq!(
            resolved.expect("a relative store path resolves"),
            first_path.join("mind.db"),
            "the name is the file in the directory that was current at open"
        );
        assert_eq!(
            also_resolved.expect("a relative store path resolves"),
            second_path.join("mind.db"),
            "the same spelling names a different file from a different directory — which is exactly
             why the resolution happens once, at open"
        );

        let nested = absolute(&first_path.join("not-yet").join("mind.db")).expect("resolves");
        assert_eq!(nested, first_path.join("not-yet").join("mind.db"));
        assert!(absolute(Path::new("/"))
            .expect("the root resolves")
            .is_absolute());
    }

    /// The create path commits its registry row before it opens the handle, so a failure in that
    /// window must not be reported as a refusal: a host that retried would create a second Mind.
    /// The answer is `OUTCOME_UNCERTAIN` carrying the id that does exist.
    #[test]
    fn a_create_whose_open_fails_after_the_commit_is_outcome_uncertain() {
        use mindcore_base::boundary::CallerId;
        use spoke_operations::SpokeResult;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let store_path = scratch.path().join("mind.db");

        let (answered, listed) = runtime.block_on(async {
            let store = match mindcore_sqlite::MindStore::open(&store_path).await {
                Ok(store) => store,
                Err(error) => panic!("the store opens: {error}"),
            };
            let client = MindcoreClient {
                store: Arc::new(store),
                caller: CallerId::new("caller_wren"),
                journal_root: None,
            };
            OPEN_FAILURE.store(true, std::sync::atomic::Ordering::SeqCst);
            let answered = client_create_mind(&client, None).await;
            assert!(
                !OPEN_FAILURE.load(std::sync::atomic::Ordering::SeqCst),
                "the seam fired: the failure is the post-commit read-back"
            );
            let listed = match mindcore_runtime::minds::list(&client.store, &client.caller).await {
                SpokeResult::Ok(rows) => rows
                    .iter()
                    .map(|row| row.id.as_str().to_string())
                    .collect::<Vec<_>>(),
                SpokeResult::Reject(reject) => panic!("list refused: {}", reject.message),
            };
            (answered, listed)
        });

        assert_eq!(
            listed.len(),
            1,
            "the create did commit, which is why the answer is uncertain rather than a refusal"
        );
        assert!(!answered.ok);
        assert_eq!(answered.code.as_deref(), Some("OUTCOME_UNCERTAIN"));
        let message = answered.message.expect("a message");
        assert!(
            message.contains("was created, but opening its handle failed"),
            "{message}"
        );
        let details = answered.details.expect("details carry the id that exists");
        assert_eq!(
            details["mindId"],
            serde_json::json!(listed[0]),
            "the refusal names the Mind the host must not create again: {details}"
        );
        assert_eq!(details["code"], serde_json::json!("INTERNAL_ERROR"));
    }

    /// A store that fails after both probes report what actually failed, never a stand-in. A
    /// database another connection holds in exclusive locking mode is the cheapest real failure to
    /// produce here.
    #[test]
    fn a_store_that_fails_to_open_reports_its_own_kind() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let store_path = scratch.path().join("mind.db");

        std::fs::write(&store_path, b"not a database at all, not even close").expect("garbage");
        assert!(
            store_path_fault(&store_path).is_none(),
            "the probes pass: the name is a writable regular file"
        );
        let error = runtime.block_on(async {
            match mindcore_sqlite::MindStore::open(&store_path).await {
                Ok(_) => panic!("garbage is not a database"),
                Err(error) => error,
            }
        });
        let details = store_open_details(&store_path, &error).expect("details");
        let kind = details["kind"]
            .as_str()
            .expect("the kind is a string")
            .to_string();
        assert!(
            !kind.is_empty() && kind != "other",
            "the refusal carries what failed, not a stand-in: {kind}"
        );
        assert_eq!(
            details["storePath"],
            serde_json::json!(store_path.display().to_string())
        );
    }

    /// A revision arrives as a JavaScript `number`: only a finite, whole, non-negative one below
    /// `2^64` names a revision, and every other value is the caller's own `INVALID_INPUT`.
    #[test]
    fn revisions_are_whole_non_negative_numbers() {
        use spoke_operations::{SpokeRejectCode, SpokeResult};

        for (value, expected) in [(0.0, 0u64), (1.0, 1), (2.0_f64.powi(53), 1 << 53)] {
            match whole_number(value, "revision") {
                SpokeResult::Ok(revision) => assert_eq!(revision, expected, "{value}"),
                SpokeResult::Reject(reject) => panic!("{value} refused: {}", reject.message),
            }
        }
        for value in [
            -1.0,
            0.5,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            2.0_f64.powi(64),
        ] {
            match whole_number(value, "revision") {
                SpokeResult::Reject(reject) => {
                    assert_eq!(reject.code, SpokeRejectCode::InvalidInput, "{value}")
                }
                SpokeResult::Ok(revision) => panic!("{value} became {revision}"),
            }
        }
        assert!(matches!(
            optional_revision(None, "revision"),
            SpokeResult::Ok(None)
        ));
    }

    /// The whole point of the idempotent walk: a level an earlier start created but never proved
    /// durable is synced again by the next start, not skipped because it now exists.
    #[test]
    fn a_failed_sync_is_retried_for_the_level_it_created() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let root = scratch_path.join("a").join("journal");
        let level_parent: PathBuf = scratch_path.join("a");
        INJECT_SYNC_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(level_parent.clone());

        assert!(
            durable_root(&root).is_err(),
            "the injected sync fails the start"
        );
        assert!(
            root.is_dir(),
            "the level was created before the sync failed"
        );
        let after_first = syncs_of(&level_parent);
        assert_eq!(after_first, 1, "the failed attempt is counted");

        durable_root(&root).expect("the retry succeeds");
        assert_eq!(
            syncs_of(&level_parent),
            after_first + 1,
            "the retry syncs the level it now finds already there"
        );
    }

    /// Point 2 of the re-review: with every level already on disk, a start still syncs each
    /// level's parent, leaf first, up to the filesystem root. Nothing records which entry an
    /// earlier start left unproven, so a restart redoes the chain rather than trusting existence.
    #[test]
    fn an_already_existing_chain_is_synced_in_full() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let upper = scratch_path.join("upper");
        let lower = upper.join("lower");
        std::fs::create_dir(&upper).expect("the upper level");
        std::fs::create_dir(&lower).expect("the lower level");
        let before_upper = syncs_of(&scratch_path);
        let before_lower = syncs_of(&upper);

        durable_root(&lower).expect("an existing root starts");

        assert_eq!(
            syncs_of(&upper),
            before_lower + 1,
            "the lower level's entry is synced in the upper directory"
        );
        assert_eq!(
            syncs_of(&scratch_path),
            before_upper + 1,
            "the upper level's entry is synced in the scratch directory"
        );
    }

    /// A root reached through a symlink needs both chains durable: the target's, which the
    /// publisher writes beneath, and the configured one, whose entry naming the link would
    /// otherwise be the one a crash loses. The link and its target sit under different parents, so
    /// the two halves of the walk are told apart by which parent got the `fsync`.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_root_syncs_the_links_parent_and_the_targets_parent() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let links = scratch_path.join("links");
        let targets = scratch_path.join("targets");
        std::fs::create_dir(&links).expect("the link's directory");
        std::fs::create_dir(&targets).expect("the target's directory");
        let target = targets.join("t");
        std::fs::create_dir(&target).expect("the target");
        let link = links.join("root");
        std::os::unix::fs::symlink(&target, &link).expect("the symlink");
        let links_before = syncs_of(&links);
        let targets_before = syncs_of(&targets);

        durable_root(&link.join("journal")).expect("a root reached through the link starts");

        assert!(target.join("journal").is_dir(), "the target holds the root");
        assert!(
            syncs_of(&targets) > targets_before,
            "the canonical chain syncs the target's parent"
        );
        assert!(
            syncs_of(&links) > links_before,
            "the configured chain syncs the directory holding the link"
        );
    }

    /// A symlink anywhere on the path, not only at the leaf: `a/link/inner` where `link` points into
    /// `elsewhere/real`. Every configured level keeps its own entry flushed, and the level that is a
    /// symlink also flushes the directory it resolves to — the parent the publisher's canonicalized
    /// root lives under. A directory is one inode under two names, so its entry is flushed once:
    /// the count is asserted across both spellings, never per spelling.
    #[cfg(unix)]
    #[test]
    fn a_symlink_below_the_root_syncs_both_its_parent_and_the_resolved_parent() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let a = scratch_path.join("a");
        let elsewhere = scratch_path.join("elsewhere");
        std::fs::create_dir(&a).expect("the configured chain's directory");
        std::fs::create_dir(&elsewhere).expect("the resolved chain's directory");
        let real = elsewhere.join("real");
        std::fs::create_dir(&real).expect("the link target");
        let link = a.join("link");
        std::os::unix::fs::symlink(&real, &link).expect("the intermediate symlink");
        let a_before = syncs_of(&a);
        let elsewhere_before = syncs_of(&elsewhere);
        let real_before = syncs_of(&real) + syncs_of(&link);

        durable_root(&link.join("inner")).expect("a root below an intermediate symlink starts");

        assert!(
            real.join("inner").is_dir(),
            "the resolved path holds the root"
        );
        assert!(
            syncs_of(&a) > a_before,
            "the directory holding the configured symlink is synced"
        );
        assert!(
            syncs_of(&elsewhere) > elsewhere_before,
            "the directory holding the resolved path is synced"
        );
        assert_eq!(
            syncs_of(&real) + syncs_of(&link),
            real_before + 1,
            "the leaf's parent is one directory under two names: flushed once, through either"
        );
    }

    /// A level another process created between the check and `create_dir` is the same outcome, not
    /// a failure, and its entry still gets synced.
    #[test]
    fn a_level_created_concurrently_is_not_a_failure() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let root = scratch_path.join("a").join("journal");
        let raced: PathBuf = scratch_path.join("a");
        CREATE_CONCURRENTLY
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((raced.clone(), true));

        durable_root(&root).expect("the race is not a failure");
        assert!(root.is_dir());
        assert!(
            syncs_of(&raced) >= 1,
            "the raced level's entry is still synced"
        );
    }

    /// An ancestor that is a regular file is refused under its own name, not under the level
    /// beneath it that cannot be reached through it.
    #[test]
    fn a_file_ancestor_is_refused_by_its_own_name() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let blocker = scratch.path().join("blocker");
        std::fs::File::create(&blocker).expect("a regular file");
        let root = blocker.join("sub").join("journal");

        let error = durable_root(&root).expect_err("a file cannot hold a journal root");
        let message = error.to_string();
        assert!(
            message.contains(&blocker.display().to_string()),
            "the refusal names the blocking component: {message}"
        );
        assert!(
            !message.contains("sub/journal"),
            "the unreachable level is not the one named: {message}"
        );
    }

    /// The post-`create_dir` check is what refuses a component that is not a directory; without it
    /// the walk would accept the path and publish into whatever it names.
    #[test]
    fn a_component_that_is_not_a_directory_is_refused_by_name() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let root = scratch_path.join("a").join("journal");
        let raced: PathBuf = scratch_path.join("a");
        CREATE_CONCURRENTLY
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((raced.clone(), false));

        let error = durable_root(&root).expect_err("a file cannot be a journal root level");
        assert!(
            error.to_string().contains(&raced.display().to_string()),
            "the refusal names the component: {error}"
        );
    }

    /// `Path::new("journal").parent()` is the empty path, which names the current directory: a
    /// single-segment relative root is created there, its entry synced, and answered as the
    /// absolute path it named — which is what the client retains, so a later working-directory
    /// change cannot move a publication.
    ///
    /// The working directory is process-wide, so the tests that set it hold `CWD` and no other
    /// test touches it.
    #[test]
    fn a_relative_root_resolves_to_the_directory_it_was_named_in() {
        let _guard = CWD.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let first = tempfile::TempDir::new().expect("a scratch directory");
        let second = tempfile::TempDir::new().expect("another scratch directory");
        let first_path = canonical(first.path());
        let second_path = canonical(second.path());
        let previous = std::env::current_dir().expect("a current directory");

        std::env::set_current_dir(&first_path).expect("enter the first directory");
        let resolved = durable_root(Path::new("journal"));
        std::env::set_current_dir(&second_path).expect("enter the second directory");
        let created_here = Path::new("journal").is_dir();
        std::env::set_current_dir(&previous).expect("leave the scratch directories");

        assert_eq!(
            resolved.expect("a relative root starts"),
            first_path.join("journal"),
            "a relative root is answered as the absolute path it named"
        );
        assert!(
            first_path.join("journal").is_dir(),
            "the relative root is created in the directory that was current"
        );
        assert!(
            !created_here,
            "the directory entered afterwards gained nothing: the root was resolved once"
        );
    }

    /// The resolved chain, not only the configured one: `/existing/link` resolving to
    /// `/other/new/real` puts `new` — the directory holding `real`'s entry — outside every name the
    /// configured path visits. The configured walk cannot flush it, so only the second walk can.
    #[cfg(unix)]
    #[test]
    fn a_symlink_into_another_directory_syncs_the_targets_whole_chain() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let configured = scratch_path.join("configured");
        let other = scratch_path.join("other");
        let link_target = other.join("new").join("real");
        std::fs::create_dir(&configured).expect("the configured chain's directory");
        std::fs::create_dir_all(other.join("new")).expect("the target's parent");
        std::fs::create_dir(&link_target).expect("the link target");
        let link = configured.join("link");
        std::os::unix::fs::symlink(&link_target, &link).expect("the symlink");
        let holding_new = other.join("new");
        let holding_other = other.clone();
        let new_before = syncs_of(&holding_new);
        let other_before = syncs_of(&holding_other);

        let resolved = durable_root(&link.join("journal")).expect("the root through the link");

        assert_eq!(resolved, link_target.join("journal"));
        assert!(
            syncs_of(&holding_new) > new_before,
            "the directory holding the resolved level is synced, though no configured name visits it"
        );
        assert!(
            syncs_of(&holding_other) > other_before,
            "every ancestor of the resolved root is synced, not only its deepest level"
        );
    }

    /// One call flushes an entry once. The two walks overlap on every ancestor of the configured
    /// root; the count pins that the overlap is not re-synced.
    #[test]
    fn an_entry_shared_by_both_walks_is_flushed_once() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let scratch_path = canonical(scratch.path());
        let root = scratch_path.join("a").join("journal");
        std::fs::create_dir(scratch_path.join("a")).expect("the level");
        let before = syncs_of(&scratch_path);

        durable_root(&root).expect("a root with no symlink starts");

        assert_eq!(
            syncs_of(&scratch_path),
            before + 1,
            "the shared ancestor is flushed once, not once per walk"
        );
    }
}
