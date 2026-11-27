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

/// The host duty a journal root requires: the root exists, and every entry on the way to it is
/// durable before the first publication writes into it.
///
/// Two chains are made durable, in this order:
///
/// 1. The *canonical* one. `journal_dir` canonicalizes the root it serves, so a root reached
///    through a symlink publishes beneath the link's target; the entries beneath the target are
///    the target's and are created here if they are missing.
/// 2. The *configured* one. The publisher is handed the path as configured, so the entry naming a
///    symlink — and every directory on the way to it — must be durable too; a crash that lost the
///    link entry would leave the canonical chain intact but the configured root unreachable.
///    Nothing is resolved or created on this pass: it only flushes the entries an existing level
///    already has. With no symlink on the path it repeats the first pass, at one `fsync` per level.
///
/// Both walks are idempotent, not first-creation-only: every level from the root up to the
/// filesystem root has its parent synced unconditionally — including levels that already existed
/// and levels an earlier start created but never proved durable. Nothing records which entry a
/// failed start left unproven, so a restart redoes the whole chain.
///
/// A level another process created between a start's look and its `create_dir` is not a failure:
/// `AlreadyExists` is the same outcome, and the entry still gets its sync. A level that is not a
/// directory is refused under its own name, top-down, before anything is created beneath it.
fn durable_root(root: &Path) -> std::io::Result<()> {
    let canonical = canonical_root(root)?;
    // Leaf first, so the same vector serves the top-down create pass in reverse.
    let mut chain: Vec<&Path> = Vec::new();
    let mut cursor = Some(canonical.as_path());
    while let Some(level) = cursor {
        chain.push(level);
        cursor = level.parent().filter(|parent| !parent.as_os_str().is_empty());
    }
    // Top-down: every level exists, then its entry in the directory above is made durable. The
    // filesystem root is its own level and has no parent holding its entry.
    for level in chain.iter().rev() {
        // A level is refused under its own name here, before anything is created beneath it: an
        // ancestor that is a regular file is the one that must be named, not the level below.
        match std::fs::metadata(level) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(not_a_directory(level)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                #[cfg(test)]
                create_concurrently(level);
                match std::fs::create_dir(level) {
                    Ok(()) => {}
                    // Another process may have created the level between the look above and this
                    // call; that is the same outcome, and the entry still gets its sync.
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !std::fs::metadata(level)?.is_dir() {
                            return Err(not_a_directory(level));
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
        if let Some(parent) = level.parent() {
            sync_parent(parent)?;
        }
    }
    sync_configured_chain(root)
}

/// The entries naming `root` as it was configured, symlink included: for every level that exists,
/// its parent is flushed. Nothing is created and nothing is resolved — a level that is not there
/// was never established at that spelling, so it has no entry to make durable.
fn sync_configured_chain(root: &Path) -> std::io::Result<()> {
    let mut cursor = Some(root);
    while let Some(level) = cursor {
        if std::fs::symlink_metadata(level).is_ok() {
            // `Path::new("journal").parent()` is the empty path, which names the current
            // directory; only `None` — the filesystem root — has no directory holding its entry.
            let parent = match level.parent() {
                None => None,
                Some(parent) if parent.as_os_str().is_empty() => Some(Path::new(".")),
                Some(parent) => Some(parent),
            };
            if let Some(parent) = parent {
                sync_parent(parent)?;
            }
        }
        cursor = level.parent().filter(|parent| !parent.as_os_str().is_empty());
    }
    Ok(())
}

/// The root as the filesystem resolves it: every symlink on the way replaced, so the synced
/// entries are the ones `journal_dir` will publish beneath.
///
/// The part below the nearest existing ancestor does not exist yet and so carries no symlink; it
/// is appended to that ancestor's canonical form verbatim. `NotFound` and `NotADirectory` both
/// climb: an ancestor that is a regular file still canonicalizes, and the walk above then refuses
/// it under its own name.
fn canonical_root(root: &Path) -> std::io::Result<PathBuf> {
    let mut missing: Vec<&std::ffi::OsStr> = Vec::new();
    let mut cursor = root;
    loop {
        match std::fs::canonicalize(cursor) {
            Ok(existing) => {
                let mut resolved = existing;
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                // `Path::new("journal").parent()` is the empty path, which names the current
                // directory — the one level whose parent is not `None` and yet holds its entry.
                let next = match cursor.parent() {
                    None => return Err(error),
                    Some(parent) if parent.as_os_str().is_empty() => Path::new("."),
                    Some(parent) => parent,
                };
                match cursor.file_name() {
                    Some(name) => missing.push(name),
                    None => return Err(error),
                }
                cursor = next;
            }
            Err(error) => return Err(error),
        }
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

/// A test seam: the next create of this exact level lands as another process's. `true` plants a
/// directory (the benign race), `false` a regular file (the refusal). Without it the window
/// between this walk's check and its `create_dir` cannot be entered from a test.
#[cfg(test)]
static CREATE_CONCURRENTLY: std::sync::Mutex<Vec<(PathBuf, bool)>> = std::sync::Mutex::new(Vec::new());

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
    use super::{durable_root, CREATE_CONCURRENTLY, INJECT_SYNC_FAILURE, SYNC_ATTEMPTS};
    use std::path::{Path, PathBuf};

    // Every seam is keyed by path and each test owns a fresh temporary root, so a shared global
    // needs no reset between tests — and a reset would clobber a test still running.

    /// The scratch directory as the walk sees it: the tests live under `/var/…` on macOS, which is
    /// a symlink to `/private/var`, so a raw temporary path is not a path any parent is keyed by.
    /// A level the walk will create does not resolve yet, so it is the canonical parent plus its
    /// own name.
    fn canonical(path: &Path) -> PathBuf {
        path.canonicalize().unwrap_or_else(|error| panic!("{}: {error}", path.display()))
    }

    fn syncs_of(path: &Path) -> usize {
        SYNC_ATTEMPTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|attempt| attempt.as_path() == path)
            .count()
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

        assert!(durable_root(&root).is_err(), "the injected sync fails the start");
        assert!(root.is_dir(), "the level was created before the sync failed");
        let after_first = syncs_of(&level_parent);
        assert_eq!(after_first, 1, "the failed attempt is counted");

        durable_root(&root).expect("the retry succeeds");
        // The retry adds one sync per pass — the canonical chain and the configured one, which
        // repeat each other when no symlink is on the path. `after_first` counts only the single
        // attempt the first start got through before the injected fault aborted it.
        assert_eq!(
            syncs_of(&level_parent),
            after_first + 2,
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

        // One sync per pass: the canonical chain and the configured one, which coincide here.
        assert_eq!(
            syncs_of(&upper),
            before_lower + 2,
            "the lower level's entry is synced in the upper directory"
        );
        assert_eq!(
            syncs_of(&scratch_path),
            before_upper + 2,
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
        let named = canonical(scratch.path()).join("blocker");
        assert!(
            message.contains(&named.display().to_string()),
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
    /// single-segment relative root is created there and its entry synced, not skipped.
    #[test]
    fn a_single_segment_relative_root_is_created_and_synced() {
        let scratch = tempfile::TempDir::new().expect("a scratch directory");
        let previous = std::env::current_dir().expect("a current directory");
        std::env::set_current_dir(scratch.path()).expect("enter the scratch directory");
        let result = durable_root(Path::new("journal"));
        let created = Path::new("journal").is_dir();
        std::env::set_current_dir(&previous).expect("leave the scratch directory");

        result.expect("a relative root starts");
        assert!(created, "the relative root is created in the current directory");
    }
}
