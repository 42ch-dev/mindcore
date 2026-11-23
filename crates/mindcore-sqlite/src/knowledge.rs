//! `KnowledgeEntryPort` adapter: one fixed collection over the store's shared pool.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use sqlx::{Executor, Sqlite, SqliteConnection, SqlitePool, Transaction};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use mindcore_base::cognition::carrier_record_violation;
use mindcore_base::knowledge::{adoption_anchor_parts, KnowledgeScope};
use spoke_operations::{
    assert_adoption_anchor_preserved, filter_knowledge_entries_by_scope,
    knowledge_entry_visible_to_viewpoint, spoke_reject, validate_cognition_module,
    validate_cognition_record, KnowledgeEntryPort, ScopedKnowledgePort, SpokeReject,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::knowledge_entry::{KnowledgeEntryModulesKey, KnowledgeEntryModulesValue};
use spoke_schemas::{KnowledgeEntry, Scope};

use crate::cognition::{
    gap_is_dedicated_appends, insert_record, read_append_prior_shape, read_records,
    read_records_through,
};
use crate::store::{MindStore, SnapshotSeam};

/// Hard bound on one list of a stored collection.
///
/// Every list decodes the collection's live rows before any disclosure predicate filters them,
/// so the collection is bounded at the query boundary instead of growing memory and work with its
/// live size. An over-bound collection refuses the list; it never truncates.
pub const SERVED_KNOWLEDGE_LIST_LIMIT: usize = 512;

/// Refuse a scope carrying a selection the knowledge face does not serve.
///
/// The host admission callback applies this to the scope it returns, before Connect merges it with
/// a list request: that merge keeps only the request's cognition selection, so a cognition
/// selection admitted but never validated would be dropped from the list scope while `get`/`put`
/// still see it. The routed store applies the same rule to every caller-declared scope.
pub fn validate_admission_scope(scope: &Scope) -> SpokeResult<()> {
    if scope.cognition.is_some()
        || scope.timeline_event_ids.is_some()
        || scope.timeline_scale.is_some()
        || scope.fork_id.is_some()
        || scope.extensions.is_some()
    {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "the knowledge face serves no cognition, timeline, fork or extension selection",
            None,
        );
    }
    SpokeResult::Ok(())
}

/// One stored entry together with the liveness marker its row carries: `superseded_by` names the
/// same-collection entry that replaced it, and stays absent while the entry is current.
#[derive(Debug)]
pub struct KnowledgeEntryLifecycle {
    pub entry: KnowledgeEntry,
    pub superseded_by: Option<String>,
}

/// What one explicit history prune did: the checkpoints it deleted for good and the earliest
/// revision the entry can still answer, or `{ deleted: 0, floor: None }` when it deleted nothing
/// and left the stored floor alone.
#[derive(Debug)]
pub struct PruneReport {
    pub deleted: u64,
    pub floor: Option<u64>,
}

/// A knowledge port bound to one collection.
///
/// The scope is fixed at construction and never changes: every statement filters on it, so the port
/// cannot read or write another collection's entries. `owner` and extension metadata travel inside
/// the payload and never select rows.
///
/// `put` persists the submitted payload unchanged except for the assigned `revision` (`1` on create,
/// stored + 1 on update) and, on update, the stored row's `created_at`, which the payload repeats so
/// payload and column agree; the SQL timestamp columns carry the store's own write bookkeeping.
/// Cognition records are the exception to "unchanged": they are stored as their own rows and the
/// payload keeps the module envelope without the inline array. A present `modules.cognition` is
/// gated here, so no caller can commit a module that the next derivation would reject.
pub struct ScopedKnowledgeStore {
    pool: SqlitePool,
    scope: KnowledgeScope,
    seam: Arc<SnapshotSeam>,
}

impl ScopedKnowledgeStore {
    pub fn new(pool: SqlitePool, scope: KnowledgeScope) -> Self {
        Self {
            pool,
            scope,
            seam: Arc::new(SnapshotSeam::default()),
        }
    }

    pub fn scope(&self) -> &KnowledgeScope {
        &self.scope
    }

    /// Every entry of this collection, ordered by `entry_id` ASC and bounded by
    /// [`SERVED_KNOWLEDGE_LIST_LIMIT`].
    ///
    /// The projection runs inside one read transaction: SQLite pins the snapshot at
    /// the first statement, so an append committed by another connection between the
    /// carrier payloads and the record rows is invisible to both — the listing is
    /// one whole committed state, never a carrier revision paired with records it
    /// never had.
    pub async fn list_entries(&self) -> SpokeResult<Vec<KnowledgeEntry>> {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                return internal(format!("knowledge entries could not be listed: {error}"))
            }
        };
        match entries(&mut tx, &self.scope.collection, &self.seam).await {
            SpokeResult::Ok(listed) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(listed),
                Err(error) => internal(format!("knowledge entries could not be listed: {error}")),
            },
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    /// One entry as it stood at `revision`: the payload and cognition head the store held then,
    /// projected from the records that existed at that revision.
    ///
    /// Checkpoint selection and record hydration share one read transaction, so the replay is a
    /// whole committed state. Revision `0`, a revision the entry never held and a missing entry
    /// answer the same generic `KnowledgeEntryNotFound`; a revision below the entry's persisted
    /// floor answers `KnowledgeEntryNotFound` with `details.reason` `HISTORY_TRUNCATED`. A
    /// superseded entry still replays from its own history.
    pub async fn get_entry_at_revision(
        &self,
        entry_id: &str,
        revision: u64,
    ) -> SpokeResult<KnowledgeEntry> {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                return internal(format!(
                    "knowledge entry revision could not be read: {error}"
                ))
            }
        };
        match read_entry_revision(&mut tx, &self.scope.collection, entry_id, revision).await {
            SpokeResult::Ok(entry) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(entry),
                Err(error) => internal(format!(
                    "knowledge entry revision could not be read: {error}"
                )),
            },
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    /// Mark `entry_id` superseded by another live entry of this collection, at
    /// `expected_base_revision`, and return the source's new revision.
    ///
    /// One `BEGIN IMMEDIATE` reservation covers both reads and the single update: the source must
    /// be live at the expected revision, the target a different live entry of the same collection,
    /// so a cycle cannot close on write. The source keeps its head, payload fields and history —
    /// only its marker, revision, DTO revision and store `updated_at` advance, and the revision
    /// trigger saves the displaced preimage as part of that update.
    pub async fn supersede_entry(
        &self,
        entry_id: &str,
        expected_base_revision: u64,
        superseding_entry_id: &str,
    ) -> SpokeResult<u64> {
        if entry_id == superseding_entry_id {
            return invalid_input("a knowledge entry cannot be superseded by itself");
        }
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => return internal(format!("knowledge supersede could not begin: {error}")),
        };
        let source = match read_stored_row(
            &mut transaction,
            &self.scope.collection,
            entry_id,
            false,
        )
        .await
        {
            SpokeResult::Ok(source) => source,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        let Ok(stored) = u64::try_from(source.revision) else {
            return internal(format!(
                "knowledge entry {entry_id} revision column is not a valid revision"
            ));
        };
        if source.superseded_by.is_some() {
            return invalid_input(format!("knowledge entry {entry_id} is already superseded"));
        }
        let mut marked = match decode(&source.payload) {
            SpokeResult::Ok(entry) => entry,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if marked.revision != Some(stored) {
            return internal(format!(
                "knowledge entry {entry_id} revision column and payload disagree"
            ));
        }
        if stored != expected_base_revision {
            return stale(entry_id, Some(expected_base_revision), Some(stored));
        }
        let target = match read_stored_row(
            &mut transaction,
            &self.scope.collection,
            superseding_entry_id,
            false,
        )
        .await
        {
            SpokeResult::Ok(target) => target,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if target.superseded_by.is_some() {
            return invalid_input(format!(
                "knowledge entry {superseding_entry_id} is already superseded"
            ));
        }
        let Some(next) = stored.checked_add(1) else {
            return internal(format!("knowledge entry {entry_id} revision is exhausted"));
        };
        let Ok(revision) = i64::try_from(next) else {
            return internal(format!("knowledge entry {entry_id} revision is exhausted"));
        };
        marked.revision = Some(next);
        let payload = match encode(&marked) {
            SpokeResult::Ok(payload) => payload,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        let written = sqlx::query(
            "UPDATE knowledge_entries \
             SET superseded_by = ?, revision = ?, payload = ?, updated_at = ? \
             WHERE collection = ? AND entry_id = ?",
        )
        .bind(superseding_entry_id)
        .bind(revision)
        .bind(&payload)
        .bind(encode_timestamp(Utc::now()))
        .bind(&self.scope.collection)
        .bind(entry_id)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = written {
            return internal(format!("knowledge supersede could not be written: {error}"));
        }
        if let Err(error) = transaction.commit().await {
            return internal(format!("knowledge supersede could not commit: {error}"));
        }
        SpokeResult::Ok(next)
    }

    /// Delete this entry's checkpoints older than its `keep` newest and move the entry's floor to
    /// the earliest revision it can still answer: the revision just above the newest deleted
    /// checkpoint. That floor need not be a stored checkpoint — a dedicated append advances the
    /// revision without writing a preimage, so a revision between surviving checkpoints is still
    /// replayed from the next one.
    ///
    /// `keep` counts checkpoints, newest first; the live row is not one. The read, the delete and
    /// the floor update share one `BEGIN IMMEDIATE` reservation, so no write can append or capture
    /// history between the count and the cut. The deletion is permanent: the removed preimages are
    /// not archived and can never be replayed. Only `history_floor_revision` is written on the
    /// entry row, so the `BEFORE UPDATE OF revision` trigger saves no checkpoint of this update.
    /// A prune that removes nothing reports `deleted: 0, floor: None` and leaves the stored floor
    /// and every checkpoint alone.
    pub async fn prune_history(&self, entry_id: &str, keep: u64) -> SpokeResult<PruneReport> {
        if keep == 0 {
            return invalid_input("a history prune keeps at least one checkpoint");
        }
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => return internal(format!("history prune could not begin: {error}")),
        };
        let live: Result<Option<i64>, sqlx::Error> = sqlx::query_scalar(
            "SELECT 1 FROM knowledge_entries \
             WHERE collection = ? AND entry_id = ? AND superseded_by IS NULL",
        )
        .bind(&self.scope.collection)
        .bind(entry_id)
        .fetch_optional(&mut *transaction)
        .await;
        match live {
            Ok(Some(_)) => {}
            Ok(None) => return entry_not_found(&self.scope.collection, entry_id),
            Err(error) => return internal(format!("knowledge entry could not be read: {error}")),
        }
        let offset = i64::try_from(keep - 1).unwrap_or(i64::MAX);
        let checkpoints: Result<Vec<i64>, sqlx::Error> = sqlx::query_scalar(
            "SELECT revision FROM knowledge_entry_revisions \
             WHERE collection = ? AND entry_id = ? \
             ORDER BY revision DESC LIMIT 2 OFFSET ?",
        )
        .bind(&self.scope.collection)
        .bind(entry_id)
        .bind(offset)
        .fetch_all(&mut *transaction)
        .await;
        let checkpoints = match checkpoints {
            Ok(checkpoints) => checkpoints,
            Err(error) => {
                return internal(format!("history checkpoints could not be read: {error}"))
            }
        };
        if checkpoints.len() < 2 {
            if let Err(error) = transaction.commit().await {
                return internal(format!("history prune could not commit: {error}"));
            }
            return SpokeResult::Ok(PruneReport {
                deleted: 0,
                floor: None,
            });
        }
        let new_floor = checkpoints[1] + 1;
        let deleted = match sqlx::query(
            "DELETE FROM knowledge_entry_revisions \
             WHERE collection = ? AND entry_id = ? AND revision < ?",
        )
        .bind(&self.scope.collection)
        .bind(entry_id)
        .bind(new_floor)
        .execute(&mut *transaction)
        .await
        {
            Ok(deleted) => deleted.rows_affected(),
            Err(error) => {
                return internal(format!("history checkpoints could not be deleted: {error}"))
            }
        };
        let updated = sqlx::query(
            "UPDATE knowledge_entries SET history_floor_revision = ? \
             WHERE collection = ? AND entry_id = ?",
        )
        .bind(new_floor)
        .bind(&self.scope.collection)
        .bind(entry_id)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = updated {
            return internal(format!("history floor could not be written: {error}"));
        }
        if let Err(error) = transaction.commit().await {
            return internal(format!("history prune could not commit: {error}"));
        }
        SpokeResult::Ok(PruneReport {
            deleted,
            floor: Some(new_floor as u64),
        })
    }

    /// One entry of this collection whether or not it is superseded: the payload (with its
    /// cognition records projected) and the marker the row carries.
    ///
    /// [`KnowledgeEntryPort::get_knowledge_entry`] serves live rows only; this is the explicit
    /// local request that still returns a superseded entry, and it never follows the marker.
    pub async fn get_entry_including_superseded(
        &self,
        entry_id: &str,
    ) -> SpokeResult<KnowledgeEntryLifecycle> {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => return internal(format!("knowledge entry could not be read: {error}")),
        };
        let mut lifecycle = match read_lifecycle(&mut tx, &self.scope.collection, entry_id).await {
            SpokeResult::Ok(lifecycle) => lifecycle,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if let SpokeResult::Reject(reject) = hydrate_cognition_records(
            &mut tx,
            &self.scope.collection,
            std::slice::from_mut(&mut lifecycle.entry),
            &self.seam,
        )
        .await
        {
            return SpokeResult::Reject(reject);
        }
        match tx.commit().await {
            Ok(()) => SpokeResult::Ok(lifecycle),
            Err(error) => internal(format!("knowledge entry could not be read: {error}")),
        }
    }

    /// The live entry an adoption anchor resolves to: the anchor's source entry id, then each
    /// same-collection marker until the first live entry.
    ///
    /// One read transaction pins the whole chain to one snapshot. Every hop passes the
    /// viewpoint-free disclosure predicate before its successor is read, so a missing or hidden
    /// hop answers with the one absence keyed to the id the anchor names, never a partial chain.
    /// Ids are tracked, so a cycle in stored markers is a storage failure instead.
    pub async fn resolve_adoption_anchor(&self, adopted: &KnowledgeEntry) -> SpokeResult<String> {
        let Some((_, source_id, _)) = adoption_anchor_parts(adopted) else {
            return invalid_input(format!(
                "knowledge entry {} carries no adoption anchor",
                adopted.entry_id
            ));
        };
        let source_id = source_id.to_string();
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                return internal(format!(
                    "adoption anchor {source_id} could not be resolved: {error}"
                ))
            }
        };
        let mut current = source_id.clone();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current.clone()) {
                return internal(format!(
                    "adoption anchor {source_id} resolves through a cycle at knowledge entry \
                     {current}"
                ));
            }
            let lifecycle = match read_lifecycle(&mut tx, &self.scope.collection, &current).await {
                SpokeResult::Ok(lifecycle) => lifecycle,
                SpokeResult::Reject(reject)
                    if reject.code == SpokeRejectCode::KnowledgeEntryNotFound =>
                {
                    return entry_not_found(&self.scope.collection, &source_id)
                }
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            };
            if !knowledge_entry_visible_to_viewpoint(&lifecycle.entry, None) {
                return entry_not_found(&self.scope.collection, &source_id);
            }
            match lifecycle.superseded_by {
                None => return SpokeResult::Ok(current),
                Some(successor) => current = successor,
            }
        }
    }
}

/// Every entry of `collection`, ordered by `entry_id` ASC, read through the
/// caller's transaction so a workflow reads one whole snapshot at one point.
///
/// Bounded by [`SERVED_KNOWLEDGE_LIST_LIMIT`] like the served list: the remote
/// workflow's knowledge slot must not read and decode a collection of any size.
pub async fn list_entries_in(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
) -> SpokeResult<Vec<KnowledgeEntry>> {
    entries(&mut *tx, collection, &SnapshotSeam::default()).await
}

/// The live rows of `collection`, ordered by `entry_id` ASC, bounded at the query
/// boundary: at most [`SERVED_KNOWLEDGE_LIST_LIMIT`] live rows are decoded, and an
/// over-bound live set refuses the list instead of growing memory and work with its
/// size. The live predicate rides in the one payload statement, so hiding a retired
/// row costs a predicate and no additional statement — the bound counts live rows,
/// so retirement frees budget rather than consuming it.
///
/// The record rows of every carrying live entry are one page-shaped statement, not
/// one statement per entry: the page's holders travel as a `json_each` list.
async fn entries(
    connection: &mut SqliteConnection,
    collection: &str,
    seam: &SnapshotSeam,
) -> SpokeResult<Vec<KnowledgeEntry>> {
    let limit = SERVED_KNOWLEDGE_LIST_LIMIT as i64;
    let rows: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT payload FROM knowledge_entries \
         WHERE collection = ? AND superseded_by IS NULL ORDER BY entry_id ASC LIMIT ?",
    )
    .bind(collection)
    .bind(limit + 1)
    .fetch_all(&mut *connection)
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) => return internal(format!("knowledge entries could not be listed: {error}")),
    };
    if rows.len() as i64 > limit {
        return over_bound();
    }
    let mut stored = Vec::with_capacity(rows.len());
    for payload in &rows {
        match decode(payload) {
            SpokeResult::Ok(entry) => stored.push(entry),
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
    }
    if let SpokeResult::Reject(reject) =
        hydrate_cognition_records(connection, collection, &mut stored, seam).await
    {
        return SpokeResult::Reject(reject);
    }
    SpokeResult::Ok(stored)
}

impl MindStore {
    /// A port for `scope`, sharing this store's single pool and its read-projection seam.
    pub fn knowledge(&self, scope: KnowledgeScope) -> ScopedKnowledgeStore {
        ScopedKnowledgeStore {
            pool: self.pool.clone(),
            scope,
            seam: Arc::clone(&self.seam),
        }
    }

    /// The connect-served knowledge port: the admitted `scope_id` selects `scope`'s collection.
    pub fn served_knowledge(
        &self,
        scope_id: impl Into<String>,
        scope: KnowledgeScope,
    ) -> RoutedKnowledgeStore {
        RoutedKnowledgeStore::new(self.pool.clone(), scope_id, scope)
    }
}

/// The connect-served knowledge face over the store's shared pool.
///
/// The host configures one exact route: an admitted `Scope` naming `scope_id` reads and writes
/// `scope`'s collection, and any other id is `NotAuthorized` before a statement runs — no peer,
/// payload or fallback picks a collection.
///
/// Every op applies the admitted refinements and SPOKE's disclosure predicate, so an entry hidden
/// from the peer answers like a missing one. `put` judges the submitted entry, and the existing row
/// in the same `BEGIN IMMEDIATE` transaction, before the common store can reveal a revision; a
/// hidden existing row is `NotAuthorized` and never overwritten.
pub struct RoutedKnowledgeStore {
    pool: SqlitePool,
    scope_id: String,
    scope: KnowledgeScope,
}

impl RoutedKnowledgeStore {
    fn new(pool: SqlitePool, scope_id: impl Into<String>, scope: KnowledgeScope) -> Self {
        Self {
            pool,
            scope_id: scope_id.into(),
            scope,
        }
    }

    /// The admission every op starts with: an unconfigured `scope_id` and a selection this face
    /// does not serve are refused instead of ignored.
    fn admit(&self, scope: &Scope) -> SpokeResult<()> {
        if scope.scope_id != self.scope_id {
            return not_authorized();
        }
        validate_admission_scope(scope)
    }

    /// The routed collection's live rows, bounded by [`SERVED_KNOWLEDGE_LIST_LIMIT`]: an
    /// over-bound live set refuses the list instead of growing memory and work with its size.
    /// The projection runs inside one read transaction, so the carrier payloads and the
    /// record rows project from one whole committed state.
    async fn served_entries(&self) -> SpokeResult<Vec<KnowledgeEntry>> {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(_) => return storage_unavailable(),
        };
        match entries(&mut tx, &self.scope.collection, &SnapshotSeam::default()).await {
            SpokeResult::Ok(listed) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(listed),
                Err(_) => storage_unavailable(),
            },
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    /// One served read: the collection's one route, the caller's admission, and the disclosure
    /// predicate all apply before the entry leaves the store.
    async fn served_get(&self, scope: &Scope, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        if let SpokeResult::Reject(reject) = self.admit(scope) {
            return SpokeResult::Reject(reject);
        }
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(_) => return storage_unavailable(),
        };
        let entry = match read_entry(&mut tx, &self.scope.collection, entry_id).await {
            SpokeResult::Ok(entry) => match tx.commit().await {
                Ok(()) => entry,
                Err(_) => return storage_unavailable(),
            },
            SpokeResult::Reject(reject)
                if reject.code == SpokeRejectCode::KnowledgeEntryNotFound =>
            {
                return not_found(entry_id)
            }
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        match visible(&entry, scope) {
            SpokeResult::Ok(true) => SpokeResult::Ok(entry),
            SpokeResult::Ok(false) => not_found(entry_id),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    /// One served write: the submitted entry, and the existing row, are judged before the store
    /// can reveal a revision.
    async fn served_put(
        &self,
        scope: &Scope,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        if let SpokeResult::Reject(reject) = self.admit(scope) {
            return SpokeResult::Reject(reject);
        }
        match visible(&entry, scope) {
            SpokeResult::Ok(true) => {}
            SpokeResult::Ok(false) => return not_authorized(),
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(_) => return storage_unavailable(),
        };
        let existing = read_stored_row(
            &mut transaction,
            &self.scope.collection,
            &entry.entry_id,
            false,
        )
        .await;
        let existing = match existing {
            SpokeResult::Ok(existing) => Some(existing),
            SpokeResult::Reject(reject)
                if reject.code == SpokeRejectCode::KnowledgeEntryNotFound =>
            {
                None
            }
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if let Some(existing) = existing {
            if existing.superseded_by.is_some() {
                return not_authorized();
            }
            let existing = match decode(&existing.payload) {
                SpokeResult::Ok(existing) => existing,
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            };
            match visible(&existing, scope) {
                SpokeResult::Ok(true) => {}
                SpokeResult::Ok(false) => return not_authorized(),
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            }
        }
        let written = write_entry(
            &mut transaction,
            &self.scope.collection,
            entry,
            expected_base_revision,
        )
        .await;
        let entry = match written {
            SpokeResult::Ok(entry) => entry,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if transaction.commit().await.is_err() {
            return storage_unavailable();
        }
        SpokeResult::Ok(entry)
    }

    /// The served list: the collection's live rows, filtered by the caller's disclosure predicate.
    async fn served_list(&self, scope: &Scope) -> SpokeResult<Vec<KnowledgeEntry>> {
        if let SpokeResult::Reject(reject) = self.admit(scope) {
            return SpokeResult::Reject(reject);
        }
        let entries = match self.served_entries().await {
            SpokeResult::Ok(entries) => entries,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        match filter_knowledge_entries_by_scope(&entries, scope) {
            SpokeResult::Ok(visible) => SpokeResult::Ok(visible.into_iter().cloned().collect()),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }
}

/// Every op reports a failed statement as [`storage_unavailable`]: the store's shared helpers
/// build their message for the local port, and it names local tables and columns.
#[async_trait]
impl ScopedKnowledgePort for RoutedKnowledgeStore {
    async fn get_scoped_knowledge_entry(
        &self,
        scope: &Scope,
        entry_id: &str,
    ) -> SpokeResult<KnowledgeEntry> {
        served(self.served_get(scope, entry_id).await)
    }

    async fn put_scoped_knowledge_entry(
        &self,
        scope: &Scope,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        served(self.served_put(scope, entry, expected_base_revision).await)
    }

    async fn list_scoped_knowledge_entries(
        &self,
        scope: &Scope,
    ) -> SpokeResult<Vec<KnowledgeEntry>> {
        served(self.served_list(scope).await)
    }
}

fn not_authorized<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::NotAuthorized,
        "knowledge scope not authorized",
        None,
    )
}

/// One entry is visible when SPOKE's scope predicate keeps it.
fn visible(entry: &KnowledgeEntry, scope: &Scope) -> SpokeResult<bool> {
    match filter_knowledge_entries_by_scope(std::slice::from_ref(entry), scope) {
        SpokeResult::Ok(visible) => SpokeResult::Ok(!visible.is_empty()),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    }
}

fn not_found<T>(entry_id: &str) -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::KnowledgeEntryNotFound,
        format!("no knowledge entry {entry_id} in the admitted scope"),
        None,
    )
}

fn revision_not_found<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::KnowledgeEntryNotFound,
        "knowledge entry revision not found",
        None,
    )
}

fn revision_pruned<T>() -> SpokeResult<T> {
    let mut details = Map::new();
    details.insert(
        "reason".to_string(),
        Value::String("HISTORY_TRUNCATED".to_string()),
    );
    spoke_reject(
        SpokeRejectCode::KnowledgeEntryNotFound,
        "knowledge entry revision was pruned",
        Some(details),
    )
}

pub(crate) fn internal<T>(message: impl Into<String>) -> SpokeResult<T> {
    spoke_reject(SpokeRejectCode::InternalError, message, None)
}

fn invalid_input<T>(message: impl Into<String>) -> SpokeResult<T> {
    spoke_reject(SpokeRejectCode::InvalidInput, message, None)
}

/// The one answer a failed statement gives on the served face: a `sqlx::Error::Database` names
/// local tables and columns, and this face forwards its message to an admitted peer. The local
/// port keeps the detailed message for the host's own logs.
fn storage_unavailable<T>() -> SpokeResult<T> {
    internal("knowledge storage is unavailable")
}

/// Replace every internal refusal a served op produced with [`storage_unavailable`], including
/// the ones the shared store helpers build for the local port. The over-bound list refusal is the
/// exception: it is about the peer's own collection, names no local schema and carries its
/// capacity class, so it is forwarded unchanged.
fn served<T>(result: SpokeResult<T>) -> SpokeResult<T> {
    match result {
        SpokeResult::Reject(reject)
            if reject.code == SpokeRejectCode::InternalError && !over_capacity(&reject) =>
        {
            storage_unavailable()
        }
        other => other,
    }
}

/// The capacity class an over-bound list refusal carries: both faces answer it, so a caller tells
/// a collection that is over the bound from a storage fault.
const OVER_CAPACITY_REASON: &str = "COLLECTION_OVER_CAPACITY";

/// The over-bound list refusal. It names the bound it crossed and its own class, and nothing
/// local: the served list forwards exactly this refusal to a peer.
fn over_bound<T>() -> SpokeResult<T> {
    let mut details = Map::new();
    details.insert(
        "reason".to_string(),
        Value::String(OVER_CAPACITY_REASON.to_string()),
    );
    details.insert(
        "limit".to_string(),
        Value::from(SERVED_KNOWLEDGE_LIST_LIMIT),
    );
    spoke_reject(
        SpokeRejectCode::InternalError,
        format!(
            "the collection holds more than {SERVED_KNOWLEDGE_LIST_LIMIT} live entries; \
             the list is bounded"
        ),
        Some(details),
    )
}

/// Whether a refusal is the over-bound list refusal, which the served face must forward intact.
fn over_capacity(reject: &SpokeReject) -> bool {
    reject
        .details
        .as_ref()
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str)
        == Some(OVER_CAPACITY_REASON)
}

fn entry_not_found<T>(collection: &str, entry_id: &str) -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::KnowledgeEntryNotFound,
        format!("no knowledge entry {entry_id} in collection {collection}"),
        None,
    )
}

fn decode(payload: &str) -> SpokeResult<KnowledgeEntry> {
    match serde_json::from_str(payload) {
        Ok(entry) => SpokeResult::Ok(entry),
        Err(error) => internal(format!("stored knowledge payload is not an entry: {error}")),
    }
}

fn encode(entry: &KnowledgeEntry) -> SpokeResult<String> {
    match serde_json::to_string(entry) {
        Ok(payload) => SpokeResult::Ok(payload),
        Err(error) => internal(format!("knowledge entry could not be serialized: {error}")),
    }
}

fn encode_timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn module_key() -> KnowledgeEntryModulesKey {
    "cognition".parse().expect("valid module namespace")
}

fn cognition_module(entry: &KnowledgeEntry) -> Option<Value> {
    match entry.modules.get(&module_key())? {
        KnowledgeEntryModulesValue::Object(module) => Some(Value::Object(module.clone())),
        KnowledgeEntryModulesValue::Array(items) => Some(Value::Array(items.clone())),
    }
}

fn append_prior_shape(entry: &KnowledgeEntry) -> i64 {
    match entry.modules.get(&module_key()) {
        None => 1,
        Some(KnowledgeEntryModulesValue::Object(module)) if module.contains_key("records") => 0,
        Some(_) => 2,
    }
}

fn stale<T>(entry_id: &str, expected: Option<u64>, stored: Option<u64>) -> SpokeResult<T> {
    let mut details = Map::new();
    details.insert("expected_base_revision".to_string(), Value::from(expected));
    details.insert("stored_revision".to_string(), Value::from(stored));
    spoke_reject(
        SpokeRejectCode::StoredRevisionStale,
        format!("knowledge entry {entry_id} does not hold the expected revision"),
        Some(details),
    )
}

/// One live entry of `collection`, read through the caller's transaction: the scoped
/// port and the workflow binding share this read, and a settlement reads it at
/// the same point as the write it guards. The cognition module's records are
/// projected from the holder's record rows in append order. Taking the
/// transaction pins the payload and the record rows to one snapshot — the same
/// whole read `list_entries_in` makes. A superseded row answers like an absent
/// one; explicit replay and [`ScopedKnowledgeStore::get_entry_including_superseded`]
/// stay the paths that still see it.
pub async fn read_entry(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<KnowledgeEntry> {
    read_entry_seeded(tx, collection, entry_id, &SnapshotSeam::default()).await
}

/// [`read_entry`] with the caller's store seam, so a unit test can hold the read
/// between its payload and record statements. Callers outside a store pass a
/// fresh seam, which nothing can install.
async fn read_entry_seeded(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
    seam: &SnapshotSeam,
) -> SpokeResult<KnowledgeEntry> {
    let stored = match read_stored_row(tx, collection, entry_id, true).await {
        SpokeResult::Ok(stored) => stored,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let mut entry = match decode(&stored.payload) {
        SpokeResult::Ok(entry) => entry,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    if let SpokeResult::Reject(reject) =
        hydrate_cognition_records(&mut *tx, collection, std::slice::from_mut(&mut entry), seam)
            .await
    {
        return SpokeResult::Reject(reject);
    }
    SpokeResult::Ok(entry)
}

/// One stored row of `collection`, read through the caller's transaction.
///
/// `live_only` puts the live-row predicate in the query instead of checking the marker after the
/// payload is decoded: a retired row's payload may be unreadable and must still answer as an
/// absence on the current-entry paths. Only callers that must decide about a marked row read one —
/// the explicit read, the routed put preflight and the marking CAS. The revision column has two
/// readers, both CAS bases: [`write_entry`] and [`supersede_entry`]. Only the marking path compares
/// the column against the payload it also reads and refuses a disagreement; `write_entry` compares
/// the column against the caller's expected revision and never inspects the stored payload, so it
/// cannot detect that drift.
struct StoredRow {
    revision: i64,
    payload: String,
    superseded_by: Option<String>,
}

async fn read_stored_row(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
    live_only: bool,
) -> SpokeResult<StoredRow> {
    let sql = if live_only {
        "SELECT revision, payload, superseded_by FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ? AND superseded_by IS NULL"
    } else {
        "SELECT revision, payload, superseded_by FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?"
    };
    let stored: Result<Option<(i64, String, Option<String>)>, sqlx::Error> = sqlx::query_as(sql)
        .bind(collection)
        .bind(entry_id)
        .fetch_optional(&mut **tx)
        .await;
    match stored {
        Ok(Some((revision, payload, superseded_by))) => SpokeResult::Ok(StoredRow {
            revision,
            payload,
            superseded_by,
        }),
        Ok(None) => entry_not_found(collection, entry_id),
        Err(error) => internal(format!("knowledge entry could not be read: {error}")),
    }
}

/// One stored entry of `collection` and the marker its row carries, read through the caller's
/// transaction. The payload is returned as written — one statement selects the marker with it —
/// so a caller that re-encodes it keeps the same cognition envelope and never copies the record
/// rows back inline; [`read_entry`] projects those rows on top.
async fn read_lifecycle(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<KnowledgeEntryLifecycle> {
    let stored: Result<Option<(String, Option<String>)>, sqlx::Error> = sqlx::query_as(
        "SELECT payload, superseded_by FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(&mut **tx)
    .await;
    match stored {
        Ok(Some((payload, superseded_by))) => match decode(&payload) {
            SpokeResult::Ok(entry) => SpokeResult::Ok(KnowledgeEntryLifecycle {
                entry,
                superseded_by,
            }),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        },
        Ok(None) => entry_not_found(collection, entry_id),
        Err(error) => internal(format!("knowledge entry could not be read: {error}")),
    }
}

/// One entry-led replay selection: the stored floor, the chosen checkpoint's revision, its payload
/// and head, and the live revision.
type RevisionSelection = (i64, i64, String, Option<i64>, i64);

/// One entry of `collection` as it was stored at `revision`, read through the caller's
/// transaction so checkpoint selection and record hydration project one snapshot.
///
/// The entry row leads the read: it carries the live revision and the floor below which preimages
/// were pruned, and the first saved checkpoint at or above the request joins it. A request below
/// the floor is refused as pruned before the live fallback can answer it with current content.
/// The first saved checkpoint at or above the requested revision supplies the payload and head;
/// the live row stands in when no saved revision is that recent. The gap between them is advanced
/// by dedicated appends alone, so it subtracts from the checkpoint head and the records are read
/// only through that derived head — a later record never reaches an earlier replay.
async fn read_entry_revision(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
    revision: u64,
) -> SpokeResult<KnowledgeEntry> {
    let Ok(assigned) = i64::try_from(revision) else {
        return revision_not_found();
    };
    if assigned < 1 {
        return revision_not_found();
    }
    let selected: Result<Option<RevisionSelection>, sqlx::Error> = sqlx::query_as(
        "SELECT k.history_floor_revision, COALESCE(r.revision, k.revision), \
                COALESCE(r.payload, k.payload), \
                CASE WHEN r.revision IS NULL THEN k.cognition_head_seq \
                     ELSE r.cognition_head_seq END, \
                k.revision \
         FROM knowledge_entries AS k \
         LEFT JOIN knowledge_entry_revisions AS r \
           ON r.collection = k.collection AND r.entry_id = k.entry_id \
          AND r.revision >= ? AND ? >= k.history_floor_revision AND ? <= k.revision \
         WHERE k.collection = ? AND k.entry_id = ? \
         ORDER BY r.revision ASC LIMIT 1",
    )
    .bind(assigned)
    .bind(assigned)
    .bind(assigned)
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(&mut **tx)
    .await;
    let (floor, checkpoint_revision, payload, head, live) = match selected {
        Ok(Some(selected)) => selected,
        Ok(None) => return revision_not_found(),
        Err(error) => {
            return internal(format!(
                "knowledge revision history could not be read: {error}"
            ))
        }
    };
    if assigned < floor {
        return revision_pruned();
    }
    if assigned > live {
        return revision_not_found();
    }
    let gap = checkpoint_revision - assigned;
    let mut entry = match decode(&payload) {
        SpokeResult::Ok(entry) => entry,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    entry.revision = Some(revision);
    let checkpoint_head = head;
    let head = match checkpoint_head {
        Some(head) => head - gap,
        None if gap == 0 => 0,
        None => {
            return internal(format!(
                "knowledge entry {entry_id} revision {revision} has an impossible history gap"
            ))
        }
    };
    if head < 0 {
        return internal(format!(
            "knowledge entry {entry_id} revision {revision} has an impossible cognition head"
        ));
    }
    if gap > 0 {
        let dedicated =
            match gap_is_dedicated_appends(&mut **tx, collection, entry_id, head, gap).await {
                SpokeResult::Ok(dedicated) => dedicated,
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            };
        if !dedicated {
            return internal(format!(
                "knowledge entry {entry_id} revision {revision} has a contradicted history gap"
            ));
        }
    }
    if head > 0 {
        let records = match read_records_through(&mut **tx, collection, entry_id, head).await {
            SpokeResult::Ok(records) => records,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if let Some(KnowledgeEntryModulesValue::Object(module)) =
            entry.modules.get_mut(&module_key())
        {
            module.insert("records".to_string(), Value::Array(records));
        }
    } else if gap > 0 {
        let shape = match read_append_prior_shape(&mut **tx, collection, entry_id, 1).await {
            SpokeResult::Ok(shape) => shape,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        match shape {
            Some(1) => {
                entry.modules.remove(&module_key());
            }
            Some(2) => {
                if let Some(KnowledgeEntryModulesValue::Object(module)) =
                    entry.modules.get_mut(&module_key())
                {
                    module.remove("records");
                }
            }
            Some(0) => {}
            Some(shape) => {
                return internal(format!(
                    "knowledge entry {entry_id} first append carries shape {shape}"
                ))
            }
            None => {
                return internal(format!(
                    "knowledge entry {entry_id} history has no first-append shape"
                ))
            }
        }
    }
    SpokeResult::Ok(entry)
}

/// Projects `modules.cognition.records` of every `entries` member from the holder's record
/// rows: one statement for the whole list, keyed by the page's entries, and the rows grouped in
/// memory in `append_seq` order. A present `records` member stays present (empty while there are
/// no rows); an absent module or member stays absent.
async fn hydrate_cognition_records(
    connection: &mut SqliteConnection,
    collection: &str,
    entries: &mut [KnowledgeEntry],
    seam: &SnapshotSeam,
) -> SpokeResult<()> {
    let holders: Vec<&str> = entries
        .iter()
        .filter(|entry| carries_cognition_records(entry))
        .map(|entry| entry.entry_id.as_str())
        .collect();
    if holders.is_empty() {
        return SpokeResult::Ok(());
    }
    let holders = match serde_json::to_string(&holders) {
        Ok(holders) => holders,
        Err(error) => return internal(format!("cognition holders could not be rendered: {error}")),
    };
    seam.interleave().await;
    let rows: Result<Vec<(String, String)>, sqlx::Error> = sqlx::query_as(
        "SELECT entry_id, payload FROM cognition_records \
         WHERE collection = ? AND entry_id IN (SELECT value FROM json_each(?)) \
         ORDER BY entry_id ASC, append_seq ASC",
    )
    .bind(collection)
    .bind(holders)
    .fetch_all(&mut *connection)
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) => return internal(format!("cognition records could not be listed: {error}")),
    };
    let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
    for (entry_id, payload) in rows {
        match serde_json::from_str(&payload) {
            Ok(record) => grouped.entry(entry_id).or_default().push(record),
            Err(error) => return internal(format!("stored cognition record is not JSON: {error}")),
        }
    }
    for entry in entries
        .iter_mut()
        .filter(|entry| carries_cognition_records(entry))
    {
        let records = grouped.remove(&entry.entry_id).unwrap_or_default();
        if let Some(KnowledgeEntryModulesValue::Object(module)) =
            entry.modules.get_mut(&module_key())
        {
            module.insert("records".to_string(), Value::Array(records));
        }
    }
    SpokeResult::Ok(())
}

/// Whether the entry's payload keeps a present `modules.cognition.records` member, the only shape
/// the storage authority hydrates.
fn carries_cognition_records(entry: &KnowledgeEntry) -> bool {
    matches!(
        entry.modules.get(&module_key()),
        Some(KnowledgeEntryModulesValue::Object(module)) if module.contains_key("records")
    )
}

/// One live holder's assigned revision and current-judgment pointer, read without hydrating its
/// payload or history. A superseded entry has no current judgment to extend, so the marker's
/// predicate rides in this statement and the row answers like an absent one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarrierHeader {
    pub revision: u64,
    pub head_seq: Option<i64>,
}

/// One holder's header, read through the caller's executor: the append path compares the
/// revision and takes the next `append_seq` from here instead of reading the carrier history.
pub async fn read_carrier_header<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<CarrierHeader>
where
    E: Executor<'e, Database = Sqlite>,
{
    let header: Result<Option<(i64, Option<i64>)>, sqlx::Error> = sqlx::query_as(
        "SELECT revision, cognition_head_seq FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ? AND superseded_by IS NULL",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(executor)
    .await;
    match header {
        Ok(Some((revision, head_seq))) => SpokeResult::Ok(CarrierHeader {
            revision: revision as u64,
            head_seq,
        }),
        Ok(None) => spoke_reject(
            SpokeRejectCode::KnowledgeEntryNotFound,
            format!("no knowledge entry {entry_id} in collection {collection}"),
            None,
        ),
        Err(error) => internal(format!("carrier header could not be read: {error}")),
    }
}

/// The stored row `write_entry` reads: revision, creation instant, liveness marker and the payload
/// whose anchor the put must preserve.
type StoredEntryRow = (i64, String, Option<String>, String);

/// Write one entry of `collection` against `expected_base_revision` through the
/// caller's transaction, assigning `1` on create and `stored + 1` on update.
///
/// The submitted cognition records are stored as their own rows: on create all of them, on
/// update only the suffix behind the immutable committed prefix — dropping, reordering or
/// rewriting a committed record is invalid input. The stored payload keeps the module
/// envelope without the inline array.
///
/// An update also restores the stored row's `created_at` into the payload: the
/// SQL column keeps the original creation instant across an update, so the
/// payload must not carry a fresher one.
///
/// The caller holds the write reservation, so the revision comparison and the
/// write cannot interleave with another handle on the same file.
/// The stored payload carries the `adoption_anchor` the put must preserve, so the
/// statement this read issues is the gate's only read.
pub async fn write_entry(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    mut entry: KnowledgeEntry,
    expected_base_revision: Option<u64>,
) -> SpokeResult<KnowledgeEntry> {
    if let Some(module) = cognition_module(&entry) {
        if let SpokeResult::Reject(reject) = validate_cognition_module(&module) {
            return SpokeResult::Reject(reject);
        }
        if let Some(records) = module.get("records").and_then(Value::as_array) {
            for record in records {
                if let Some(violation) = carrier_record_violation(record, &entry.entry_id) {
                    return spoke_reject(SpokeRejectCode::InvalidInput, violation, None);
                }
            }
        }
    }

    let stored: Result<Option<StoredEntryRow>, sqlx::Error> = sqlx::query_as(
        "SELECT revision, created_at, superseded_by, payload FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(&entry.entry_id)
    .fetch_optional(&mut **tx)
    .await;
    let stored = match stored {
        Ok(stored) => stored,
        Err(error) => return internal(format!("stored revision could not be read: {error}")),
    };
    let (stored, stored_created_at, stored_payload) = match stored {
        Some((revision, created_at, superseded_by, payload)) => {
            if let Some(superseded_by) = superseded_by {
                let mut details = Map::new();
                details.insert("superseded_by".to_string(), Value::from(superseded_by));
                return spoke_reject(
                    SpokeRejectCode::InvalidInput,
                    format!("knowledge entry {} is superseded", entry.entry_id),
                    Some(details),
                );
            }
            (Some(revision as u64), Some(created_at), Some(payload))
        }
        None => (None, None, None),
    };
    let mut submitted_records: Vec<Value> = match entry.modules.get_mut(&module_key()) {
        Some(KnowledgeEntryModulesValue::Object(module)) => match module.get_mut("records") {
            Some(records) => match std::mem::replace(records, Value::Array(Vec::new())) {
                Value::Array(records) => records,
                _ => {
                    return spoke_reject(
                        SpokeRejectCode::InvalidInput,
                        format!(
                            "knowledge entry {} cognition records must be an array",
                            entry.entry_id
                        ),
                        None,
                    )
                }
            },
            None => Vec::new(),
        },
        _ => Vec::new(),
    };

    let next = match expected_base_revision {
        None => match stored {
            None => 1,
            Some(_) => {
                return spoke_reject(
                    SpokeRejectCode::KnowledgeEntryAlreadyExists,
                    format!("knowledge entry {} already exists", entry.entry_id),
                    None,
                )
            }
        },
        Some(expected) => match stored {
            Some(stored) if stored == expected => expected + 1,
            stored => return stale(&entry.entry_id, Some(expected), stored),
        },
    };
    let Ok(revision) = i64::try_from(next) else {
        return internal(format!(
            "knowledge entry {} revision is exhausted",
            entry.entry_id
        ));
    };

    // The stored payload this read already fetched carries the anchor the put must preserve, so
    // the gate adds no statement.
    let stored_entry = match stored_payload {
        Some(payload) => match decode(&payload) {
            SpokeResult::Ok(stored) => Some(stored),
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        },
        None => None,
    };
    if let SpokeResult::Reject(reject) =
        assert_adoption_anchor_preserved(&entry, stored_entry.as_ref())
    {
        return SpokeResult::Reject(reject);
    }

    let now = Utc::now();
    entry.revision = Some(next);
    if let Some(stored_created_at) = stored_created_at {
        let created_at = match DateTime::parse_from_rfc3339(&stored_created_at) {
            Ok(created_at) => created_at.with_timezone(&Utc),
            Err(error) => {
                return internal(format!(
                    "stored created_at of knowledge entry {} is not RFC 3339: {error}",
                    entry.entry_id
                ))
            }
        };
        entry.created_at = Some(created_at);
    }

    let committed = match stored {
        Some(_) => match read_records(&mut **tx, collection, &entry.entry_id).await {
            SpokeResult::Ok(records) => records,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        },
        None => Vec::new(),
    };
    if submitted_records.len() < committed.len() {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            format!(
                "knowledge entry {} drops {} committed cognition record(s)",
                entry.entry_id,
                committed.len() - submitted_records.len()
            ),
            None,
        );
    }
    for (index, (committed_record, submitted)) in
        committed.iter().zip(submitted_records.iter()).enumerate()
    {
        if committed_record != submitted {
            return spoke_reject(
                SpokeRejectCode::InvalidInput,
                format!(
                    "knowledge entry {} rewrites committed cognition record at position {index}",
                    entry.entry_id
                ),
                None,
            );
        }
    }

    let payload = match encode(&entry) {
        SpokeResult::Ok(payload) => payload,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let created_at = encode_timestamp(entry.created_at.unwrap_or(now));
    let updated_at = encode_timestamp(now);
    let head = (!submitted_records.is_empty()).then_some(submitted_records.len() as i64);
    let suffix = &submitted_records[committed.len()..];

    if stored.is_none() {
        let written = sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, cognition_head_seq, payload, created_at, updated_at)
             VALUES (?, ?, ?, NULL, ?, ?, ?)",
        )
        .bind(collection)
        .bind(&entry.entry_id)
        .bind(revision)
        .bind(&payload)
        .bind(&created_at)
        .bind(&updated_at)
        .execute(&mut **tx)
        .await;
        if let Err(error) = written {
            return internal(format!("knowledge entry could not be written: {error}"));
        }
    }
    for (offset, record) in suffix.iter().enumerate() {
        if let SpokeResult::Reject(reject) = insert_record(
            tx,
            collection,
            &entry.entry_id,
            committed.len() as i64 + offset as i64 + 1,
            record,
            None,
        )
        .await
        {
            return SpokeResult::Reject(reject);
        }
    }
    let written: Result<(), sqlx::Error> = match stored {
        Some(_) => sqlx::query(
            "UPDATE knowledge_entries \
             SET revision = ?, cognition_head_seq = ?, payload = ?, updated_at = ? \
             WHERE collection = ? AND entry_id = ?",
        )
        .bind(revision)
        .bind(head)
        .bind(&payload)
        .bind(&updated_at)
        .bind(collection)
        .bind(&entry.entry_id)
        .execute(&mut **tx)
        .await
        .map(|_| ()),
        None => match head {
            None => Ok(()),
            Some(head) => sqlx::query(
                "UPDATE knowledge_entries SET cognition_head_seq = ? \
                 WHERE collection = ? AND entry_id = ?",
            )
            .bind(head)
            .bind(collection)
            .bind(&entry.entry_id)
            .execute(&mut **tx)
            .await
            .map(|_| ()),
        },
    };
    if let Err(error) = written {
        return internal(format!("knowledge entry could not be written: {error}"));
    }
    if let Some(KnowledgeEntryModulesValue::Object(module)) = entry.modules.get_mut(&module_key()) {
        if let Some(records) = module.get_mut("records") {
            *records = Value::Array(std::mem::take(&mut submitted_records));
        }
    }
    SpokeResult::Ok(entry)
}

/// One carrier row as the append reads it: revision, head, payload as written and liveness marker.
type CarrierRow = (i64, Option<i64>, String, Option<String>);

/// Append one validated cognition record inside the caller's transaction: compare the
/// carrier revision without reading history, insert the record row at `head + 1` and
/// advance the head and the outer revision together. Returns the assigned revision.
///
/// A stale expected revision is the generic stale rejection — `commit_cognition` rechecks
/// the revision first and reports the retryable conflict. A duplicate scoped `record_id`
/// is invalid input, not a storage failure. Rollback of the caller's transaction leaves
/// every row and the revision unchanged.
pub async fn append_cognition(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
    expected_revision: u64,
    record: &Value,
) -> SpokeResult<u64> {
    let header: Result<Option<CarrierRow>, sqlx::Error> = sqlx::query_as(
        "SELECT revision, cognition_head_seq, payload, superseded_by FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(&mut **tx)
    .await;
    let (stored_revision, head_seq, payload, superseded_by) = match header {
        Ok(Some(header)) => header,
        Ok(None) => {
            return spoke_reject(
                SpokeRejectCode::KnowledgeEntryNotFound,
                format!("no knowledge entry {entry_id} in collection {collection}"),
                None,
            )
        }
        Err(error) => return internal(format!("carrier header could not be read: {error}")),
    };
    if superseded_by.is_some() {
        return invalid_input(format!("knowledge entry {entry_id} is superseded"));
    }
    let stored_revision = stored_revision as u64;
    if stored_revision != expected_revision {
        return stale(entry_id, Some(expected_revision), Some(stored_revision));
    }
    if let SpokeResult::Reject(reject) = validate_cognition_record(record) {
        return SpokeResult::Reject(reject);
    }
    if let Some(violation) = carrier_record_violation(record, entry_id) {
        return spoke_reject(SpokeRejectCode::InvalidInput, violation, None);
    }
    let append_seq = head_seq.map_or(1, |head| head + 1);
    let next = stored_revision + 1;
    let Ok(next_revision) = i64::try_from(next) else {
        return internal(format!("knowledge entry {entry_id} revision is exhausted"));
    };
    let mut entry = match decode(&payload) {
        SpokeResult::Ok(entry) => entry,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    if let SpokeResult::Reject(reject) = insert_record(
        tx,
        collection,
        entry_id,
        append_seq,
        record,
        Some(append_prior_shape(&entry)),
    )
    .await
    {
        return SpokeResult::Reject(reject);
    }

    entry.revision = Some(next);
    let module = entry.modules.entry(module_key()).or_insert_with(|| {
        KnowledgeEntryModulesValue::Object(Map::from_iter([
            ("records".to_string(), Value::Array(Vec::new())),
            ("world_facts".to_string(), Value::Array(Vec::new())),
            ("extensions".to_string(), Value::Object(Map::new())),
        ]))
    });
    if let KnowledgeEntryModulesValue::Object(module) = module {
        module.insert("records".to_string(), Value::Array(Vec::new()));
    }
    let payload = match encode(&entry) {
        SpokeResult::Ok(payload) => payload,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let written = sqlx::query(
        "UPDATE knowledge_entries \
         SET revision = ?, cognition_head_seq = ?, payload = ?, updated_at = ? \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(next_revision)
    .bind(append_seq)
    .bind(&payload)
    .bind(encode_timestamp(Utc::now()))
    .bind(collection)
    .bind(entry_id)
    .execute(&mut **tx)
    .await;
    if let Err(error) = written {
        return internal(format!("knowledge entry could not be written: {error}"));
    }
    SpokeResult::Ok(next)
}

#[async_trait]
impl KnowledgeEntryPort for ScopedKnowledgeStore {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => return internal(format!("knowledge entry could not be read: {error}")),
        };
        match read_entry_seeded(&mut tx, &self.scope.collection, entry_id, &self.seam).await {
            SpokeResult::Ok(entry) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(entry),
                Err(error) => internal(format!("knowledge entry could not be read: {error}")),
            },
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    async fn put_knowledge_entry(
        &self,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => return internal(format!("knowledge write could not begin: {error}")),
        };
        let written = write_entry(
            &mut transaction,
            &self.scope.collection,
            entry,
            expected_base_revision,
        )
        .await;
        let entry = match written {
            SpokeResult::Ok(entry) => entry,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if let Err(error) = transaction.commit().await {
            return internal(format!("knowledge write could not commit: {error}"));
        }
        SpokeResult::Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::time::Duration;

    use mindcore_base::cognition::CARRIER_NAMESPACE;
    use serde_json::{json, Value};
    use spoke_schemas::knowledge_entry::{KnowledgeEntryBody, KnowledgeEntryCanonicalName};
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    fn plain_carrier(entry_id: &str) -> KnowledgeEntry {
        KnowledgeEntry {
            adoption_anchor: None,
            body: KnowledgeEntryBody::default(),
            canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
                .expect("non-empty canonical name"),
            created_at: None,
            disclosure: None,
            entry_id: entry_id.to_string(),
            entry_type: "character".to_string(),
            extensions: HashMap::new(),
            modules: HashMap::new(),
            owner: None,
            revision: Some(1),
            schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
            source_anchor: None,
            status: "confirmed".to_string(),
            updated_at: None,
        }
    }

    fn cognition_carrier(entry_id: &str) -> KnowledgeEntry {
        let mut entry = plain_carrier(entry_id);
        entry.modules.insert(
            KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key"),
            KnowledgeEntryModulesValue::from(
                json!({"records": [], "world_facts": [], "extensions": {}})
                    .as_object()
                    .expect("module fixture is an object")
                    .clone(),
            ),
        );
        entry
    }

    fn belief(record_id: &str) -> Value {
        json!({
            "kind": "belief",
            "order": 1,
            "record_id": record_id,
            "holder": {
                "kind": "knowledge_entry",
                "id": "ke_wren",
                "namespace": CARRIER_NAMESPACE,
            },
            "target": {
                "carrier": {
                    "kind": "knowledge_entry",
                    "id": "ke_shared_north_gate",
                    "namespace": CARRIER_NAMESPACE,
                },
                "target_id": "prop_north_gate_closed",
                "kind": "proposition",
            },
            "assessor": {
                "kind": "knowledge_entry",
                "id": "ke_wren",
                "namespace": CARRIER_NAMESPACE,
            },
            "assessor_role": "holder_self_report",
            "basis": {
                "kind": "evidence",
                "evidence": [{
                    "kind": "memory_record",
                    "id": "mem_hearsay",
                    "namespace": CARRIER_NAMESPACE,
                }],
            },
            "assessment_context": {"context_id": "ctx_dusk_at_the_gate"},
            "effective_at": "2026-11-19T18:00:00Z",
            "assessed_at": "2026-11-19T18:05:00Z",
            "view": "current",
            "q": {
                "value": 0.4,
                "assertion": "belief_attribution",
                "method": "single_self_report",
                "calibration": "uncalibrated",
            },
            "extensions": {},
        })
    }

    fn module_of(entry: &KnowledgeEntry) -> Value {
        serde_json::to_value(&entry.modules).expect("modules serialize")["cognition"].clone()
    }

    fn record_ids(entry: &KnowledgeEntry) -> Vec<String> {
        let module = module_of(entry);
        module["records"]
            .as_array()
            .map(|records| {
                records
                    .iter()
                    .filter_map(|record| {
                        record
                            .get("record_id")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
        match result {
            SpokeResult::Ok(value) => value,
            SpokeResult::Reject(reject) => {
                panic!("{context}: unexpectedly rejected with {reject:?}")
            }
        }
    }

    async fn second_pool(path: &std::path::Path) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .foreign_keys(true),
            )
            .await
            .expect("open second connection")
    }

    async fn wait_for_seam(test_rx: &mut mpsc::UnboundedReceiver<&'static str>) {
        let signal = timeout(Duration::from_secs(10), test_rx.recv())
            .await
            .expect("projection did not reach the seam in time");
        assert_eq!(signal, Some("at_records_seam"));
    }

    /// Regression for the task-2 review Critical: the full-DTO port reads ran the
    /// carrier payload statement and the records statement as two autocommit
    /// queries, so an append committed between them paired an older carrier
    /// revision with records it never had — a DTO that never existed.
    ///
    /// Construction: the seam suspends each projection exactly between its two
    /// statements, and a second connection commits an append in that window. Every
    /// asserted DTO must be a state that actually existed: revision `r` of
    /// `ke_wren` pairs with exactly the `r - 1` records appended before it.
    #[tokio::test]
    async fn full_dto_reads_project_one_snapshot_across_a_concurrent_append() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("mindcore.db");
        let store = MindStore::open(&path).await.expect("open store");
        let port = store.knowledge(KnowledgeScope::new("wren"));
        let second = second_pool(&path).await;

        expect_ok(
            port.put_knowledge_entry(cognition_carrier("ke_wren"), None)
                .await,
            "create cognition carrier",
        );
        expect_ok(
            port.put_knowledge_entry(plain_carrier("ke_abel"), None)
                .await,
            "create module-less carrier",
        );

        let (hook_to_test, mut test_rx) = mpsc::unbounded_channel();
        let (test_to_hook, hook_rx) = mpsc::unbounded_channel();
        store.seam.install(hook_to_test, hook_rx);
        let read_port = store.knowledge(KnowledgeScope::new("wren"));
        let reader = tokio::spawn(async move { read_port.get_knowledge_entry("ke_wren").await });
        wait_for_seam(&mut test_rx).await;

        let mut tx = second
            .begin_with("BEGIN IMMEDIATE")
            .await
            .expect("begin append");
        let assigned = expect_ok(
            append_cognition(&mut tx, "wren", "ke_wren", 1, &belief("cog_hearsay")).await,
            "concurrent append",
        );
        assert_eq!(assigned, 2);
        tx.commit().await.expect("commit append");
        test_to_hook.send("appended").expect("reader alive");

        let dto = expect_ok(
            timeout(Duration::from_secs(10), reader)
                .await
                .expect("entry read did not finish in time")
                .expect("reader task"),
            "full DTO read",
        );
        assert_eq!(dto.revision, Some(1));
        assert!(
            record_ids(&dto).is_empty(),
            "revision 1's snapshot has no records, got {:?}",
            record_ids(&dto)
        );

        let fresh = expect_ok(port.get_knowledge_entry("ke_wren").await, "fresh read");
        assert_eq!(fresh.revision, Some(2));
        assert_eq!(record_ids(&fresh), vec!["cog_hearsay"]);

        let (hook_to_test, mut test_rx) = mpsc::unbounded_channel();
        let (test_to_hook, hook_rx) = mpsc::unbounded_channel();
        store.seam.install(hook_to_test, hook_rx);
        let list_port = store.knowledge(KnowledgeScope::new("wren"));
        let reader = tokio::spawn(async move { list_port.list_entries().await });
        wait_for_seam(&mut test_rx).await;

        let mut tx = second
            .begin_with("BEGIN IMMEDIATE")
            .await
            .expect("begin append");
        let assigned = expect_ok(
            append_cognition(&mut tx, "wren", "ke_wren", 2, &belief("cog_newer")).await,
            "second concurrent append",
        );
        assert_eq!(assigned, 3);
        tx.commit().await.expect("commit append");
        test_to_hook.send("appended").expect("reader alive");

        let listed = expect_ok(
            timeout(Duration::from_secs(10), reader)
                .await
                .expect("listing did not finish in time")
                .expect("reader task"),
            "listing",
        );
        assert_eq!(listed.len(), 2);
        let abel = listed
            .iter()
            .find(|entry| entry.entry_id == "ke_abel")
            .expect("abel listed");
        assert!(
            module_of(abel).get("records").is_none(),
            "absent module stays absent"
        );
        let wren = listed
            .iter()
            .find(|entry| entry.entry_id == "ke_wren")
            .expect("wren listed");
        assert_eq!(wren.revision, Some(2));
        assert_eq!(
            record_ids(wren),
            vec!["cog_hearsay"],
            "wren's records must come from its own snapshot"
        );

        let fresh = expect_ok(port.get_knowledge_entry("ke_wren").await, "fresh read");
        assert_eq!(fresh.revision, Some(3));
        assert_eq!(record_ids(&fresh), vec!["cog_hearsay", "cog_newer"]);

        second.close().await;
        store.close().await;
    }

    /// The seam belongs to one store rather than the process: a hydration running on
    /// another store can never consume it, so the interleave test always attaches to its
    /// own reader even when the test binary runs other reads concurrently.
    #[tokio::test]
    async fn the_snapshot_seam_belongs_to_its_own_store() {
        let temp = TempDir::new().expect("temp dir");
        let store = MindStore::open(&temp.path().join("seam.db"))
            .await
            .expect("open store");
        let other = MindStore::open(&temp.path().join("other.db"))
            .await
            .expect("open other store");
        expect_ok(
            store
                .knowledge(KnowledgeScope::new("wren"))
                .put_knowledge_entry(cognition_carrier("ke_wren"), None)
                .await,
            "create the seam store's carrier",
        );
        expect_ok(
            other
                .knowledge(KnowledgeScope::new("wren"))
                .put_knowledge_entry(cognition_carrier("ke_wren"), None)
                .await,
            "create the other store's carrier",
        );

        let (hook_to_test, mut test_rx) = mpsc::unbounded_channel();
        let (test_to_hook, hook_rx) = mpsc::unbounded_channel();
        store.seam.install(hook_to_test, hook_rx);

        let other_port = other.knowledge(KnowledgeScope::new("wren"));
        let dto = timeout(
            Duration::from_secs(10),
            other_port.get_knowledge_entry("ke_wren"),
        )
        .await
        .expect("another store's read suspended on this store's seam");
        expect_ok(dto, "other store read");

        let read_port = store.knowledge(KnowledgeScope::new("wren"));
        let reader = tokio::spawn(async move { read_port.get_knowledge_entry("ke_wren").await });
        wait_for_seam(&mut test_rx).await;
        test_to_hook.send("release").expect("reader alive");
        expect_ok(
            timeout(Duration::from_secs(10), reader)
                .await
                .expect("seam store read did not finish in time")
                .expect("reader task"),
            "seam store read",
        );

        other.close().await;
        store.close().await;
    }
}
