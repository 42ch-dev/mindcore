//! The one served knowledge port a face installs, and the grant check it and the later served
//! faces share.
//!
//! SPOKE's [`ScopedKnowledgePort`] carries no principal, so the store cannot know whose grant to
//! read. The admission callback that does know the peer binds the caller into the scope it
//! returns (`mindcore_sqlite::bind_caller`); this port reads that binding back and authorizes
//! every operation on the caller's own live grant in the Mind's space, through the adapter's own
//! evaluation ([`crate::admission`]), never a second copy of it. Grant evaluation belongs to the
//! protocol adapter, not to the storage crate (`2026-11-20-storage-ir-layering.md`), which is why
//! the guard lives here rather than inside `RoutedKnowledgeStore`.
//!
//! A scope with no binding, a caller with no live grant in the Mind's space and a caller whose
//! grant does not cover the operation answer one fixed `NOT_AUTHORIZED` with no `details`, in
//! Connect's own admission-deny words. No answer names the caller, the capability or the space.

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{Sqlite, SqlitePool, Transaction};

use mindcore_base::ids::SpaceId;
use mindcore_sqlite::{bound_caller, validate_admission_scope};
use spoke_operations::{
    spoke_reject, GrantRequirement, ScopedKnowledgePort, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{KnowledgeEntry, Scope};

use crate::admission;
use crate::{KNOWLEDGE_READ, KNOWLEDGE_WRITE};

/// The one answer every authorization failure of the served faces gives: Connect's own
/// admission-deny message, so a refusal for want of a capability is the same bytes as a refusal
/// for want of admission. It names no caller, capability or space, and carries no `details`.
fn refuse<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::NotAuthorized,
        "knowledge scope not authorized",
        None,
    )
}

/// The knowledge face's fixed storage text: a driver message names local tables and columns and
/// never reaches a peer.
fn storage_unavailable<T>() -> SpokeResult<T> {
    spoke_reject(
        SpokeRejectCode::InternalError,
        "knowledge storage is unavailable",
        None,
    )
}

/// A space-wide requirement: the op names no subject, no context and no shared read.
fn space_wide(capability: &str) -> GrantRequirement<'_> {
    GrantRequirement {
        capability,
        subject_id: None,
        context_ids: Vec::new(),
        reads_shared: false,
        subject_all: false,
    }
}

/// Require ONE live grant of `principal` in `space` to cover every named `capability`.
///
/// The grant rows are read through the transaction the caller passes, so the decision sees one
/// committed state of that principal's grants — the one that transaction's snapshot holds. It is
/// not one atomic step with any later write: whoever calls this and then writes opens the write on
/// its own transaction, so a revocation or an expiry that commits between the two does not stop
/// the already-authorized write (the recorded window is store issue `I-000154`, and callers whose
/// grant must share the write's snapshot pass the write's own transaction). Requirements never
/// union: a caller holding `knowledge_write` on one grant and `knowledge_read` on another is
/// refused. A failed statement answers the fixed storage text; a missing grant, and a grant that
/// does not cover an operation, answer the one fixed refusal.
pub async fn authorize_knowledge(
    tx: &mut Transaction<'_, Sqlite>,
    principal: &str,
    space: &SpaceId,
    capabilities: &[&str],
) -> SpokeResult<()> {
    let rows = match mindcore_sqlite::decodable_grants(tx, space, Some(principal)).await {
        Ok(rows) => rows,
        Err(_) => return storage_unavailable(),
    };
    let now = Utc::now();
    let grants = match admission::live_wire_grants(&rows, now) {
        Ok(grants) => grants,
        Err(_) => return storage_unavailable(),
    };
    let requirements: Vec<GrantRequirement<'_>> = capabilities
        .iter()
        .map(|capability| space_wide(capability))
        .collect();
    if admission::one_grant_covers(&grants, principal, space.as_str(), now, &requirements) {
        SpokeResult::Ok(())
    } else {
        refuse()
    }
}

/// The served knowledge face's port: the scope's shape, then the caller's live grant, then the
/// wrapped store.
///
/// The wrapped store is the storage port this face serves (`RoutedKnowledgeStore` today), and it
/// keeps no grant rule of its own; a face that installs it directly skips this guard, so no face
/// does. The port takes the pool for the grant read, which is its own transaction and never
/// shares a snapshot with the store call.
pub struct MindKnowledgePort {
    pool: SqlitePool,
    store: std::sync::Arc<dyn ScopedKnowledgePort + Send + Sync>,
}

impl MindKnowledgePort {
    pub fn new(
        pool: SqlitePool,
        store: std::sync::Arc<dyn ScopedKnowledgePort + Send + Sync>,
    ) -> Self {
        Self { pool, store }
    }

    /// Everything before the wrapped store: the admitted shape, the bound caller, and the
    /// caller's own live grant covering every named capability.
    ///
    /// The Mind id is the space id, so the scope's `scope_id` names the space whose grants decide
    /// the call.
    async fn authorize(&self, scope: &Scope, capabilities: &[&str]) -> SpokeResult<()> {
        match validate_admission_scope(scope) {
            SpokeResult::Ok(()) => {}
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        let Some(principal) = bound_caller(scope) else {
            return refuse();
        };
        let space = SpaceId::new(scope.scope_id.as_str());
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(_) => return storage_unavailable(),
        };
        match authorize_knowledge(&mut tx, principal, &space, capabilities).await {
            SpokeResult::Ok(()) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(()),
                Err(_) => storage_unavailable(),
            },
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }
}

#[async_trait]
impl ScopedKnowledgePort for MindKnowledgePort {
    async fn get_scoped_knowledge_entry(
        &self,
        scope: &Scope,
        entry_id: &str,
    ) -> SpokeResult<KnowledgeEntry> {
        match self.authorize(scope, &[KNOWLEDGE_READ]).await {
            SpokeResult::Ok(()) => {}
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        self.store.get_scoped_knowledge_entry(scope, entry_id).await
    }

    /// A put is authorized on both capabilities: Connect reads the entry through `get` before it
    /// writes, for the anchor check and the compare-and-swap, and that read passes this port's
    /// `get`. The cognition append is guarded here, not removed: a caller holding such a grant
    /// appends records exactly as before.
    async fn put_scoped_knowledge_entry(
        &self,
        scope: &Scope,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        match self
            .authorize(scope, &[KNOWLEDGE_WRITE, KNOWLEDGE_READ])
            .await
        {
            SpokeResult::Ok(()) => {}
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        self.store
            .put_scoped_knowledge_entry(scope, entry, expected_base_revision)
            .await
    }

    async fn list_scoped_knowledge_entries(
        &self,
        scope: &Scope,
    ) -> SpokeResult<Vec<KnowledgeEntry>> {
        match self.authorize(scope, &[KNOWLEDGE_READ]).await {
            SpokeResult::Ok(()) => {}
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        self.store.list_scoped_knowledge_entries(scope).await
    }
}
