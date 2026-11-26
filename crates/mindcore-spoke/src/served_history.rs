//! The one served history port a face installs, and the grant rule it shares with the knowledge
//! face.
//!
//! `HistoryPort` methods take the admitted [`Principal`], so history needs no `Scope.extensions`
//! binding: the grant to read is the caller's own, in the space the request scope names. The
//! history face grants no access the knowledge face does not (`serving-seam.md`): a replay needs
//! `knowledge_read`, the equivalent of a knowledge get, and a prune needs one grant covering both
//! `knowledge_write` and `knowledge_read`, the equivalent of a knowledge put whose anchor read
//! passes through `get`.
//!
//! An **authorization** refusal is the knowledge face's own fixed `NOT_AUTHORIZED` with no
//! `details`, byte for byte: a refusal for want of a capability is the same answer the equivalent
//! knowledge get or put gives, and it names no caller, capability or space. An **admission**
//! refusal is not identical across the two faces: Connect's knowledge branch replaces every
//! admission `Reject` with its own constant, while its history branch returns this callback's
//! `Reject` verbatim — the Mind admission's own message. Both carry `NOT_AUTHORIZED`, neither
//! carries `details`, and neither reaches a port.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::SqlitePool;

use mindcore_base::ids::SpaceId;
use spoke_operations::{HistoryPort, HistoryPruneResult, SpokeResult};
use spoke_schemas::{Principal, RevisionSnapshot, Scope};

use crate::served_knowledge::{authorize_knowledge, storage_unavailable};
use crate::{KNOWLEDGE_READ, KNOWLEDGE_WRITE};

/// The served history face's port: the caller's own live grant, then the wrapped port.
///
/// The wrapped port is the history port the face serves (`RoutedKnowledgeStore` today); it keeps
/// no grant rule of its own, so a face that installs it directly skips this guard, and no face
/// does. The grant is read on a read transaction of the port's own, and the wrapped call runs on
/// the store's next one — the recorded window of `I-000154`, as on the knowledge face.
///
/// The Mind id is the space id, so the request scope's `scope_id` names the space whose grants
/// decide the call.
pub struct MindHistoryPort {
    pool: SqlitePool,
    port: Arc<dyn HistoryPort + Send + Sync>,
}

impl MindHistoryPort {
    pub fn new(pool: SqlitePool, port: Arc<dyn HistoryPort + Send + Sync>) -> Self {
        Self { pool, port }
    }

    /// Everything before the wrapped port: the caller's own live grant covering every named
    /// capability, in the space the request scope names.
    async fn authorize(
        &self,
        principal: &Principal,
        scope: &Scope,
        capabilities: &[&str],
    ) -> SpokeResult<()> {
        let space = SpaceId::new(scope.scope_id.as_str());
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(_) => return storage_unavailable(),
        };
        match authorize_knowledge(
            &mut tx,
            crate::principal_id(principal),
            &space,
            capabilities,
        )
        .await
        {
            SpokeResult::Ok(()) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(()),
                Err(_) => storage_unavailable(),
            },
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }
}

#[async_trait]
impl HistoryPort for MindHistoryPort {
    async fn get_history_snapshot(
        &self,
        principal: &Principal,
        scope: &Scope,
        entity_kind: &str,
        entity_id: &str,
        revision: u64,
    ) -> SpokeResult<RevisionSnapshot> {
        match self.authorize(principal, scope, &[KNOWLEDGE_READ]).await {
            SpokeResult::Ok(()) => {}
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        self.port
            .get_history_snapshot(principal, scope, entity_kind, entity_id, revision)
            .await
    }

    async fn prune_history(
        &self,
        principal: &Principal,
        scope: &Scope,
        entity_kind: &str,
        entity_id: &str,
        keep: u64,
    ) -> SpokeResult<HistoryPruneResult> {
        match self
            .authorize(principal, scope, &[KNOWLEDGE_WRITE, KNOWLEDGE_READ])
            .await
        {
            SpokeResult::Ok(()) => {}
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
        self.port
            .prune_history(principal, scope, entity_kind, entity_id, keep)
            .await
    }
}
