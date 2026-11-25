//! The Mind registry and its operations. A Mind is one memory space: its id is the space's
//! `space_id` and there is no second Mind id. `create` establishes the space, the cognition
//! carrier entry, the registry row and the creating principal's one default grant row in the
//! caller's transaction; a failure leaves no row. `list` and `get` read the registry back under
//! the caller's live grants. No operation archives or deletes a Mind: deactivation is the
//! space's frozen state.

use chrono::Utc;
use serde_json::{Map, Number, Value};
use sqlx::{Sqlite, Transaction};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{mint, MindId, SpaceId};
use mindcore_base::ir::{MemoryGrantRow, MemoryRepository, MemorySpaceRow};
use mindcore_base::knowledge::KnowledgeScope;
use spoke_operations::{spoke_reject, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{KnowledgeEntryBody, KnowledgeEntryCanonicalName};
use spoke_schemas::KnowledgeEntry;

use crate::knowledge::{internal, write_entry};
use crate::repository::SqliteMemoryRepository;
use crate::store::MindStore;

/// One Mind, as its registry row and its space row state it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mind {
    pub id: MindId,
    pub display_name: Option<String>,
    pub status: MindStatus,
    /// The collection the Mind's entries and carrier live in.
    pub knowledge: KnowledgeScope,
    /// The entry in `knowledge` that holds the Mind's current judgment.
    pub carrier_entry_id: String,
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MindStatus {
    Active,
    Frozen,
}

/// What `create` writes a Mind from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMind {
    /// The id the caller names; `None` lets the library assign one.
    pub mind_id: Option<MindId>,
    pub display_name: Option<String>,
    /// The principal that receives the default grant row.
    pub owner: CallerId,
}

/// The capabilities of the one grant row `create` writes, in the owner's order.
pub const DEFAULT_GRANT_CAPABILITIES: [&str; 7] = [
    "recall",
    "capture",
    "review",
    "records_write",
    "knowledge_read",
    "knowledge_write",
    "admin",
];

/// The carrier entry id inside every Mind's collection.
pub const CARRIER_ENTRY_ID: &str = "ke_carrier";

/// `details.reason` of the refusal a taken Mind id answers.
pub const MIND_ALREADY_EXISTS: &str = "MIND_ALREADY_EXISTS";

/// The collection a Mind's entries and carrier live in: `mind:` and the Mind's id.
pub fn knowledge_collection_of(id: &MindId) -> String {
    format!("mind:{id}")
}

/// The one answer a failed statement gives here, mirroring the knowledge face: a driver message
/// names local tables and columns.
fn storage_unavailable<T>() -> SpokeResult<T> {
    internal("mind storage is unavailable")
}

fn already_exists<T>(id: &MindId) -> SpokeResult<T> {
    let mut details = Map::new();
    details.insert("reason".to_string(), Value::from(MIND_ALREADY_EXISTS));
    spoke_reject(
        SpokeRejectCode::InvalidInput,
        format!("mind {id} already exists"),
        Some(details),
    )
}

fn invalid_input<T>(message: impl Into<String>) -> SpokeResult<T> {
    spoke_reject(SpokeRejectCode::InvalidInput, message, None)
}

/// The empty carrier `create` establishes: an ordinary entry of type `character` with no
/// modules, so it holds no records until the first append.
fn carrier_entry() -> KnowledgeEntry {
    KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody::default(),
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!(
            "{CARRIER_ENTRY_ID} carrier"
        ))
        .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: CARRIER_ENTRY_ID.to_string(),
        entry_type: "character".to_string(),
        extensions: std::collections::HashMap::new(),
        modules: std::collections::HashMap::new(),
        owner: None,
        revision: Some(1),
        schema_version: std::num::NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

/// The operation both entry points call. It borrows `tx`: the caller begins, commits and rolls
/// back. Writes, in order, inside the caller's transaction: the space row, the carrier entry,
/// the registry row and the creating principal's one default grant row. Every refusal returns
/// before the commit and the caller's rollback leaves no row.
pub async fn create(tx: &mut Transaction<'_, Sqlite>, request: &CreateMind) -> SpokeResult<Mind> {
    if let Some(id) = &request.mind_id {
        if id.as_str().is_empty() {
            return invalid_input("mind id must not be empty");
        }
    }
    if request.owner.as_str().is_empty() {
        return invalid_input("mind owner must not be empty");
    }
    let id = request
        .mind_id
        .clone()
        .unwrap_or_else(|| MindId::new(mint("spc")));
    let space = SpaceId::new(id.as_str());
    let created_at = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

    let repository = SqliteMemoryRepository;
    let space_row = MemorySpaceRow {
        space_id: space.clone(),
        display_name: request.display_name.clone(),
        authority_host_id: "mindcore".to_string(),
        authority_generation: 1,
        status: "active".to_string(),
        revision: Number::from(1),
        created_at: created_at.clone(),
        pending_receipts: 0,
    };
    if let Err(error) = repository.insert_space(tx, &space_row).await {
        return repo_error(error, &id);
    }

    let collection = knowledge_collection_of(&id);
    match write_entry(tx, &collection, carrier_entry(), None).await {
        SpokeResult::Ok(_) => {}
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    }

    let written = sqlx::query(
        "INSERT INTO minds (space_id, knowledge_collection, carrier_entry_id) VALUES (?, ?, ?)",
    )
    .bind(space.as_str())
    .bind(&collection)
    .bind(CARRIER_ENTRY_ID)
    .execute(&mut **tx)
    .await;
    if let Err(error) = written {
        return driver_error(error, &id);
    }

    let grant = MemoryGrantRow {
        grant_id: format!("grt_{id}"),
        space_id: space,
        subject_id: None,
        principal_id: request.owner.clone(),
        capabilities: DEFAULT_GRANT_CAPABILITIES
            .iter()
            .map(|capability| (*capability).to_string())
            .collect(),
        context_ids: Vec::new(),
        shared_access: false,
        expires_at: None,
        revoked_at: None,
        revision: Number::from(1),
        created_at: created_at.clone(),
        extensions: Map::new(),
    };
    if let Err(error) = repository.insert_grant(tx, &grant).await {
        return repo_error(error, &id);
    }

    SpokeResult::Ok(Mind {
        id,
        display_name: request.display_name.clone(),
        status: MindStatus::Active,
        knowledge: KnowledgeScope::new(collection),
        carrier_entry_id: CARRIER_ENTRY_ID.to_string(),
        created_at,
    })
}

/// A repository refusal of `create`: a colliding key is the taken-id refusal, anything else is
/// the fixed storage answer.
fn repo_error<T>(error: crate::repository::SqliteRepositoryError, id: &MindId) -> SpokeResult<T> {
    match error {
        crate::repository::SqliteRepositoryError::Conflict(_) => already_exists(id),
        _ => storage_unavailable(),
    }
}

/// A driver failure of the registry write: a unique violation is the taken-id refusal.
fn driver_error<T>(error: sqlx::Error, id: &MindId) -> SpokeResult<T> {
    match error {
        sqlx::Error::Database(error) if error.is_unique_violation() => already_exists(id),
        _ => storage_unavailable(),
    }
}

/// One row of the shared read: the registry row joined to its space row and one live-grant
/// candidate.
type JoinRow = (
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    Option<String>,
);

/// The read `list_minds` and `get_mind` share, driven from the principal's grant index. Rows
/// arrive grouped by Mind; a Mind is visible when any of its unrevoked grant rows is live — no
/// expiry, or an expiry that parses as an instant later than now. The expiry is compared as an
/// instant and never as text, so a `+08:00` offset is honoured. `id` narrows the read to one
/// Mind.
async fn read_visible(
    store: &MindStore,
    caller: &CallerId,
    id: Option<&MindId>,
) -> SpokeResult<Vec<Mind>> {
    let id = id.map(|id| id.as_str().to_string());
    let rows: Result<Vec<JoinRow>, sqlx::Error> = sqlx::query_as(
        "SELECT m.space_id, m.knowledge_collection, m.carrier_entry_id, \
         s.display_name, s.status, s.created_at, g.expires_at \
         FROM memory_grants g \
         JOIN minds m ON m.space_id = g.space_id \
         JOIN memory_spaces s ON s.space_id = g.space_id \
         WHERE g.principal_id = ?1 AND g.revoked_at IS NULL AND (?2 IS NULL OR g.space_id = ?2) \
         ORDER BY m.space_id, g.grant_id",
    )
    .bind(caller.as_str())
    .bind(id)
    .fetch_all(&store.pool)
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(_) => return storage_unavailable(),
    };
    let now = Utc::now();
    let mut minds = Vec::new();
    // Rows arrive grouped by Mind: the adjacent-group de-duplication keeps the first live row
    // of each group and skips the rest, and an expired row only hides its Mind when no later
    // row of the same group is live.
    let mut emitted: Option<String> = None;
    for (space_id, collection, carrier, display_name, status, created_at, expires_at) in rows {
        if emitted.as_deref() == Some(space_id.as_str()) {
            continue;
        }
        let live = match expires_at {
            None => true,
            Some(raw) => match chrono::DateTime::parse_from_rfc3339(&raw) {
                Ok(at) => at.with_timezone(&Utc) > now,
                Err(_) => false,
            },
        };
        if !live {
            continue;
        }
        let status = match status.as_str() {
            "active" => MindStatus::Active,
            "frozen" => MindStatus::Frozen,
            // Anything else is a row the schema's CHECK forbids: the strict decode refuses it.
            other => {
                return internal(format!(
                    "mind space {space_id} status {other} is not decodable"
                ))
            }
        };
        emitted = Some(space_id.clone());
        minds.push(Mind {
            id: MindId::new(space_id),
            display_name,
            status,
            knowledge: KnowledgeScope::new(collection),
            carrier_entry_id: carrier,
            created_at,
        });
    }
    SpokeResult::Ok(minds)
}

impl MindStore {
    /// `create` in one immediate-write transaction.
    pub async fn create_mind(&self, request: &CreateMind) -> SpokeResult<Mind> {
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(_) => return storage_unavailable(),
        };
        let created = create(&mut transaction, request).await;
        let mind = match created {
            SpokeResult::Ok(mind) => mind,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if transaction.commit().await.is_err() {
            return storage_unavailable();
        }
        SpokeResult::Ok(mind)
    }

    /// Every Mind whose space `caller` holds a live grant in, in `id` order.
    pub async fn list_minds(&self, caller: &CallerId) -> SpokeResult<Vec<Mind>> {
        read_visible(self, caller, None).await
    }

    /// One Mind `caller` may see; `NotFound` for any other id.
    pub async fn get_mind(&self, caller: &CallerId, id: &MindId) -> SpokeResult<Mind> {
        match read_visible(self, caller, Some(id)).await {
            SpokeResult::Ok(mut minds) => {
                if let Some(mind) = minds.pop() {
                    SpokeResult::Ok(mind)
                } else {
                    spoke_reject(
                        SpokeRejectCode::NotFound,
                        format!("mind {id} not found"),
                        None,
                    )
                }
            }
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }
}
