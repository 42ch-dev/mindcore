//! The Mind registry and its operations. A Mind is one memory space: its id is the space's
//! `space_id` and there is no second Mind id. `create` establishes the space, the cognition
//! carrier entry, the registry row and the creating principal's one default grant row in the
//! caller's transaction; a failure leaves no row. `list` and `get` read the registry back under
//! the caller's live grants. No operation archives or deletes a Mind: deactivation is the
//! space's frozen state.

use std::future::poll_fn;

use chrono::Utc;
use serde_json::{Map, Number, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, Sqlite, Transaction};

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

/// `details.reason` of the refusal a Mind id the schema cannot hold answers.
pub const MIND_ID_INVALID: &str = "MIND_ID_INVALID";

/// `details.reason` of the refusal a populated target collection answers.
pub const MIND_COLLECTION_OCCUPIED: &str = "MIND_COLLECTION_OCCUPIED";
/// `details.reason` of the refusal a registered carrier's supersede answers.
pub const MIND_CARRIER_CANNOT_BE_SUPERSEDED: &str = "MIND_CARRIER_CANNOT_BE_SUPERSEDED";

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

fn invalid_input_with_reason<T>(message: impl Into<String>, reason: &str) -> SpokeResult<T> {
    let mut details = Map::new();
    details.insert("reason".to_string(), Value::from(reason));
    spoke_reject(SpokeRejectCode::InvalidInput, message, Some(details))
}

/// The refusal superseding a registered carrier answers; shared with the knowledge port, which
/// owns the supersede write.
pub(crate) fn carrier_cannot_be_superseded<T>(entry_id: &str) -> SpokeResult<T> {
    invalid_input_with_reason(
        format!("mind carrier {entry_id} cannot be superseded"),
        MIND_CARRIER_CANNOT_BE_SUPERSEDED,
    )
}

/// The empty carrier `create` establishes: an ordinary entry of type `character` with no
/// modules, so it holds no records until the first append.
fn carrier_entry(
) -> Result<KnowledgeEntry, spoke_schemas::data::knowledge_entry::error::ConversionError> {
    Ok(KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody::default(),
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!(
            "{CARRIER_ENTRY_ID} carrier"
        ))?,
        created_at: None,
        disclosure: None,
        entry_id: CARRIER_ENTRY_ID.to_string(),
        entry_type: "character".to_string(),
        extensions: std::collections::HashMap::new(),
        modules: std::collections::HashMap::new(),
        owner: None,
        revision: Some(1),
        // SAFETY: the wire baseline one is not zero.
        schema_version: unsafe { std::num::NonZeroU64::new_unchecked(1) },
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    })
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
        // `memory_spaces` carries no CHECK a create input can violate: the only schema refusal
        // the id can hit is the registry's length check, mapped in `driver_error`.
        return match error {
            crate::repository::SqliteRepositoryError::Conflict(_) => already_exists(&id),
            _ => storage_unavailable(),
        };
    }

    // A Mind's collection is its own: a caller-named id must never claim entries the Mind did
    // not create. A populated target collection is refused before the carrier is written; the
    // refusal is not the taken-id one, because no Mind of this id need exist.
    let collection = knowledge_collection_of(&id);
    let occupied =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM knowledge_entries WHERE collection = ?")
            .bind(&collection)
            .fetch_one(&mut **tx)
            .await;
    match occupied {
        Ok(0) => {}
        Ok(_) => {
            return invalid_input_with_reason(
                format!("mind {id} collection is occupied"),
                MIND_COLLECTION_OCCUPIED,
            )
        }
        Err(_) => return storage_unavailable(),
    }

    let carrier = match carrier_entry() {
        Ok(entry) => entry,
        Err(error) => return internal(format!("the carrier entry could not be built: {error}")),
    };
    match write_entry(tx, &collection, carrier, None).await {
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
        grant_id: mint("grt"),
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
    // A colliding grant id is not a taken Mind id: the id is minted independently, and its
    // failure means nothing about the Mind the caller asked for.
    if repository.insert_grant(tx, &grant).await.is_err() {
        return storage_unavailable();
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

/// A driver failure of the registry write: a unique violation is the taken-id refusal, a
/// violated length check on the id is invalid input, anything else is the fixed storage answer.
fn driver_error<T>(error: sqlx::Error, id: &MindId) -> SpokeResult<T> {
    match error {
        sqlx::Error::Database(error) if error.is_unique_violation() => already_exists(id),
        sqlx::Error::Database(error) if error.is_check_violation() => {
            invalid_input_with_reason(format!("mind id {id} is not storable"), MIND_ID_INVALID)
        }
        _ => storage_unavailable(),
    }
}

/// One row of the shared read: the registry row joined to its space row and one live-grant
/// candidate, with the two grant columns whose decode admission requires.
type JoinRow = (
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    Option<String>,
    String,
    String,
);

// The one projection and join both visibility reads share. A macro rather than a constant,
// because `concat!` joins literals only, and sqlx's safety lint accepts literals alone.
macro_rules! grant_columns {
    () => {
        "SELECT m.space_id, m.knowledge_collection, m.carrier_entry_id, \
         s.display_name, s.status, s.created_at, g.expires_at, g.capabilities_json, \
         g.context_ids_json \
         FROM memory_grants g \
         JOIN minds m ON m.space_id = g.space_id \
         JOIN memory_spaces s ON s.space_id = g.space_id"
    };
}

/// A joined row that admission would refuse because its grant columns do not decode admits
/// nothing, so the registry reads skip it instead of erroring or naming a Mind.
fn row_decodes(capabilities_json: &str, context_ids_json: &str) -> bool {
    crate::repository::decode_grant_columns(capabilities_json, context_ids_json).is_ok()
}

/// One joined row from a raw driver row, or `None` when any column does not decode.
///
/// `query_as` maps the whole row through SQLx's `try_map`, whose error ends the stream, so a good
/// row sorted after an undecodable one would never be seen. The registry reads take raw driver
/// rows and decode each one here: an undecodable row is invisible rather than fatal.
fn join_of(row: &SqliteRow) -> Option<JoinRow> {
    Some((
        row.try_get("space_id").ok()?,
        row.try_get("knowledge_collection").ok()?,
        row.try_get("carrier_entry_id").ok()?,
        row.try_get("display_name").ok()?,
        row.try_get("status").ok()?,
        row.try_get("created_at").ok()?,
        row.try_get("expires_at").ok()?,
        row.try_get("capabilities_json").ok()?,
        row.try_get("context_ids_json").ok()?,
    ))
}

/// One `visible_mind_ids` row from a raw driver row, or `None` when any column does not decode:
/// the id read omits an undecodable row and keeps serving the rest.
fn visible_id_of(row: &SqliteRow) -> Option<(String, Option<String>, String, String)> {
    Some((
        row.try_get("space_id").ok()?,
        row.try_get("expires_at").ok()?,
        row.try_get("capabilities_json").ok()?,
        row.try_get("context_ids_json").ok()?,
    ))
}

// The visibility rule of `expiry_passed` in `crates/mindcore-spoke/src/admission.rs`, restated
// here because `mindcore-sqlite` cannot depend on the adapter: no expiry is live; an expiry
// that does not parse as RFC 3339 is never live; the expiry is compared as an instant and never
// as text, so an offset is honoured and an expiry equal to now is already passed.
fn live(expires_at: &Option<String>) -> bool {
    // The instant is read here, at the moment the row is examined and never earlier: a grant
    // that expires while the single-connection pool makes the query wait is already passed.
    let now = Utc::now();
    match expires_at {
        None => true,
        Some(raw) => match chrono::DateTime::parse_from_rfc3339(raw) {
            Ok(at) => at.with_timezone(&Utc) > now,
            Err(_) => false,
        },
    }
}

/// One joined row becomes a Mind; the strict status decode refuses a row the schema's CHECK
/// forbids, and the fixed message names no stored value.
fn mind_of(
    space_id: String,
    collection: String,
    carrier: String,
    display_name: Option<String>,
    status: String,
    created_at: String,
) -> SpokeResult<Mind> {
    let status = match status.as_str() {
        "active" => MindStatus::Active,
        "frozen" => MindStatus::Frozen,
        _ => {
            return internal(format!(
                "the status of mind space {space_id} is not decodable"
            ))
        }
    };
    SpokeResult::Ok(Mind {
        id: MindId::new(space_id),
        display_name,
        status,
        knowledge: KnowledgeScope::new(collection),
        carrier_entry_id: carrier,
        created_at,
    })
}

/// The read `list_minds` runs, driven from the principal's grant index. Rows stream one at a
/// time — peak memory is the output plus the current row, never one copy per matching grant.
/// Rows arrive grouped by Mind: the adjacent-group de-duplication keeps the first live row of
/// each group and skips the rest, and an expired row only hides its Mind when no later row of
/// the same group is live. A row whose columns do not decode is invisible rather than fatal:
/// it is skipped and the read keeps going, so a good row sorted after it still lists the Mind.
async fn read_visible(store: &MindStore, caller: &CallerId) -> SpokeResult<Vec<Mind>> {
    let mut rows = sqlx::query(concat!(
        grant_columns!(),
        " WHERE g.principal_id = ?1 AND g.revoked_at IS NULL ORDER BY m.space_id, g.grant_id"
    ))
    .bind(caller.as_str())
    .fetch(&store.pool);
    let mut minds = Vec::new();
    let mut emitted: Option<String> = None;
    while let Some(row) = poll_fn(|cx| rows.as_mut().poll_next(cx)).await {
        let row = match row {
            Ok(row) => row,
            Err(_) => return storage_unavailable(),
        };
        let Some((
            space_id,
            collection,
            carrier,
            display_name,
            status,
            created_at,
            expires_at,
            capabilities_json,
            context_ids_json,
        )) = join_of(&row)
        else {
            continue;
        };
        if emitted.as_deref() == Some(space_id.as_str()) {
            continue;
        }
        if !live(&expires_at) {
            continue;
        }
        if !row_decodes(&capabilities_json, &context_ids_json) {
            continue;
        }
        let mind = match mind_of(
            space_id,
            collection,
            carrier,
            display_name,
            status,
            created_at,
        ) {
            SpokeResult::Ok(mind) => mind,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        emitted = Some(mind.id.as_str().to_string());
        minds.push(mind);
    }
    SpokeResult::Ok(minds)
}

/// The read `get_mind` runs: the point constraint on `(principal_id, space_id)` lets the grant
/// index seek directly, so a point read never scans unrelated grants. The visibility rule is the
/// one `read_visible` applies.
async fn read_one_visible(
    store: &MindStore,
    caller: &CallerId,
    id: &MindId,
) -> SpokeResult<Vec<Mind>> {
    let mut rows = sqlx::query(concat!(
        grant_columns!(),
        " WHERE g.principal_id = ?1 AND g.space_id = ?2 AND g.revoked_at IS NULL ORDER BY g.grant_id"
    ))
    .bind(caller.as_str())
    .bind(id.as_str())
    .fetch(&store.pool);
    while let Some(row) = poll_fn(|cx| rows.as_mut().poll_next(cx)).await {
        let row = match row {
            Ok(row) => row,
            Err(_) => return storage_unavailable(),
        };
        let Some((
            space_id,
            collection,
            carrier,
            display_name,
            status,
            created_at,
            expires_at,
            capabilities_json,
            context_ids_json,
        )) = join_of(&row)
        else {
            continue;
        };
        if !live(&expires_at) {
            continue;
        }
        if !row_decodes(&capabilities_json, &context_ids_json) {
            continue;
        }
        return match mind_of(
            space_id,
            collection,
            carrier,
            display_name,
            status,
            created_at,
        ) {
            SpokeResult::Ok(mind) => SpokeResult::Ok(vec![mind]),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        };
    }
    SpokeResult::Ok(Vec::new())
}

/// The Mind ids `caller` holds a live grant in, read through the caller's own
/// transaction so `memory.spaces.list` sees exactly the rows its effect
/// commits against. This is the registry's own join `read_visible` runs,
/// reduced to the id: one joined row per live grant, de-duplicated in `id`
/// order by the same adjacent-group rule. A frozen Mind's id is answered — the
/// space row carries its status — and a space with no registry row never
/// joins, so it is never in the answer.
pub async fn visible_mind_ids(
    tx: &mut Transaction<'_, Sqlite>,
    caller: &str,
) -> SpokeResult<Vec<String>> {
    let mut rows = sqlx::query(
        "SELECT m.space_id, g.expires_at, g.capabilities_json, g.context_ids_json \
         FROM memory_grants g \
         JOIN minds m ON m.space_id = g.space_id \
         WHERE g.principal_id = ?1 AND g.revoked_at IS NULL \
         ORDER BY m.space_id, g.grant_id",
    )
    .bind(caller)
    .fetch(&mut **tx);
    let mut ids = Vec::new();
    let mut emitted: Option<String> = None;
    while let Some(row) = poll_fn(|cx| rows.as_mut().poll_next(cx)).await {
        let row = match row {
            Ok(row) => row,
            Err(_) => return storage_unavailable(),
        };
        let Some((space_id, expires_at, capabilities_json, context_ids_json)) = visible_id_of(&row)
        else {
            continue;
        };
        if emitted.as_deref() == Some(space_id.as_str()) {
            continue;
        }
        if !live(&expires_at) {
            continue;
        }
        if !row_decodes(&capabilities_json, &context_ids_json) {
            continue;
        }
        emitted = Some(space_id.clone());
        ids.push(space_id);
    }
    SpokeResult::Ok(ids)
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
        read_visible(self, caller).await
    }

    /// One Mind `caller` may see; `NotFound` for any other id.
    pub async fn get_mind(&self, caller: &CallerId, id: &MindId) -> SpokeResult<Mind> {
        match read_one_visible(self, caller, id).await {
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
