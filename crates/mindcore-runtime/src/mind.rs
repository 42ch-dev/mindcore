//! The Mind handle: the in-process facade over one Mind. `open` is one registry read —
//! `minds::get` under the caller's live grants — and the registry's own refusal answers as it
//! is. The handle keeps the row that read answers and nothing about grants: it caches no
//! grant and acts on no status from the row, so a frozen Mind opens and every later operation
//! re-reads the live state on its own transaction.

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::MindId;
use mindcore_sqlite::minds::Mind as MindRow;
use mindcore_sqlite::MindStore;
use spoke_operations::SpokeResult;
use sqlx::SqlitePool;

/// A caller's handle to one Mind: the pool, the caller and the row `minds::get` answered at
/// open.
///
/// The handle is not a grant — it confers no access beyond what the caller's live grants admit
/// on every operation — and its row is a snapshot: the status it carries is the one at open,
/// never re-read here.
pub struct Mind {
    // Read by the memory, knowledge and cognition operations of later tasks; `open` itself
    // never acts on them.
    #[allow(dead_code)]
    pool: SqlitePool,
    #[allow(dead_code)]
    caller: CallerId,
    row: MindRow,
}

impl Mind {
    /// Open the Mind `mind_id` for `caller`: one call to `minds::get` under the caller's live
    /// grants. A Mind the caller cannot see answers the registry's own `NotFound`; a frozen
    /// Mind opens.
    pub async fn open(store: &MindStore, caller: &CallerId, mind_id: &MindId) -> SpokeResult<Self> {
        match crate::minds::get(store, caller, mind_id).await {
            SpokeResult::Ok(row) => SpokeResult::Ok(Mind {
                pool: store.pool(),
                caller: caller.clone(),
                row,
            }),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    /// The entry in the Mind's collection that holds its current judgment, as the row named it
    /// at open.
    pub fn carrier_entry_id(&self) -> &str {
        &self.row.carrier_entry_id
    }

    /// The registry row `open` resolved, as it was at open.
    pub fn row(&self) -> &MindRow {
        &self.row
    }
}
