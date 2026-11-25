//! The library entry points `create`, `list` and `get`: one forwarding call onto `MindStore`
//! each, and the record types the answers and requests carry. The entry points own no
//! transaction — `MindStore::create_mind` opens and commits the one `create` writes in; the
//! reads join the registry under the caller's live grants.

pub use mindcore_sqlite::minds::{CreateMind, Mind, MindStatus};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::MindId;
use mindcore_sqlite::MindStore;
use spoke_operations::SpokeResult;

/// Write a Mind in one immediate-write transaction: the space, the cognition carrier, the
/// registry row and the owner's default grant. A refusal leaves no row.
pub async fn create(store: &MindStore, request: &CreateMind) -> SpokeResult<Mind> {
    store.create_mind(request).await
}

/// Every Mind whose space `caller` holds a live grant in, in `id` order.
pub async fn list(store: &MindStore, caller: &CallerId) -> SpokeResult<Vec<Mind>> {
    store.list_minds(caller).await
}

/// One Mind `caller` may see; `NotFound` for any other id.
pub async fn get(store: &MindStore, caller: &CallerId, id: &MindId) -> SpokeResult<Mind> {
    store.get_mind(caller, id).await
}
