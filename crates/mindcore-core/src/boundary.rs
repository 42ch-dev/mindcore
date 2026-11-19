//! The request scope supplied by the embedding adapter.

use crate::ids::{id_newtype, SpaceId};

id_newtype!(CallerId);

/// Supplied by the local adapter; not an authentication or admission mechanism and never persisted
/// as mental state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedScope {
    pub space: SpaceId,
    pub caller: CallerId,
}

impl TrustedScope {
    pub fn new(space: SpaceId, caller: CallerId) -> Self {
        Self { space, caller }
    }

    pub fn space(&self) -> &SpaceId {
        &self.space
    }

    pub fn caller(&self) -> &CallerId {
        &self.caller
    }
}
