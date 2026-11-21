//! The scoped recall selection the borrowed-transaction repository boundary takes.

use crate::ids::{ContextId, SpaceId, SubjectId};

/// Selects live records by placement; `context: None` recalls shared rows and every context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecallQuery {
    pub space: SpaceId,
    pub subject: SubjectId,
    pub context: Option<ContextId>,
}
