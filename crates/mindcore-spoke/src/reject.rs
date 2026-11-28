//! The protocol refusal surface: every failure leaves the adapter as a
//! [`SpokeReject`] whose `details.retry_class` tells the caller whether the
//! transaction may be retried. Storage diagnostics never cross this boundary.

use serde_json::{Map, Value};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};

use mindcore_base::ir::wire::WireError;
use mindcore_sqlite::SqliteRepositoryError;

/// A refusal before the boundary projection.
pub(crate) struct Reject {
    pub(crate) code: SpokeRejectCode,
    pub(crate) message: String,
    pub(crate) details: Map<String, Value>,
}

pub(crate) type AResult<T> = Result<T, Reject>;

const RETRY_CLASS: &str = "retry_class";

impl Reject {
    /// A refusal that repeating the same request cannot change.
    pub(crate) fn terminal(code: SpokeRejectCode, message: impl Into<String>) -> Self {
        Self::with_class(code, message, "terminal")
    }

    /// A refusal the caller may retry because no effect can have committed.
    pub(crate) fn retryable(code: SpokeRejectCode, message: impl Into<String>) -> Self {
        Self::with_class(code, message, "retryable")
    }

    /// The caller must resolve the outcome before retrying (§14.2/§14.3).
    pub(crate) fn uncertain(message: impl Into<String>) -> Self {
        Self::with_class(SpokeRejectCode::OutcomeUncertain, message, "uncertain")
    }

    /// Malformed wire input, version or selector.
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::terminal(SpokeRejectCode::InvalidInput, message)
    }

    /// A pre-commit repository failure: the caller rolls the transaction back.
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::retryable(SpokeRejectCode::InternalError, message)
    }

    pub(crate) fn not_found(what: impl std::fmt::Display) -> Self {
        Self::terminal(SpokeRejectCode::NotFound, format!("{what} not found"))
    }

    /// Mind admission: one `NOT_AUTHORIZED` an unknown, a frozen-ungranted and
    /// an unadmitted Mind share, byte-equal in the three cases. It carries no
    /// details at all — not even `retry_class` — because the refusal must not
    /// disclose whether the named Mind exists, and its fixed shape is what pins
    /// that indistinguishability.
    pub(crate) fn admission_refusal() -> Self {
        Self {
            code: SpokeRejectCode::NotAuthorized,
            message: "the caller is not admitted to the named mind".to_string(),
            details: Map::new(),
        }
    }

    pub(crate) fn not_authorized(message: impl Into<String>) -> Self {
        Self::terminal(SpokeRejectCode::NotAuthorized, message)
    }

    /// §5.2/D-10: an authorized read met an unsettled journal intent for its
    /// subject. The closed vocabulary names no withholding, so it maps to a
    /// terminal `INTERNAL_ERROR` carrying only `reason` and `subject_id` —
    /// never the generic retryable path, and no receipt id, target or storage
    /// diagnostic.
    pub(crate) fn unreconciled(subject_id: &str) -> Self {
        Self::terminal(
            SpokeRejectCode::InternalError,
            "subject content is withheld until its durable intents settle",
        )
        .detail("reason", Value::String("UNRECONCILED".to_string()))
        .detail("subject_id", Value::String(subject_id.to_string()))
    }

    /// The space-level withholding of a subjectless deliberation: any durable
    /// intent in the space is unsettled, whichever subject it belongs to. The
    /// refusal names the space and no subject.
    pub(crate) fn unreconciled_space(space_id: &str) -> Self {
        Self::terminal(
            SpokeRejectCode::InternalError,
            "space content is withheld until its durable intents settle",
        )
        .detail("reason", Value::String("UNRECONCILED".to_string()))
        .detail("space_id", Value::String(space_id.to_string()))
    }

    /// An optional family or op this adapter build does not serve (§4.1).
    pub(crate) fn unsupported(op: &str) -> Self {
        Self::terminal(
            SpokeRejectCode::CapabilityPortMissing,
            format!("{op} is not served by this provider build"),
        )
        .detail("op", Value::String(op.to_string()))
    }

    pub(crate) fn detail(mut self, key: &str, value: Value) -> Self {
        self.details.insert(key.to_string(), value);
        self
    }

    pub(crate) fn into_spoke(self) -> SpokeReject {
        SpokeReject {
            code: self.code,
            message: self.message,
            details: if self.details.is_empty() {
                None
            } else {
                Some(self.details)
            },
        }
    }

    fn with_class(code: SpokeRejectCode, message: impl Into<String>, class: &str) -> Self {
        let mut details = Map::new();
        details.insert(RETRY_CLASS.to_string(), Value::String(class.to_string()));
        Self {
            code,
            message: message.into(),
            details,
        }
    }
}

impl From<SqliteRepositoryError> for Reject {
    fn from(error: SqliteRepositoryError) -> Self {
        if contended(&error) {
            return Self::internal("a concurrent writer refused this transaction before commit");
        }
        Self::internal("storage failure before commit")
    }
}

/// Whether a repository failure is SQLite write contention.
///
/// `SQLITE_BUSY` covers both the busy-handler timeout and the write refused
/// because another connection's commit invalidated this transaction's WAL
/// snapshot; the primary and extended codes are both accepted because the
/// driver reports whichever the connection enables.
fn contended(error: &SqliteRepositoryError) -> bool {
    matches!(
        error,
        SqliteRepositoryError::Sql(sqlx::Error::Database(database))
            if matches!(database.code().as_deref(), Some("5" | "517"))
    )
}

/// Reclassify a phase-2 reject whose effect phase already committed: a stored
/// publication or out-of-transaction composition is durable, so a pre-commit
/// storage failure leaves an unsettled receipt the caller must resolve — the
/// `OUTCOME_UNCERTAIN` boundary, not the retryable pre-commit assertion.
/// Anything already honest (terminal, uncertain) passes through.
pub(crate) fn unsettled(reject: Reject) -> Reject {
    if reject.code == SpokeRejectCode::InternalError {
        Reject::uncertain(reject.message)
    } else {
        reject
    }
}

impl From<WireError> for Reject {
    fn from(error: WireError) -> Self {
        Self::invalid(error.to_string())
    }
}

/// Whether `reject` is the one Mind-admission refusal [`Reject::admission_refusal`] answers.
///
/// The knowledge admission callback has to tell that refusal from a storage fault: Connect maps
/// every admission `Reject` to the same fixed deny, so a fault answered as a `Reject` would be
/// indistinguishable from an unadmitted caller. This is the predicate that separates them, held
/// beside the constructor so the two cannot drift apart — it compares the whole value, including
/// the absent details the refusal's indistinguishability depends on.
#[must_use]
pub fn is_mind_admission_refusal(reject: &SpokeReject) -> bool {
    let refusal = Reject::admission_refusal().into_spoke();
    reject == &refusal
}

/// Project one operation result onto the wire channel.
pub(crate) fn settle<T>(result: AResult<T>) -> SpokeResult<T> {
    match result {
        Ok(value) => spoke_operations::spoke_ok(value),
        Err(reject) => SpokeResult::Reject(reject.into_spoke()),
    }
}
