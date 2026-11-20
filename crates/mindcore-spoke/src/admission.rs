//! Protocol admission: the single-grant coverage check (§6.5), the lifecycle
//! gates a fresh effect must pass (§7.5/§7.7/§8.2), and the scoped replay probe
//! (§7.0.5). Every check reads the caller's own transaction, so the row a write
//! will touch is the row authorization saw.

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{Sqlite, Transaction};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::SpaceId;
use mindcore_base::ir::{
    MemoryCandidateIr, MemoryGrantRow, MemoryRecordIr, MemoryRepository, MemorySpaceRow,
    MemorySubjectRow, Placement, ReceiptKey, ReceiptRead, ReviewReceiptIr,
};
use mindcore_sqlite::SqliteMemoryRepository;
use spoke_operations::SpokeResult;
use spoke_operations::{
    grant_covers, is_freeze_allowlisted, validate_grant_issuance, GrantRequirement, IssuedGrant,
};
use spoke_schemas::MemoryGrant;

use crate::project;
use crate::reject::{AResult, Reject};

pub(crate) type Tx<'a> = Transaction<'a, Sqlite>;

/// The placement an op writes or reads, as the request names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    Shared,
    Context(String),
}

impl Scope {
    /// Parse a `{visibility, context_id?}` placement from the original request
    /// subtree.
    pub(crate) fn from_json(value: &Value, field: &str) -> AResult<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| Reject::invalid(format!("{field} must be an object")))?;
        match object.get("visibility").and_then(Value::as_str) {
            Some("shared") => Ok(Self::Shared),
            Some("context") => match object.get("context_id").and_then(Value::as_str) {
                Some(context_id) if !context_id.is_empty() => {
                    Ok(Self::Context(context_id.to_string()))
                }
                _ => Err(Reject::invalid(format!("{field}.context_id is required"))),
            },
            _ => Err(Reject::invalid(format!("{field}.visibility is invalid"))),
        }
    }

    pub(crate) fn ir(&self) -> Placement {
        match self {
            Self::Shared => Placement::Shared,
            Self::Context(context_id) => Placement::Context(context_id.clone()),
        }
    }

    fn context_id(&self) -> Option<&str> {
        match self {
            Self::Shared => None,
            Self::Context(context_id) => Some(context_id.as_str()),
        }
    }

    pub(crate) fn requirement<'a>(
        &'a self,
        capability: &'a str,
        subject_id: &'a str,
    ) -> GrantRequirement<'a> {
        GrantRequirement {
            capability,
            subject_id: Some(subject_id),
            context_ids: self.context_id().map(str::to_string).into_iter().collect(),
            reads_shared: self.context_id().is_none(),
            subject_all: false,
        }
    }
}

impl From<&Placement> for Scope {
    fn from(placement: &Placement) -> Self {
        match placement {
            Placement::Shared => Self::Shared,
            Placement::Context(context_id) => Self::Context(context_id.clone()),
        }
    }
}

/// The per-op request facts every admission, probe and receipt stamp reads.
pub(crate) struct Op<'a> {
    pub(crate) space: SpaceId,
    pub(crate) requester: &'a str,
    pub(crate) operation: &'static str,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) payload_digest: String,
    pub(crate) now: DateTime<Utc>,
}

impl Op<'_> {
    fn receipt_key(&self) -> AResult<ReceiptKey> {
        let Some(key) = self.idempotency_key.clone() else {
            return Err(Reject::invalid("idempotency_key is required"));
        };
        Ok(ReceiptKey {
            space: self.space.clone(),
            requester: CallerId::new(self.requester),
            operation: self.operation.to_string(),
            idempotency_key: key,
        })
    }
}

/// What admits a mutating op (§6.5): requirements known from the request, or
/// requirements derived from the row the op names.
pub(crate) enum Coverage<'a> {
    Requirements(Vec<GrantRequirement<'a>>),
    Candidate {
        candidate_id: &'a str,
        capabilities: &'a [&'a str],
    },
    Record {
        record_id: &'a str,
        capabilities: &'a [&'a str],
        target: Option<&'a Scope>,
    },
}

pub(crate) struct Admission<'a> {
    pub(crate) operation: &'static str,
    pub(crate) space: &'a SpaceId,
    pub(crate) principal: &'a str,
    pub(crate) coverage: Coverage<'a>,
    pub(crate) issuance: Option<IssuedGrant<'a>>,
    pub(crate) subject_gate: Option<&'a str>,
    pub(crate) placement_gate: Option<&'a Scope>,
    pub(crate) freeze_gated: bool,
}

/// §7.24/S-3: the ops that write a subject's own content, and therefore need
/// the subject row to exist.
fn writes_subject_content(operation: &str) -> bool {
    matches!(
        operation,
        "memory.capture" | "memory.records.create" | "memory.profile.update"
    )
}

/// §6.12/§6.5: the coverage a whole read needs — every named context, shared
/// access when shared rows are in scope, unrestricted coverage for `subject_all`.
pub(crate) fn selection_requirement<'a>(
    capability: &'a str,
    subject_id: &'a str,
    selection: &mindcore_base::ir::PlacementSelection,
) -> GrantRequirement<'a> {
    use mindcore_base::ir::PlacementSelection;
    match selection {
        PlacementSelection::SubjectAll => GrantRequirement {
            capability,
            subject_id: Some(subject_id),
            context_ids: Vec::new(),
            reads_shared: false,
            subject_all: true,
        },
        PlacementSelection::SharedOnly => GrantRequirement {
            capability,
            subject_id: Some(subject_id),
            context_ids: Vec::new(),
            reads_shared: true,
            subject_all: false,
        },
        PlacementSelection::SelectedContexts {
            context_ids,
            include_shared,
        } => GrantRequirement {
            capability,
            subject_id: Some(subject_id),
            context_ids: context_ids.iter().map(ToString::to_string).collect(),
            reads_shared: *include_shared,
            subject_all: false,
        },
    }
}

/// The §6.7 coverage one candidate names: its own subject and capture placement.
pub(crate) fn candidate_requirement<'a>(
    capability: &'a str,
    subject_id: &'a str,
    placement: &'a mindcore_base::ir::Placement,
) -> GrantRequirement<'a> {
    let context_id = match placement {
        mindcore_base::ir::Placement::Shared => None,
        mindcore_base::ir::Placement::Context(context_id) => Some(context_id.as_str()),
    };
    GrantRequirement {
        capability,
        subject_id: Some(subject_id),
        context_ids: context_id.map(str::to_string).into_iter().collect(),
        reads_shared: context_id.is_none(),
        subject_all: false,
    }
}

// ── Rows and grants ────────────────────────────────────────────────────

pub(crate) async fn space(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    space: &SpaceId,
) -> AResult<MemorySpaceRow> {
    match repository.space(tx, space).await? {
        Some(row) => Ok(row),
        None => Err(Reject::not_found(format!("space {}", space.as_str()))),
    }
}

pub(crate) async fn subject(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    space: &SpaceId,
    subject_id: &str,
) -> AResult<Option<MemorySubjectRow>> {
    Ok(repository.subject(tx, space, subject_id).await?)
}

pub(crate) async fn candidate(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    space: &SpaceId,
    candidate_id: &str,
) -> AResult<Option<MemoryCandidateIr>> {
    Ok(repository.candidate(tx, space, candidate_id).await?)
}

pub(crate) async fn record(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    space: &SpaceId,
    record_id: &str,
) -> AResult<Option<MemoryRecordIr>> {
    Ok(repository.record(tx, space, record_id).await?)
}

pub(crate) async fn grants(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    space: &SpaceId,
) -> AResult<Vec<MemoryGrantRow>> {
    Ok(repository.list_grants(tx, space, None).await?)
}

/// The protocol's own coverage evaluator takes the wire grant union; the stored
/// row projects to it losslessly.
pub(crate) fn wire_grants(rows: &[MemoryGrantRow]) -> AResult<Vec<MemoryGrant>> {
    rows.iter()
        .map(|row| {
            serde_json::from_value(project::grant_body(row)).map_err(|error| {
                Reject::terminal(
                    spoke_operations::SpokeRejectCode::InternalError,
                    format!("stored grant projection: {error}"),
                )
            })
        })
        .collect()
}

pub(crate) fn covered(
    grants: &[MemoryGrant],
    principal: &str,
    space: &str,
    now: DateTime<Utc>,
    requirement: &GrantRequirement<'_>,
) -> bool {
    grants
        .iter()
        .any(|grant| grant_covers(grant, principal, space, now, requirement))
}

/// §6.5: does ONE grant satisfy every requirement? Requirements never union.
pub(crate) fn one_grant_covers(
    grants: &[MemoryGrant],
    principal: &str,
    space: &str,
    now: DateTime<Utc>,
    requirements: &[GrantRequirement<'_>],
) -> bool {
    grants.iter().any(|grant| {
        requirements
            .iter()
            .all(|requirement| grant_covers(grant, principal, space, now, requirement))
    })
}

/// Whether a stored expiry instant has passed. RFC 3339 offsets are honoured,
/// so a `+hh:mm` expiry is compared as an instant and never as text.
pub(crate) fn expiry_passed(expires_at: Option<&str>, now: DateTime<Utc>) -> bool {
    let Some(raw) = expires_at else {
        return false;
    };
    match chrono::DateTime::parse_from_rfc3339(raw) {
        Ok(at) => at.with_timezone(&Utc) <= now,
        // An unparseable stored expiry fails closed: it cannot be shown live.
        Err(_) => true,
    }
}

/// A grant is live when it is unrevoked and unexpired; current participation
/// (§7.28) is exactly this predicate, never revocation alone.
pub(crate) fn live_grant(row: &MemoryGrantRow, now: DateTime<Utc>) -> bool {
    row.revoked_at.is_none() && !expiry_passed(row.expires_at.as_deref(), now)
}

/// Does any live grant reach this subject at all (§7.6 "any capability")?
pub(crate) fn any_grant_for_subject(
    rows: &[MemoryGrantRow],
    principal: &str,
    space: &str,
    subject_id: &str,
    now: DateTime<Utc>,
) -> bool {
    rows.iter().any(|row| {
        row.principal_id.as_str() == principal
            && row.space_id.as_str() == space
            && row.revoked_at.is_none()
            && !expiry_passed(row.expires_at.as_deref(), now)
            && row
                .subject_id
                .as_ref()
                .is_none_or(|scoped| scoped.as_str() == subject_id)
    })
}

/// §7.23: the profile read needs any capability on the subject with shared
/// access or unrestricted coverage.
pub(crate) fn profile_visible(
    rows: &[MemoryGrantRow],
    principal: &str,
    space: &str,
    subject_id: &str,
    now: DateTime<Utc>,
) -> bool {
    rows.iter().any(|row| {
        row.principal_id.as_str() == principal
            && row.space_id.as_str() == space
            && row.revoked_at.is_none()
            && !expiry_passed(row.expires_at.as_deref(), now)
            && row
                .subject_id
                .as_ref()
                .is_none_or(|scoped| scoped.as_str() == subject_id)
            && (row.context_ids.is_empty() || row.shared_access)
    })
}

/// §7.7/MEM-R024: a `fenced`/`archived` subject rejects the write, and so does a
/// placement into any context it has fenced.
pub(crate) fn require_write_allowed(
    subject: &MemorySubjectRow,
    placements: &[&Scope],
) -> AResult<()> {
    if subject.status != "active" {
        return Err(Reject::terminal(
            spoke_operations::SpokeRejectCode::SubjectFenced,
            format!(
                "subject {} is {}",
                subject.subject_id.as_str(),
                subject.status
            ),
        ));
    }
    let fenced: Vec<&str> = subject
        .fenced_contexts
        .iter()
        .map(|context| context.as_str())
        .collect();
    for placement in placements {
        let Some(context_id) = placement.context_id() else {
            continue;
        };
        if fenced.contains(&context_id) {
            return Err(Reject::terminal(
                spoke_operations::SpokeRejectCode::SubjectFenced,
                format!(
                    "subject {} fenced context {context_id}",
                    subject.subject_id.as_str()
                ),
            ));
        }
    }
    Ok(())
}

// ── Admission, gate and probe ──────────────────────────────────────────

fn refuse_coverage(admission: &Admission<'_>, requirements: &[GrantRequirement<'_>]) -> Reject {
    let capability = requirements
        .first()
        .map_or("any", |requirement| requirement.capability);
    Reject::not_authorized(format!(
        "principal {} holds no grant covering {capability} in space {}",
        admission.principal,
        admission.space.as_str()
    ))
}

/// §6.5 admission of one op, evaluated inside the effect transaction.
pub(crate) async fn authorize(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    admission: &Admission<'_>,
    now: DateTime<Utc>,
) -> AResult<Vec<MemoryGrantRow>> {
    space(repository, tx, admission.space).await?;
    let rows = grants(repository, tx, admission.space).await?;
    let wire = wire_grants(&rows)?;
    let space_id = admission.space.as_str();
    match &admission.coverage {
        Coverage::Requirements(requirements) => {
            if !one_grant_covers(&wire, admission.principal, space_id, now, requirements) {
                return Err(refuse_coverage(admission, requirements));
            }
        }
        Coverage::Candidate {
            candidate_id,
            capabilities,
        } => {
            let Some(row) = candidate(repository, tx, admission.space, candidate_id).await? else {
                return Err(Reject::not_found(format!("candidate {candidate_id}")));
            };
            let subject = row.subject_id.to_string();
            let scope = Scope::from(&row.capture_scope);
            let requirements: Vec<GrantRequirement<'_>> = capabilities
                .iter()
                .map(|capability| scope.requirement(capability, subject.as_str()))
                .collect();
            if !one_grant_covers(&wire, admission.principal, space_id, now, &requirements) {
                // §7.13/C-102: incomplete coverage hides the row exactly as an
                // id this store never held.
                return Err(Reject::not_found(format!("candidate {candidate_id}")));
            }
        }
        Coverage::Record {
            record_id,
            capabilities,
            target,
        } => {
            let Some(row) = record(repository, tx, admission.space, record_id).await? else {
                return Err(Reject::not_found(format!("record {record_id}")));
            };
            let subject = row.subject_id.to_string();
            let scope = Scope::from(&row.scope);
            let mut requirements: Vec<GrantRequirement<'_>> = Vec::new();
            for capability in *capabilities {
                requirements.push(scope.requirement(capability, subject.as_str()));
                if let Some(target) = target {
                    requirements.push(target.requirement(capability, subject.as_str()));
                }
            }
            if !one_grant_covers(&wire, admission.principal, space_id, now, &requirements) {
                return Err(refuse_coverage(admission, &requirements));
            }
        }
    }
    Ok(rows)
}

/// §7.9 non-amplification over the grant set the coverage check already read.
pub(crate) fn authorize_issuance(
    rows: &[MemoryGrantRow],
    admission: &Admission<'_>,
    now: DateTime<Utc>,
) -> AResult<()> {
    let Some(issued) = admission.issuance.as_ref() else {
        return Ok(());
    };
    let wire = wire_grants(rows)?;
    match validate_grant_issuance(
        &wire,
        admission.principal,
        admission.space.as_str(),
        now,
        issued,
    ) {
        SpokeResult::Ok(()) => Ok(()),
        SpokeResult::Reject(reject) => Err(Reject::not_authorized(reject.message)),
    }
}

/// Lifecycle gates that apply only to a fresh effect, never to a replay.
pub(crate) async fn gate(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    admission: &Admission<'_>,
) -> AResult<()> {
    if admission.freeze_gated {
        let row = space(repository, tx, admission.space).await?;
        if row.status == "frozen" && !is_freeze_allowlisted(admission.operation) {
            return Err(Reject::terminal(
                spoke_operations::SpokeRejectCode::SpaceFrozen,
                format!(
                    "space {} is frozen; {} is outside the maintenance allowlist",
                    row.space_id.as_str(),
                    admission.operation
                ),
            ));
        }
    }
    if let Some(subject_id) = admission.subject_gate {
        match subject(repository, tx, admission.space, subject_id).await? {
            Some(row) => {
                let placements: Vec<&Scope> = admission.placement_gate.into_iter().collect();
                require_write_allowed(&row, &placements)?;
            }
            None if writes_subject_content(admission.operation) => {
                return Err(Reject::not_found(format!("subject {subject_id}")));
            }
            None => {}
        }
    }
    Ok(())
}

/// §7.0.5: the dedup-namespace replay probe. A different payload under the same
/// key is a conflict at any age; a body the retention sweep evicted is
/// `RECEIPT_EXPIRED` rather than re-executed.
pub(crate) async fn probe(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    op: &Op<'_>,
) -> AResult<Option<ReviewReceiptIr>> {
    if op.idempotency_key.is_none() {
        return Ok(None);
    }
    let key = op.receipt_key()?;
    match repository.review_receipt(tx, &key).await? {
        None => Ok(None),
        Some(ReceiptRead::Retained(receipt)) => {
            if receipt.payload_digest != op.payload_digest {
                return Err(Reject::terminal(
                    spoke_operations::SpokeRejectCode::IdempotencyConflict,
                    format!(
                        "idempotency_key {} was used for a different payload",
                        op.idempotency_key.as_deref().unwrap_or_default()
                    ),
                ));
            }
            Ok(Some(*receipt))
        }
        Some(ReceiptRead::Evicted {
            payload_digest,
            status,
            ..
        }) => {
            if payload_digest != op.payload_digest {
                return Err(Reject::terminal(
                    spoke_operations::SpokeRejectCode::IdempotencyConflict,
                    format!(
                        "idempotency_key {} was used for a different payload",
                        op.idempotency_key.as_deref().unwrap_or_default()
                    ),
                ));
            }
            Err(Reject::terminal(
                spoke_operations::SpokeRejectCode::ReceiptExpired,
                format!(
                    "receipt body for idempotency_key {} was reclaimed",
                    op.idempotency_key.as_deref().unwrap_or_default()
                ),
            )
            .detail("last_known_status", json!(status)))
        }
    }
}

// ── Receipts and requests ──────────────────────────────────────────────

/// One pending receipt row stamped for this effect.
pub(crate) fn pending_receipt(op: &Op<'_>, receipt_id: &str) -> ReviewReceiptIr {
    ReviewReceiptIr {
        receipt_id: receipt_id.to_string(),
        space_id: op.space.as_str().to_string(),
        requester: op.requester.to_string(),
        operation: op.operation.to_string(),
        idempotency_key: op.idempotency_key.clone(),
        payload_digest: op.payload_digest.clone(),
        created_at: crate::policy::timestamp(op.now),
        state: mindcore_base::ir::ReceiptState::Pending,
        extensions: serde_json::Map::new(),
    }
}

/// One committed receipt row carrying its result.
pub(crate) fn committed_receipt(op: &Op<'_>, receipt_id: &str, result: Value) -> ReviewReceiptIr {
    let mut receipt = pending_receipt(op, receipt_id);
    receipt.state = mindcore_base::ir::ReceiptState::Committed {
        completed_at: crate::policy::timestamp(op.now),
        result,
    };
    receipt
}
