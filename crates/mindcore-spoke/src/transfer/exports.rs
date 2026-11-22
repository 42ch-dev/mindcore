//! §7.29/§7.30 outbound transfer: the committed export artifact written inside
//! the caller's transaction, and the paged read that serves it while its fixed
//! retention window is open.

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use spoke_operations::{GrantRequirement, SpokeRejectCode, SpokeResult};
use spoke_schemas::{
    MemoryExportsCreateRequest, MemoryExportsCreateResponse, MemoryExportsGetRequest,
    MemoryExportsGetResponse, Principal,
};

use mindcore_base::ids::{SpaceId, SubjectId};
use mindcore_base::ir::{MemoryRepository, PlacementSelection};
use mindcore_sqlite::{MemoryExportRow, SqliteMemoryRepository};

use super::{build, ExportSpec};
use crate::admission::{self, Admission, Coverage};
use crate::policy;
use crate::project;
use crate::reject::{settle, AResult, Reject};
use crate::wire;
use crate::{mint, principal_id, MemoryAdapter, MemoryStage, Tx};

const CREATE: &str = "memory.exports.create";
const GET: &str = "memory.exports.get";

/// The coverage one export names. A whole-space export has no subject and needs
/// a space-wide grant; a subject export needs the selection's own coverage.
fn export_requirement<'a>(
    capability: &'a str,
    subject: Option<&'a str>,
    selection: &PlacementSelection,
) -> GrantRequirement<'a> {
    match subject {
        Some(subject) => admission::selection_requirement(capability, subject, selection),
        None => GrantRequirement {
            capability,
            subject_id: None,
            context_ids: Vec::new(),
            reads_shared: false,
            subject_all: true,
        },
    }
}

/// The stored export's original selection; `None` is the scope's default.
fn stored_selection(row: &MemoryExportRow) -> AResult<PlacementSelection> {
    match &row.selection {
        Some(stored) => {
            let value: Value = serde_json::from_str(stored)
                .map_err(|_| Reject::internal("stored export selection is unreadable"))?;
            Ok(wire::PlacementSelectionWire::from_json(&value)?.ir())
        }
        None => Ok(PlacementSelection::SubjectAll),
    }
}

/// §7.30: the coverage paging this stored export requires. A tombstone-inclusive
/// export additionally needs `admin`.
fn read_requirements<'a>(
    row: &'a MemoryExportRow,
    selection: &'a PlacementSelection,
) -> Vec<GrantRequirement<'a>> {
    let subject = row.subject_id.as_deref();
    let mut requirements = vec![export_requirement("transfer", subject, selection)];
    if row.include_tombstoned {
        requirements.push(export_requirement("admin", subject, selection));
    }
    requirements
}

/// §7.30/§7.31: whether the caller may page this stored export at all. Staging
/// asks before it answers any staging fact about the export's chunks.
pub(super) async fn readable(
    repository: &SqliteMemoryRepository,
    tx: &mut Tx<'_>,
    row: &MemoryExportRow,
    principal: &str,
    now: DateTime<Utc>,
) -> AResult<bool> {
    let space = SpaceId::new(row.space_id.as_str());
    let selection = stored_selection(row)?;
    let requirements = read_requirements(row, &selection);
    let grants = admission::wire_grants(&repository.list_grants(tx, &space, None).await?)?;
    Ok(admission::one_grant_covers(
        &grants,
        principal,
        space.as_str(),
        now,
        &requirements,
    ))
}

impl MemoryAdapter {
    // contract §7.29 exports.create

    pub async fn create_export(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.create_export_body(tx, principal, request).await)
    }

    async fn create_export_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let _request: MemoryExportsCreateRequest =
            wire::decode(&raw, "memory.exports.create request")?;
        let object = wire::object(&raw, "memory.exports.create request")?;
        let principal_id = principal_id(principal);
        let space_id = wire::required(object, "space_id")?;
        let subject = wire::optional(object, "subject_id")?.map(SubjectId::new);
        let selection = match object.get("selection") {
            Some(_) => {
                let parsed = wire::selection(object, "selection")?;
                if parsed.mode == wire::SelectionMode::SelectedContexts
                    && parsed.context_ids.is_empty()
                {
                    return Err(Reject::invalid("selection.context_ids must not be empty"));
                }
                parsed.ir()
            }
            None => PlacementSelection::SubjectAll,
        };
        let selection_json = match object.get("selection") {
            Some(value) => Some(
                serde_json::to_string(value)
                    .map_err(|error| Reject::invalid(format!("selection: {error}")))?,
            ),
            None => None,
        };
        let flag = |key: &str| object.get(key).and_then(Value::as_bool).unwrap_or(false);
        let include_tombstoned = flag("include_tombstoned");
        let include_reflections = flag("include_reflections");
        let require_quiescent = flag("require_quiescent");
        let idempotency_key = wire::required(object, "idempotency_key")?;
        let op = Self::op(space_id, principal_id, CREATE, Some(idempotency_key), &raw)?;

        let subject_ref = subject.as_ref().map(SubjectId::as_str);
        let mut requirements = vec![export_requirement("transfer", subject_ref, &selection)];
        if include_tombstoned {
            requirements.push(export_requirement("admin", subject_ref, &selection));
        }
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(requirements),
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryExportsCreateResponse>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        if require_quiescent {
            let space = admission::space(&self.repository, tx, &op.space).await?;
            if space.status != "frozen" || space.pending_receipts != 0 {
                return Err(
                    Reject::invalid("a quiescent export requires a frozen, drained space")
                        .detail("reason", json!("space_not_drained")),
                );
            }
        }

        let spec = ExportSpec {
            export_id: mint("exp"),
            space_id: op.space.clone(),
            subject_id: subject,
            selection,
            include_tombstoned,
            include_reflections,
            created_at: op.now,
        };
        let artifact = build(&self.repository, tx, &spec).await?;
        let chunk_count = artifact.chunks.len() as u64;
        let total_bytes: u64 = artifact
            .chunks
            .iter()
            .map(|chunk| chunk["bytes"].as_u64().unwrap_or_default())
            .sum();
        let count = |field: &str| {
            artifact
                .bundle
                .get(field)
                .and_then(Value::as_array)
                .map_or(0, |items| items.len() as u64)
        };
        let bundle_digest = artifact.digest.clone();
        let result = json!({
            "bundle_digest": &bundle_digest,
            "chunk_count": chunk_count,
            "counts": {
                "candidates": count("candidates"),
                "profiles": count("profiles"),
                "receipt_summaries": count("receipt_summaries"),
                "records": count("records"),
                "reflections": count("reflections"),
                "subjects": count("subjects"),
            },
            "export_id": &spec.export_id,
            "total_bytes": total_bytes,
        });

        let retention = i64::try_from(policy::EXPORT_RETENTION_SECONDS)
            .map_err(|_| Reject::internal("export retention is out of range"))?;
        self.repository
            .insert_export(
                tx,
                &MemoryExportRow {
                    export_id: spec.export_id.clone(),
                    space_id: op.space.as_str().to_string(),
                    requester: principal_id.to_string(),
                    subject_id: spec.subject_id.as_ref().map(|id| id.as_str().to_string()),
                    selection: selection_json,
                    include_tombstoned,
                    bundle_digest,
                    chunk_count,
                    created_at: policy::timestamp(op.now),
                    expires_at: policy::timestamp(op.now + chrono::Duration::seconds(retention)),
                },
            )
            .await?;
        for (ordinal, chunk) in artifact.chunks.iter().enumerate() {
            self.repository
                .insert_export_chunk(tx, &spec.export_id, ordinal as u64, &chunk.to_string())
                .await?;
        }
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemoryExportsCreateResponse>(project::receipt_body(&receipt))
    }

    // contract §7.30 exports.get

    pub async fn get_export(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.get_export_body(tx, principal, request).await)
    }

    async fn get_export_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryExportsGetRequest = wire::decode(&raw, "memory.exports.get request")?;
        let principal_id = principal_id(principal);
        let export_id = request.export_id.to_string();
        let ordinal = request.chunk_ordinal;
        let space = request.space_id.to_string();
        let op = Self::op(&space, principal_id, GET, None, &raw)?;

        let Some(row) = self.repository.export(tx, &op.space, &export_id).await? else {
            return Err(Reject::not_found(format!("export {export_id}")));
        };
        let selection = stored_selection(&row)?;
        let requirements = read_requirements(&row, &selection);
        let admission = Admission {
            operation: GET,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(requirements),
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: false,
        };
        if let Err(reject) = admission::authorize(&self.repository, tx, &admission, op.now).await {
            // §7.30: a scope this caller cannot read is indistinguishable from an
            // export the store never held, so neither ordinal nor expiry leaks.
            return Err(match reject.code {
                SpokeRejectCode::NotAuthorized | SpokeRejectCode::NotFound => {
                    Reject::not_found(format!("export {export_id}"))
                }
                _ => reject,
            });
        }

        let expires = DateTime::parse_from_rfc3339(&row.expires_at)
            .map_err(|_| Reject::internal("stored export expiry is unreadable"))?
            .with_timezone(&Utc);
        if op.now >= expires {
            return Err(Reject::terminal(
                SpokeRejectCode::ExportExpired,
                format!("export {export_id} expired"),
            ));
        }
        if ordinal >= row.chunk_count {
            return Err(Reject::invalid(format!(
                "chunk_ordinal {ordinal} is outside the export's {} chunks",
                row.chunk_count
            )));
        }
        let payload = self
            .repository
            .export_chunk(tx, &export_id, ordinal)
            .await?
            .ok_or_else(|| Reject::internal("the export names a chunk this store does not hold"))?;
        let chunk: Value = serde_json::from_str(&payload)
            .map_err(|_| Reject::internal("stored export chunk is unreadable"))?;
        Self::ready::<MemoryExportsGetResponse>(json!({ "chunk": chunk }))
    }
}
