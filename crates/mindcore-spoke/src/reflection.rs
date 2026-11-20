//! F1–F3: the `spoke-memory-reflection` family. An instance whose fixture
//! policy withholds Reflection refuses all three names before any storage read.

use chrono::Utc;
use serde_json::{json, Map, Value};
use spoke_operations::{
    validate_memory_reflections_generate, validate_memory_reflections_list,
    validate_memory_reflections_state, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{
    MemoryReflectionsGenerateRequest, MemoryReflectionsListRequest, MemoryReflectionsListResponse,
    MemoryReflectionsStateRequest, MemoryReflectionsStateResponse, Principal,
};

use mindcore_base::ids::SpaceId;
use mindcore_base::ir::{
    MemoryReflectionRow, MemoryRepository, PlacementSelection, ReceiptLease, RecordSelection,
    RecordState,
};

use crate::admission::{self, Admission, Coverage};
use crate::content::read_selection;
use crate::policy;
use crate::project;
use crate::reject::{settle, AResult, Reject};
use crate::wire;
use crate::{mint, principal_id, MemoryAdapter, MemoryStage, ReflectionWork, Tx};

/// §7.0.3: how long a pending generation's lease stays live.
const GENERATION_LEASE_SECONDS: i64 = 30;

fn validated(result: SpokeResult<()>) -> AResult<()> {
    match result {
        SpokeResult::Ok(()) => Ok(()),
        SpokeResult::Reject(reject) => Err(Reject::terminal(reject.code, reject.message)),
    }
}

/// The pinned selection a reflection row carries.
fn pinned_selection(value: &Value) -> AResult<PlacementSelection> {
    match value.get("mode").and_then(Value::as_str) {
        Some("subject_all") => Ok(PlacementSelection::SubjectAll),
        Some("shared_only") => Ok(PlacementSelection::SharedOnly),
        Some("selected_contexts") => {
            let context_ids = match value.get("context_ids") {
                Some(Value::Array(items)) => items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|id| mindcore_base::ids::ContextId::new(id.to_string()))
                    .collect(),
                _ => Vec::new(),
            };
            Ok(PlacementSelection::SelectedContexts {
                context_ids,
                include_shared: value
                    .get("include_shared")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        }
        _ => Err(Reject::internal(
            "stored reflection selection is not a selection",
        )),
    }
}

fn pinned_filter(value: Option<&Value>) -> AResult<Option<Vec<String>>> {
    match value {
        None => Ok(None),
        Some(filter) => match filter.get("source_context_ids") {
            Some(Value::Array(items)) => Ok(Some(
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect(),
            )),
            _ => Err(Reject::internal(
                "stored reflection filter names no source_context_ids",
            )),
        },
    }
}

fn is_state_digest(raw: &str) -> bool {
    raw.len() == 64
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

impl MemoryAdapter {
    // ── §7.25 reflections.state ────────────────────────────────────────

    pub async fn get_reflection_state(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.reflection_state_body(tx, principal, request).await)
    }

    async fn reflection_state_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> AResult<MemoryStage> {
        if !self.reflections {
            return Err(Reject::unsupported("memory.reflections.state"));
        }
        let request: MemoryReflectionsStateRequest =
            wire::decode(&request, "memory.reflections.state request")?;
        validated(validate_memory_reflections_state(&request))?;
        let payload = wire::canonical(&request)?;
        let object = wire::object(&payload, "memory.reflections.state request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let subject_id = request.subject_id.to_string();
        let (selection, filter) = read_selection(object)?;
        let now = Utc::now();
        admission::space(&self.repository, tx, &space).await?;
        let rows = admission::grants(&self.repository, tx, &space).await?;
        let wire_grants = admission::wire_grants(&rows)?;
        let base = admission::selection_requirement("reflect", subject_id.as_str(), &selection);
        if !admission::covered(&wire_grants, principal_id, space.as_str(), now, &base) {
            return Err(Reject::not_authorized(format!(
                "principal {principal_id} holds no reflect grant covering the selection"
            )));
        }
        let basis = self
            .basis(
                tx,
                &space,
                subject_id.as_str(),
                &selection,
                filter.as_deref(),
            )
            .await?;
        let (profile_kinds, missing) = policy::profile_projection(&basis.profile_revisions);
        let state_digest = policy::reflection_state_digest(
            &basis.records,
            &basis.profile_revisions,
            &selection,
            filter.as_deref(),
        )?;
        let visible = self
            .visible_reflections(
                tx,
                &space,
                subject_id.as_str(),
                Some(&selection),
                filter.as_deref(),
                principal_id,
                &wire_grants,
                now,
            )
            .await?;
        let last = visible.first();
        let stale = last.is_some_and(|(_, stale)| *stale);
        Self::ready::<MemoryReflectionsStateResponse>(json!({
            "basis": {
                "profile_kinds": profile_kinds,
                "record_count": basis.records.len(),
            },
            "data_sufficiency": policy::data_sufficiency(basis.records.len()),
            "missing_required_profile_kinds": missing,
            "stale": stale,
            "state_digest": state_digest,
            "last_reflection_id": last.map(|(row, _)| row.reflection_id.clone()),
        }))
    }

    // ── §7.26 reflections.list ─────────────────────────────────────────

    pub async fn list_reflections(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.list_reflections_body(tx, principal, request).await)
    }

    async fn list_reflections_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> AResult<MemoryStage> {
        if !self.reflections {
            return Err(Reject::unsupported("memory.reflections.list"));
        }
        let request: MemoryReflectionsListRequest =
            wire::decode(&request, "memory.reflections.list request")?;
        validated(validate_memory_reflections_list(
            &request,
            policy::MAX_PAGE_SIZE_BOUND,
        ))?;
        let payload = wire::canonical(&request)?;
        let object = wire::object(&payload, "memory.reflections.list request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let subject_id = request.subject_id.to_string();
        // An absent selection lists subject-wide, which admission treats as the
        // unrestricted selection rather than as no requirement.
        let requested = match object.get("selection") {
            None | Some(Value::Null) => PlacementSelection::SubjectAll,
            Some(_) => wire::selection(object, "selection")?.ir(),
        };
        let filter = wire::PlacementSelectionWire::filter(object, "filter")?;
        let now = Utc::now();
        admission::space(&self.repository, tx, &space).await?;
        let rows = admission::grants(&self.repository, tx, &space).await?;
        let wire_grants = admission::wire_grants(&rows)?;
        let base = admission::selection_requirement("reflect", subject_id.as_str(), &requested);
        if !admission::covered(&wire_grants, principal_id, space.as_str(), now, &base) {
            return Err(Reject::not_authorized(format!(
                "principal {principal_id} holds no reflect grant covering the selection"
            )));
        }
        let visible = self
            .visible_reflections(
                tx,
                &space,
                subject_id.as_str(),
                Some(&requested),
                filter.as_deref(),
                principal_id,
                &wire_grants,
                now,
            )
            .await?;
        let subject = admission::subject(&self.repository, tx, &space, subject_id.as_str()).await?;
        let subject_status = subject.as_ref().map(|row| row.status.clone());
        let profile_revisions = self
            .profile_revisions(tx, &space, subject_id.as_str())
            .await?;
        let all_records = self
            .all_record_pairs(tx, &space, subject_id.as_str())
            .await?;
        let marker = policy::snapshot_marker(
            visible
                .iter()
                .map(|(row, _)| {
                    (
                        row.reflection_id.clone(),
                        row.revision.as_i64().unwrap_or(0),
                    )
                })
                .chain(all_records)
                .collect(),
            &rows
                .iter()
                .map(|row| (row.grant_id.clone(), row.revision.as_i64().unwrap_or(0)))
                .collect::<Vec<_>>(),
            subject_status.as_deref(),
        )?;
        let scope = policy::cursor_scope(
            "memory.reflections.list",
            Some(space.as_str()),
            Some(subject_id.as_str()),
            None,
        );
        let cursor = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_string());
        let (offset, claimed) = policy::cursor_page(cursor.as_deref(), principal_id, &scope)?;
        let (page, next) = policy::page(visible, policy::page_limit(request.limit), offset);
        let bodies: Vec<Value> = page
            .iter()
            .map(|(row, stale)| project::reflection_body(row, *stale))
            .collect();
        let next_cursor =
            next.map(|offset| policy::encode_cursor(principal_id, &scope, offset, &marker));
        policy::verify_cursor(claimed.as_deref(), &marker)?;
        let profile_revisions = profile_revisions.len();
        let _ = profile_revisions;
        Self::ready::<MemoryReflectionsListResponse>(
            json!({ "reflections": bodies, "next_cursor": next_cursor }),
        )
    }

    // ── §7.27 reflections.generate ─────────────────────────────────────

    pub async fn generate_reflection(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.generate_body(tx, principal, request).await)
    }

    async fn generate_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> AResult<MemoryStage> {
        if !self.reflections {
            return Err(Reject::unsupported("memory.reflections.generate"));
        }
        let request: MemoryReflectionsGenerateRequest =
            wire::decode(&request, "memory.reflections.generate request")?;
        validated(validate_memory_reflections_generate(&request))?;
        let payload = wire::canonical(&request)?;
        let object = wire::object(&payload, "memory.reflections.generate request")?;
        let principal_id = principal_id(principal);
        let subject_id = request.subject_id.to_string();
        let selection = wire::selection(object, "selection")?;
        let filter = wire::PlacementSelectionWire::filter(object, "filter")?;
        let selection_json = object
            .get("selection")
            .cloned()
            .ok_or_else(|| Reject::invalid("selection is required"))?;
        let filter_json = object.get("filter").cloned();
        let expected_state_digest = wire::required(object, "expected_state_digest")?.to_string();
        if !is_state_digest(expected_state_digest.as_str()) {
            return Err(Reject::invalid(
                "expected_state_digest must be the 64 lowercase hex digest the state op published",
            ));
        }
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.reflections.generate",
            Some(request.idempotency_key.as_str()),
            &payload,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(vec![admission::selection_requirement(
                "reflect",
                subject_id.as_str(),
                &selection.ir(),
            )]),
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let basis = self
            .basis(
                tx,
                &op.space,
                subject_id.as_str(),
                &selection.ir(),
                filter.as_deref(),
            )
            .await?;
        let current = policy::reflection_state_digest(
            &basis.records,
            &basis.profile_revisions,
            &selection.ir(),
            filter.as_deref(),
        )?;
        if current != expected_state_digest {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                "the authorized basis changed since the state digest was published",
            )
            .detail("current", json!(current)));
        }
        let (_, missing) = policy::profile_projection(&basis.profile_revisions);
        if !missing.is_empty() {
            return Err(Reject::terminal(
                SpokeRejectCode::ProfileIncomplete,
                format!("subject {subject_id} misses required profile kinds"),
            )
            .detail("missing", json!(missing)));
        }
        if policy::data_sufficiency(basis.records.len()) == "insufficient" {
            return Err(Reject::terminal(
                SpokeRejectCode::InsufficientData,
                format!("subject {subject_id} has no authorized basis to generate from"),
            ));
        }
        let receipt_id = mint("rct");
        let basis_json = json!({
            "profile_revisions": basis.profile_revisions.iter().map(|(kind, revision)| (kind.clone(), json!(revision))).collect::<Map<String, Value>>(),
            "record_count": basis.records.len(),
            "source_set_digest": policy::reflection_source_set_digest(&basis.records)?,
            "state_digest": current,
        });
        // §7.0.3: transaction 1 publishes the pending receipt and its lease; the
        // caller commits, composes, and completes in transaction 2.
        let lease = ReceiptLease {
            owner_id: mint("own"),
            expires_at: policy::timestamp(
                op.now + chrono::Duration::seconds(GENERATION_LEASE_SECONDS),
            ),
        };
        let receipt = admission::pending_receipt(&op, receipt_id.as_str());
        self.repository
            .insert_receipt(tx, &receipt, Some(&lease))
            .await?;
        Ok(MemoryStage::ReflectionPending(ReflectionWork {
            space: op.space.clone(),
            receipt_id,
            owner_id: lease.owner_id,
            subject_id,
            selection: selection_json,
            filter: filter_json,
            basis: basis_json,
            engine_policy_version: policy::ENGINE_POLICY_VERSION,
        }))
    }
}

impl MemoryAdapter {
    /// The current authorized basis: live record revisions and profile
    /// revisions for one pinned selection and provenance filter.
    async fn basis(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
        subject_id: &str,
        selection: &PlacementSelection,
        filter: Option<&[String]>,
    ) -> AResult<Basis> {
        let selection = RecordSelection {
            placement: selection.clone(),
            provenance_filter: filter.map(<[String]>::to_vec),
            kinds: Vec::new(),
            include_tombstoned: true,
        };
        let mut records: Vec<(String, i64)> = self
            .repository
            .list_records(tx, space, subject_id, &selection)
            .await?
            .into_iter()
            .filter(|record| matches!(record.state, RecordState::Live { .. }))
            .map(|record| (record.record_id, record.revision.as_i64().unwrap_or(0)))
            .collect();
        records.sort();
        Ok(Basis {
            records,
            profile_revisions: self.profile_revisions(tx, space, subject_id).await?,
        })
    }

    async fn profile_revisions(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
        subject_id: &str,
    ) -> AResult<Vec<(String, i64)>> {
        let mut revisions: Vec<(String, i64)> = self
            .repository
            .list_profiles(tx, space, subject_id, &[])
            .await?
            .into_iter()
            .map(|profile| (profile.profile_kind, profile.revision.as_i64().unwrap_or(0)))
            .collect();
        revisions.sort();
        Ok(revisions)
    }

    /// Every record identity of the subject, live and tombstoned, for the
    /// cursor's invalidation marker.
    async fn all_record_pairs(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
        subject_id: &str,
    ) -> AResult<Vec<(String, i64)>> {
        let selection = RecordSelection {
            placement: PlacementSelection::SubjectAll,
            provenance_filter: None,
            kinds: Vec::new(),
            include_tombstoned: true,
        };
        Ok(self
            .repository
            .list_records(tx, space, subject_id, &selection)
            .await?
            .into_iter()
            .map(|record| (record.record_id, record.revision.as_i64().unwrap_or(0)))
            .collect())
    }

    /// §7.26 visibility narrowing plus per-row staleness recomputed at read
    /// time, newest first.
    #[allow(clippy::too_many_arguments)]
    async fn visible_reflections(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
        subject_id: &str,
        requested: Option<&PlacementSelection>,
        requested_filter: Option<&[String]>,
        principal: &str,
        wire_grants: &[spoke_schemas::MemoryGrant],
        now: chrono::DateTime<Utc>,
    ) -> AResult<Vec<(MemoryReflectionRow, bool)>> {
        let mut visible = Vec::new();
        for row in self
            .repository
            .list_reflections(tx, space, subject_id, requested)
            .await?
        {
            let pinned = pinned_selection(&row.selection)?;
            let filter = pinned_filter(row.filter.as_ref())?;
            if requested_filter.is_some_and(|requested| {
                Some(crate::policy::canonical_ids(requested))
                    != filter
                        .as_ref()
                        .map(|pinned| crate::policy::canonical_ids(pinned))
            }) {
                continue;
            }
            if !admission::covered(
                wire_grants,
                principal,
                space.as_str(),
                now,
                &admission::selection_requirement("reflect", subject_id, &pinned),
            ) {
                continue;
            }
            let basis = self
                .basis(tx, space, subject_id, &pinned, filter.as_deref())
                .await?;
            let current = policy::reflection_state_digest(
                &basis.records,
                &basis.profile_revisions,
                &pinned,
                filter.as_deref(),
            )?;
            let pinned_state = row
                .basis
                .get("state_digest")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            visible.push((row, current != pinned_state));
        }
        Ok(visible)
    }
}

struct Basis {
    records: Vec<(String, i64)>,
    profile_revisions: Vec<(String, i64)>,
}
