//! F1–F3: the `spoke-memory-reflection` family. An instance whose fixture
//! policy withholds Reflection refuses all three names before any storage read.

use chrono::Utc;
use serde_json::{json, Map, Value};
use spoke_operations::{
    validate_memory_reflections_generate, validate_memory_reflections_list,
    validate_memory_reflections_state, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{
    MemoryReflectionsGenerateRequest, MemoryReflectionsGenerateResponse,
    MemoryReflectionsListRequest, MemoryReflectionsListResponse, MemoryReflectionsStateRequest,
    MemoryReflectionsStateResponse, Principal,
};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{SpaceId, SubjectId};
use mindcore_base::ir::{
    MemoryReflectionRow, MemoryRepository, PlacementSelection, ReceiptLease, ReceiptRead,
    ReceiptState, RecordSelection, RecordState, ReviewReceiptIr,
};

use crate::admission::{self, Admission, Coverage};
use crate::content::read_selection;
use crate::policy;
use crate::project;
use crate::reject::{settle, unsettled, AResult, Reject};
use crate::wire;
use crate::{
    mint, principal_id, recorded_failure, MemoryAdapter, MemoryStage, ReflectionEffect,
    ReflectionWork, Tx,
};

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
    // contract §7.25 reflections.state

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
        raw: Value,
    ) -> AResult<MemoryStage> {
        if !self.reflections {
            return Err(Reject::unsupported("memory.reflections.state"));
        }
        wire::reject_cognition_filter(&raw)?;
        let request: MemoryReflectionsStateRequest =
            wire::decode(&raw, "memory.reflections.state request")?;
        validated(validate_memory_reflections_state(&request))?;
        let object = wire::object(&raw, "memory.reflections.state request")?;
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
        self.withhold_if_unsettled(tx, &space, subject_id.as_str())
            .await?;
        let basis = self
            .basis(
                tx,
                &space,
                subject_id.as_str(),
                &selection,
                filter.as_deref(),
            )
            .await?;
        let (profile_kinds, missing) = policy::profile_projection(&basis.profile_documents);
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

    // contract §7.26 reflections.list

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
        raw: Value,
    ) -> AResult<MemoryStage> {
        if !self.reflections {
            return Err(Reject::unsupported("memory.reflections.list"));
        }
        wire::reject_cognition_filter(&raw)?;
        let request: MemoryReflectionsListRequest =
            wire::decode(&raw, "memory.reflections.list request")?;
        validated(validate_memory_reflections_list(
            &request,
            policy::MAX_PAGE_SIZE_BOUND,
        ))?;
        let object = wire::object(&raw, "memory.reflections.list request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let subject_id = request.subject_id.to_string();
        // why: an absent selection lists subject-wide, which admission treats as the
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
        self.withhold_if_unsettled(tx, &space, subject_id.as_str())
            .await?;
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
        Self::ready::<MemoryReflectionsListResponse>(
            json!({ "reflections": bodies, "next_cursor": next_cursor }),
        )
    }

    // contract §7.27 reflections.generate

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
        raw: Value,
    ) -> AResult<MemoryStage> {
        if !self.reflections {
            return Err(Reject::unsupported("memory.reflections.generate"));
        }
        wire::reject_cognition_filter(&raw)?;
        let request: MemoryReflectionsGenerateRequest =
            wire::decode(&raw, "memory.reflections.generate request")?;
        validated(validate_memory_reflections_generate(&request))?;
        let object = wire::object(&raw, "memory.reflections.generate request")?;
        let principal_id = principal_id(principal);
        let subject_id = request.subject_id.to_string();
        let selection = wire::selection(object, "selection")?;
        let filter = wire::PlacementSelectionWire::filter(object, "filter")?;
        let selection_json = object
            .get("selection")
            .cloned()
            .ok_or_else(|| Reject::invalid("selection is required"))?;
        // why: a request `filter: null` stages as absent — the persisted row must not
        // carry a literal null the readers would later treat as malformed.
        let filter_json = object
            .get("filter")
            .filter(|value| !value.is_null())
            .cloned();
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
            &raw,
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
        // why: a probed receipt settles the key outright, except a pending row whose lease
        // already lapsed: that retry claims it below instead of answering uncertain
        // forever.
        let take_over = match admission::probe(&self.repository, tx, &op).await? {
            Some(receipt)
                if matches!(receipt.state, ReceiptState::Pending)
                    && self
                        .repository
                        .receipt_lease(tx, &op.space, receipt.receipt_id.as_str())
                        .await?
                        .is_some_and(|expires_at| expires_at < policy::timestamp(op.now)) =>
            {
                Some(receipt.receipt_id.clone())
            }
            Some(receipt) => {
                return Self::replay::<MemoryReflectionsGenerateResponse>(&receipt);
            }
            None => None,
        };
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
        let (_, missing) = policy::profile_projection(&basis.profile_documents);
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
        let basis_json = json!({
            "profile_revisions": basis.profile_revisions.iter().map(|(kind, revision)| (kind.clone(), json!(revision))).collect::<Map<String, Value>>(),
            "record_count": basis.records.len(),
            "source_set_digest": policy::reflection_source_set_digest(&basis.records)?,
            "state_digest": current,
        });
        // contract §7.0.3 — transaction 1 publishes the pending receipt and its lease; the
        // caller commits, composes, and completes in transaction 2. A retry that
        // found the abandoned pending row keeps its receipt id and re-leases it in
        // this same transaction, so the returned work's owner is the persisted one.
        let (receipt_id, owner_id) = match take_over {
            Some(receipt_id) => {
                let owner_id = mint("own");
                // why: the lease is stamped at claim time inside the repository, not at
                // `op.now` — the staging tx may sit on its basis work for longer
                // than the lease itself.
                let claimed = self
                    .repository
                    .claim_receipt_lease(
                        tx,
                        &op.space,
                        receipt_id.as_str(),
                        owner_id.as_str(),
                        GENERATION_LEASE_SECONDS,
                    )
                    .await?;
                if !claimed {
                    // why: the abandoned row raced — the loser follows the same
                    // replay/uncertainty contract as a live pending probe, never
                    // a fresh attempt or work item.
                    match admission::probe(&self.repository, tx, &op).await? {
                        Some(receipt) => {
                            return Self::replay::<MemoryReflectionsGenerateResponse>(&receipt)
                        }
                        None => {
                            return Err(Reject::uncertain(format!(
                                "receipt {receipt_id} is still pending"
                            )))
                        }
                    }
                }
                (receipt_id, owner_id)
            }
            None => {
                let receipt_id = mint("rct");
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
                (receipt_id, lease.owner_id)
            }
        };
        Ok(MemoryStage::ReflectionPending(ReflectionWork {
            space: op.space.clone(),
            receipt_id,
            owner_id,
            subject_id,
            selection: selection_json,
            filter: filter_json,
            basis: basis_json,
            engine_policy_version: policy::ENGINE_POLICY_VERSION,
        }))
    }

    // contract §7.27 generate completion (transaction 2)

    /// Complete a leased generation in a fresh caller transaction: insert
    /// the envelope and settle the pending receipt only while that receipt
    /// is still pending under the composed effect's owner — the owner a
    /// successful lease claim persisted (a lapsed lease alone no longer
    /// admits any owner). A live or re-leased row under another owner, or
    /// a settled row, rolls the envelope back with the settlement, and the
    /// completion is returned only after this transaction commits.
    pub async fn complete_reflection(
        &self,
        tx: &mut Tx<'_>,
        effect: ReflectionEffect,
    ) -> SpokeResult<Value> {
        settle(
            self.complete_reflection_body(tx, &effect)
                .await
                .map_err(unsettled),
        )
    }

    async fn complete_reflection_body(
        &self,
        tx: &mut Tx<'_>,
        effect: &ReflectionEffect,
    ) -> AResult<Value> {
        let receipt = match self
            .repository
            .receipt(tx, &effect.space, effect.receipt_id.as_str())
            .await?
        {
            None => {
                return Err(Reject::not_found(format!("receipt {}", effect.receipt_id)));
            }
            Some(ReceiptRead::Evicted { status, .. }) => {
                return Err(Reject::terminal(
                    SpokeRejectCode::ReceiptExpired,
                    format!("receipt {} body was reclaimed", effect.receipt_id),
                )
                .detail("last_known_status", json!(status)));
            }
            Some(ReceiptRead::Retained(receipt)) => receipt,
        };
        match &receipt.state {
            // why: an already-committed completion returns its stored result; only
            // the pending transition writes the envelope.
            ReceiptState::Committed { .. } => {
                return project::checked::<MemoryReflectionsGenerateResponse>(
                    project::receipt_body(&receipt),
                );
            }
            ReceiptState::Failed { error, .. } => {
                return Err(recorded_failure(error));
            }
            ReceiptState::Pending => {}
        }
        if receipt.operation != "memory.reflections.generate" {
            return Err(Reject::invalid(format!(
                "receipt {} belongs to {}",
                effect.receipt_id, receipt.operation
            )));
        }
        let settled_at = policy::timestamp(Utc::now());
        let reflection_id = mint("rfl");
        let (unit_kind, units) = effect.cost.clone();
        let envelope = MemoryReflectionRow {
            seq: 0,
            reflection_id: reflection_id.clone(),
            space_id: effect.space.clone(),
            subject_id: SubjectId::new(effect.subject_id.clone()),
            requester: CallerId::new(receipt.requester.clone()),
            selection: effect.selection.clone(),
            filter: effect.filter.clone(),
            cognition_filter: None,
            basis: effect.basis.clone(),
            engine_policy_version: effect.engine_policy_version.to_string(),
            model: None,
            schema_version: project::SCHEMA_VERSION as i64,
            revision: serde_json::Number::from(1),
            extensions: Map::new(),
            created_at: settled_at.clone(),
            body_text: effect.body_text.clone(),
            body_format: effect.body_format.clone(),
            cost: Some((unit_kind.clone(), units)),
            stale_cache: false,
        };
        self.repository.insert_reflection(tx, &envelope).await?;
        let settled = ReviewReceiptIr {
            state: ReceiptState::Committed {
                completed_at: settled_at,
                result: json!({
                    "reflection_id": reflection_id,
                    "cost": { "unit_kind": unit_kind, "units": units },
                }),
            },
            ..*receipt
        };
        // why: the CAS carries the lease identity exactly — only the owner the pending row is
        // leased to may settle — expiry no longer admits a bypass, so a takeover must have
        // persisted its own lease first. A re-settled or wrong-owner row refuses here and
        // the caller's rollback removes the envelope with it.
        if !self
            .repository
            .update_receipt(tx, &settled, "pending", Some(effect.owner_id.as_str()))
            .await?
        {
            return Err(Reject::uncertain(format!(
                "receipt {} is not pending under the composed owner",
                effect.receipt_id
            )));
        }
        project::checked::<MemoryReflectionsGenerateResponse>(project::receipt_body(&settled))
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
        let profiles = self
            .repository
            .list_profiles(tx, space, subject_id, &[])
            .await?;
        let mut profile_revisions = Vec::with_capacity(profiles.len());
        let mut profile_documents = Vec::with_capacity(profiles.len());
        for profile in profiles {
            profile_revisions.push((
                profile.profile_kind.clone(),
                profile.revision.as_i64().unwrap_or(0),
            ));
            profile_documents.push((
                profile.profile_kind,
                policy::profile_document_has_content(&profile.document),
            ));
        }
        profile_revisions.sort();
        Ok(Basis {
            records,
            profile_revisions,
            profile_documents,
        })
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
    profile_documents: Vec<(String, bool)>,
}
