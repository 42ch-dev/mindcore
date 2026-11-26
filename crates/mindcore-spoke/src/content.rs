//! D1–D15 and Y04: captures, candidates, retained records, recall, profiles
//! and the receipt channel.
//!
//! Mutations that project a file (record, consumed candidate, profile) stage
//! their effect, pending receipt and durable intent in the caller's transaction
//! and report [`MemoryStage::JournalPending`]; the caller commits, publishes and
//! then settles. Captures own no projection, so they commit their receipt here.

use chrono::Utc;
use serde_json::{json, Map, Number, Value};
use spoke_operations::{
    validate_memory_candidates_list, validate_memory_candidates_review, validate_memory_capture,
    validate_memory_recall, validate_memory_records_create, validate_memory_records_list,
    validate_memory_records_update, GrantRequirement, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{
    MemoryCandidatesDismissRequest, MemoryCandidatesDismissResponse, MemoryCandidatesGetRequest,
    MemoryCandidatesGetResponse, MemoryCandidatesListRequest, MemoryCandidatesListResponse,
    MemoryCandidatesReviewRequest, MemoryCandidatesReviewResponse, MemoryCaptureRequest,
    MemoryCaptureResponse, MemoryProfileGetRequest, MemoryProfileGetResponse,
    MemoryProfileUpdateRequest, MemoryProfileUpdateResponse, MemoryRecallRequest,
    MemoryRecallResponse, MemoryReceiptsGetRequest, MemoryReceiptsGetResponse,
    MemoryRecordsCreateRequest, MemoryRecordsCreateResponse, MemoryRecordsDeleteRequest,
    MemoryRecordsDeleteResponse, MemoryRecordsGetRequest, MemoryRecordsGetResponse,
    MemoryRecordsListRequest, MemoryRecordsListResponse, MemoryRecordsPromoteRequest,
    MemoryRecordsPromoteResponse, MemoryRecordsUpdateRequest, MemoryRecordsUpdateResponse,
    Principal,
};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{SpaceId, SubjectId};
use mindcore_base::ir::wire::{memory_candidate_from_wire, memory_record_from_wire};
use mindcore_base::ir::{
    CandidateSelection, CandidateState, MemoryCandidateIr, MemoryGrantRow, MemoryJournalRow,
    MemoryProfileRow, MemoryRecordIr, MemoryRepository, PlacementSelection, ReceiptRead,
    ReceiptState, RecordSelection, RecordState, ReviewReceiptIr,
};

use crate::admission::{self, Admission, Coverage, Scope};
use crate::effects;
use crate::policy;
use crate::project;
use crate::reject::{settle, unsettled, AResult, Reject};
use crate::wire;
use crate::{bump, mint, principal_id, recorded_failure, MemoryAdapter, MemoryStage, Tx};

fn validated(result: SpokeResult<()>) -> AResult<()> {
    match result {
        SpokeResult::Ok(()) => Ok(()),
        SpokeResult::Reject(reject) => Err(Reject::terminal(reject.code, reject.message)),
    }
}

/// The content object normalized to the retained-record wire shape: original
/// subtrees preserved, no `null` members invented.
pub(crate) fn content_wire(value: &Value) -> AResult<Value> {
    let object = wire::object(value, "content")?;
    let mut out = Map::new();
    out.insert(
        "text".to_string(),
        Value::String(wire::required(object, "text")?.to_string()),
    );
    if let Some(structured) = object.get("structured") {
        out.insert("structured".to_string(), structured.clone());
    }
    if let Some(Value::Array(records)) = object.get("cognition") {
        out.insert("cognition".to_string(), Value::Array(records.clone()));
    }
    Ok(Value::Object(out))
}

fn provenance_value(
    object: &Map<String, Value>,
    principal: &str,
    now: chrono::DateTime<Utc>,
) -> AResult<Value> {
    let anchors = match object.get("anchors") {
        None | Some(Value::Null) => Value::Array(Vec::new()),
        Some(anchors) => anchors.clone(),
    };
    Ok(json!({
        "source_context_ids": wire::strings(object, "source_context_ids")?,
        "anchors": anchors,
        "captured_by": principal,
        "captured_at": policy::timestamp(now),
    }))
}

pub(crate) fn placement_wire(scope: &Scope) -> Value {
    project::placement_body(&scope.ir())
}

fn record_from_wire(wire_value: Value) -> AResult<MemoryRecordIr> {
    let (_, record) = memory_record_from_wire(wire_value)?;
    Ok(record)
}

fn candidate_from_wire(wire_value: Value) -> AResult<MemoryCandidateIr> {
    let (_, candidate) = memory_candidate_from_wire(wire_value)?;
    Ok(candidate)
}

fn grant_revisions(rows: &[MemoryGrantRow]) -> Vec<(String, i64)> {
    rows.iter()
        .map(|row| (row.grant_id.clone(), row.revision.as_i64().unwrap_or(0)))
        .collect()
}

/// The §6.12 read selection plus the provenance filter a read names.
pub(crate) fn read_selection(
    object: &Map<String, Value>,
) -> AResult<(PlacementSelection, Option<Vec<String>>)> {
    let selection = wire::selection(object, "selection")?.ir();
    let filter = wire::PlacementSelectionWire::filter(object, "filter")?;
    Ok((selection, filter))
}

/// One review decision, reduced to what the write path needs.
enum Decision {
    Promote {
        kind: Option<String>,
        content: Option<Value>,
    },
    Fragment {
        items: Vec<(String, Value)>,
    },
    Drop,
}

impl Decision {
    const fn outcome(&self) -> &'static str {
        match self {
            Self::Promote { .. } => "promoted",
            Self::Fragment { .. } => "fragmented",
            Self::Drop => "dropped",
        }
    }
}

fn decision_of(value: &Value) -> AResult<Decision> {
    let decision = wire::object(value, "decision")?;
    match wire::required(decision, "action")? {
        "promote" => {
            wire::closed(
                decision,
                "action",
                &["kind", "content_override"],
                "decision",
            )?;
            let kind = wire::optional(decision, "kind")?;
            let content = match decision.get("content_override") {
                None | Some(Value::Null) => None,
                Some(content) => Some(content_wire(content)?),
            };
            Ok(Decision::Promote { kind, content })
        }
        "fragment" => {
            wire::closed(decision, "action", &["fragments"], "decision")?;
            let Value::Array(items) = decision
                .get("fragments")
                .ok_or_else(|| Reject::invalid("decision.fragments is required"))?
            else {
                return Err(Reject::invalid("decision.fragments must be an array"));
            };
            let mut parsed = Vec::with_capacity(items.len());
            for item in items {
                let item = wire::object(item, "decision.fragments[]")?;
                wire::closed(item, "", &["kind", "content"], "decision.fragments[]")?;
                parsed.push((
                    wire::required(item, "kind")?.to_string(),
                    content_wire(
                        item.get("content")
                            .ok_or_else(|| Reject::invalid("fragment item content"))?,
                    )?,
                ));
            }
            Ok(Decision::Fragment { items: parsed })
        }
        "drop" => {
            wire::closed(decision, "action", &["rationale"], "decision")?;
            Ok(Decision::Drop)
        }
        _ => Err(Reject::invalid(
            "decision must be promote, fragment or drop",
        )),
    }
}

impl MemoryAdapter {
    pub async fn capture(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(
            self.capture_body(tx, principal, request)
                .await
                .map(|captured| captured.0),
        )
    }

    /// §7.11 capture that also reports whether the dedup namespace already held
    /// this key's receipt: the ids are then the recorded ones, so nothing was
    /// captured now.
    pub async fn capture_with_replay(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<(MemoryStage, bool)> {
        settle(self.capture_body(tx, principal, request).await)
    }

    async fn capture_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<(MemoryStage, bool)> {
        let request: MemoryCaptureRequest = wire::decode(&raw, "memory.capture request")?;
        validated(validate_memory_capture(
            &request,
            policy::max_content_bytes(),
        ))?;
        let object = wire::object(&raw, "memory.capture request")?;
        let principal_id = principal_id(principal);
        let subject_id = request.subject_id.to_string();
        let placement = wire::placement(object, "capture_scope")?;
        let idempotency_key = request.idempotency_key.to_string();
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.capture",
            Some(idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(vec![
                placement.requirement("capture", subject_id.as_str())
            ]),
            issuance: None,
            subject_gate: Some(subject_id.as_str()),
            placement_gate: Some(&placement),
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryCaptureResponse>(&receipt).map(|stage| (stage, true));
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let content = content_wire(
            object
                .get("content")
                .ok_or_else(|| Reject::invalid("content is required"))?,
        )?;
        let text = content
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let kind_hint = wire::optional(object, "kind_hint")?;
        let classification = policy::classify(text.as_str(), kind_hint.as_deref());
        let mut candidate_ids: Vec<String> = Vec::new();
        if classification.disposition != "drop" {
            let candidate_id = mint("cnd");
            let kind = kind_hint.unwrap_or_else(|| "note".to_string());
            let mut candidate_wire = json!({
                "schema_version": project::SCHEMA_VERSION,
                "candidate_id": candidate_id,
                "space_id": op.space.as_str(),
                "subject_id": subject_id,
                "capture_scope": placement_wire(&placement),
                "classification": {
                    "disposition": classification.disposition,
                    "rationale_codes": classification.rationale_codes,
                },
                "provenance": provenance_value(object, principal_id, op.now)?,
                "status": "pending",
                "proposed": { "kind": kind, "content": content },
                "created_at": policy::timestamp(op.now),
                "revision": 1,
                "extensions": {},
            });
            if let Some(occurred_at) = object.get("occurred_at").and_then(Value::as_str) {
                candidate_wire
                    .as_object_mut()
                    .expect("candidate wire is an object")
                    .insert("occurred_at".to_string(), json!(occurred_at));
            }
            let candidate = candidate_from_wire(candidate_wire)?;
            self.repository.insert_candidate(tx, &candidate).await?;
            candidate_ids.push(candidate_id);
        }
        let result = json!({
            "candidate_ids": candidate_ids,
            "classified": candidate_ids.len(),
        });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemoryCaptureResponse>(project::receipt_body(&receipt))
            .map(|stage| (stage, false))
    }

    pub async fn list_candidates(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.list_candidates_body(tx, principal, request).await)
    }

    async fn list_candidates_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryCandidatesListRequest =
            wire::decode(&raw, "memory.candidates.list request")?;
        validated(validate_memory_candidates_list(
            &request,
            policy::MAX_PAGE_SIZE_BOUND,
        ))?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let subject_id = request.subject_id.to_string();
        let status = match request.status.as_ref() {
            Some(status) => Some(
                serde_json::to_value(status)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_string))
                    .ok_or_else(|| Reject::internal("candidate status projection"))?,
            ),
            None => None,
        };
        let now = Utc::now();
        let rows = admission::admit_mind(tx, principal_id, &space).await?;
        let wire_grants = admission::live_wire_grants(&rows, now)?;
        let base = GrantRequirement {
            capability: "review",
            subject_id: Some(subject_id.as_str()),
            context_ids: Vec::new(),
            reads_shared: false,
            subject_all: false,
        };
        if !admission::covered(&wire_grants, principal_id, space.as_str(), now, &base) {
            return Err(Reject::not_authorized(format!(
                "principal {principal_id} holds no review grant on subject {subject_id}"
            )));
        }
        self.withhold_if_unsettled(tx, &space, subject_id.as_str())
            .await?;
        let subject = admission::subject(&self.repository, tx, &space, subject_id.as_str()).await?;
        let subject_status = subject.as_ref().map(|row| row.status.clone());
        let archived = status.is_none() && subject_status.as_deref() == Some("archived");
        let visible: Vec<MemoryCandidateIr> = if archived {
            Vec::new()
        } else {
            let selection = CandidateSelection {
                placement: PlacementSelection::SubjectAll,
                status,
            };
            self.repository
                .list_candidates(tx, &space, subject_id.as_str(), &selection)
                .await?
                .into_iter()
                .filter(|candidate| {
                    admission::covered(
                        &wire_grants,
                        principal_id,
                        space.as_str(),
                        now,
                        &admission::candidate_requirement(
                            "review",
                            subject_id.as_str(),
                            &candidate.capture_scope,
                        ),
                    )
                })
                .collect()
        };
        let marker = policy::snapshot_marker(
            visible
                .iter()
                .map(|candidate| {
                    (
                        candidate.candidate_id.clone(),
                        candidate.revision.as_i64().unwrap_or(0),
                    )
                })
                .collect(),
            &grant_revisions(&rows),
            subject_status.as_deref(),
        )?;
        let scope = policy::cursor_scope(
            "memory.candidates.list",
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
        let bodies: Vec<Value> = page.iter().map(project::candidate_body).collect();
        let next_cursor =
            next.map(|offset| policy::encode_cursor(principal_id, &scope, offset, &marker));
        policy::verify_cursor(claimed.as_deref(), &marker)?;
        Self::ready::<MemoryCandidatesListResponse>(
            json!({ "candidates": bodies, "next_cursor": next_cursor }),
        )
    }

    pub async fn get_candidate(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.get_candidate_body(tx, principal, request).await)
    }

    async fn get_candidate_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryCandidatesGetRequest =
            wire::decode(&raw, "memory.candidates.get request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let candidate_id = request.candidate_id.to_string();
        let rows = admission::admit_mind(tx, principal_id, &space).await?;
        let Some(head) =
            admission::candidate_head(&self.repository, tx, &space, candidate_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("candidate {candidate_id}")));
        };
        let now = Utc::now();
        let wire_grants = admission::live_wire_grants(&rows, now)?;
        let subject_id = head.subject_id.as_str();
        if !admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            Utc::now(),
            &admission::candidate_requirement("review", subject_id, &head.capture_scope),
        ) {
            return Err(Reject::not_found(format!("candidate {candidate_id}")));
        }
        self.withhold_if_unsettled(tx, &space, subject_id).await?;
        let Some(candidate) =
            admission::candidate(&self.repository, tx, &space, candidate_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("candidate {candidate_id}")));
        };
        Self::ready::<MemoryCandidatesGetResponse>(
            json!({ "candidate": project::candidate_body(&candidate) }),
        )
    }

    pub async fn review_candidate(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.review_body(tx, principal, request).await)
    }

    async fn review_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryCandidatesReviewRequest =
            wire::decode(&raw, "memory.candidates.review request")?;
        validated(validate_memory_candidates_review(&request))?;
        let object = wire::object(&raw, "memory.candidates.review request")?;
        let candidate_id = request.candidate_id.to_string();
        let expected_revision = Number::from(request.expected_revision.get());
        let idempotency_key = request.idempotency_key.to_string();
        let decision = decision_of(
            object
                .get("decision")
                .ok_or_else(|| Reject::invalid("decision is required"))?,
        )?;
        let principal_id = principal_id(principal);
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.candidates.review",
            Some(idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Candidate {
                candidate_id: candidate_id.as_str(),
                capabilities: &["review", "records_write"],
            },
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        self.consume::<MemoryCandidatesReviewResponse>(
            tx,
            &op,
            &admission,
            candidate_id.as_str(),
            &expected_revision,
            Some(&decision),
        )
        .await
    }

    pub async fn dismiss_candidate(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.dismiss_body(tx, principal, request).await)
    }

    async fn dismiss_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryCandidatesDismissRequest =
            wire::decode(&raw, "memory.candidates.dismiss request")?;
        let candidate_id = request.candidate_id.to_string();
        let expected_revision = Number::from(request.expected_revision.get());
        let idempotency_key = request.idempotency_key.to_string();
        let principal_id = principal_id(principal);
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.candidates.dismiss",
            Some(idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Candidate {
                candidate_id: candidate_id.as_str(),
                capabilities: &["review"],
            },
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        self.consume::<MemoryCandidatesDismissResponse>(
            tx,
            &op,
            &admission,
            candidate_id.as_str(),
            &expected_revision,
            None,
        )
        .await
    }

    pub async fn create_record(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.create_record_body(tx, principal, request).await)
    }

    async fn create_record_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryRecordsCreateRequest =
            wire::decode(&raw, "memory.records.create request")?;
        validated(validate_memory_records_create(
            &request,
            policy::max_content_bytes(),
        ))?;
        let object = wire::object(&raw, "memory.records.create request")?;
        let principal_id = principal_id(principal);
        let subject_id = request.subject_id.to_string();
        let scope = wire::placement(object, "scope")?;
        let idempotency_key = request.idempotency_key.to_string();
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.records.create",
            Some(idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(vec![
                scope.requirement("records_write", subject_id.as_str())
            ]),
            issuance: None,
            subject_gate: Some(subject_id.as_str()),
            placement_gate: Some(&scope),
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryRecordsCreateResponse>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let record_id = mint("rec");
        let content = content_wire(
            object
                .get("content")
                .ok_or_else(|| Reject::invalid("content is required"))?,
        )?;
        let kind = wire::required(object, "kind")?.to_string();
        let mut record_wire = json!({
            "schema_version": project::SCHEMA_VERSION,
            "record_id": record_id,
            "space_id": op.space.as_str(),
            "subject_id": subject_id,
            "kind": kind,
            "content": content,
            "scope": placement_wire(&scope),
            "provenance": provenance_value(object, principal_id, op.now)?,
            "created_at": policy::timestamp(op.now),
            "updated_at": policy::timestamp(op.now),
            "revision": 1,
            "extensions": {},
        });
        if let Some(occurred_at) = object.get("occurred_at").and_then(Value::as_str) {
            record_wire
                .as_object_mut()
                .expect("record wire is an object")
                .insert("occurred_at".to_string(), json!(occurred_at));
        }
        let record = record_from_wire(record_wire)?;
        self.repository.insert_record(tx, &record).await?;
        let effect = effects::record_effect(&record, None)?;
        let result = json!({ "record_id": record.record_id, "revision": 1 });
        let receipt_id = mint("rct");
        self.stage_journal(
            tx,
            &op,
            subject_id.as_str(),
            &receipt_id,
            vec![effect],
            &result,
        )
        .await
    }

    pub async fn get_record(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.get_record_body(tx, principal, request).await)
    }

    async fn get_record_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryRecordsGetRequest = wire::decode(&raw, "memory.records.get request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let record_id = request.record_id.to_string();
        let rows = admission::admit_mind(tx, principal_id, &space).await?;
        let now = Utc::now();
        let wire_grants = admission::live_wire_grants(&rows, now)?;
        let Some(head) =
            admission::record_head(&self.repository, tx, &space, record_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("record {record_id}")));
        };
        let subject_id = head.subject_id.as_str();
        let scope = Scope::from(&head.scope);
        let reads = admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &scope.requirement("records_read", subject_id),
        ) || admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &scope.requirement("recall", subject_id),
        );
        let tombstoned = head.tombstoned;
        let admin = admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &GrantRequirement {
                capability: "admin",
                subject_id: Some(subject_id),
                context_ids: Vec::new(),
                reads_shared: false,
                subject_all: false,
            },
        );
        if !reads || (tombstoned && !admin) {
            return Err(Reject::not_found(format!("record {record_id}")));
        }
        self.withhold_if_unsettled(tx, &space, subject_id).await?;
        let Some(record) =
            admission::record(&self.repository, tx, &space, record_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("record {record_id}")));
        };
        Self::ready::<MemoryRecordsGetResponse>(json!({ "record": project::record_body(&record) }))
    }

    pub async fn list_records(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.list_records_body(tx, principal, request).await)
    }

    async fn list_records_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        wire::reject_cognition_filter(&raw)?;
        let request: MemoryRecordsListRequest = wire::decode(&raw, "memory.records.list request")?;
        validated(validate_memory_records_list(
            &request,
            policy::MAX_PAGE_SIZE_BOUND,
        ))?;
        let object = wire::object(&raw, "memory.records.list request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let subject_id = request.subject_id.to_string();
        let (placement, filter) = read_selection(object)?;
        let kinds = request.kinds.clone();
        let include_tombstoned = request.include_tombstoned;
        let now = Utc::now();
        let rows = admission::admit_mind(tx, principal_id, &space).await?;
        let wire_grants = admission::live_wire_grants(&rows, now)?;
        let list = admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &admission::selection_requirement("records_read", subject_id.as_str(), &placement),
        );
        let recall = admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &admission::selection_requirement("recall", subject_id.as_str(), &placement),
        );
        if !list && !recall {
            return Err(Reject::not_authorized(format!(
                "principal {principal_id} holds no read grant covering the selection"
            )));
        }
        if include_tombstoned
            && !admission::covered(
                &wire_grants,
                principal_id,
                space.as_str(),
                now,
                &GrantRequirement {
                    capability: "admin",
                    subject_id: Some(subject_id.as_str()),
                    context_ids: Vec::new(),
                    reads_shared: false,
                    subject_all: false,
                },
            )
        {
            return Err(Reject::not_authorized(
                "include_tombstoned requires an admin grant on the subject",
            ));
        }
        self.withhold_if_unsettled(tx, &space, subject_id.as_str())
            .await?;
        let subject = admission::subject(&self.repository, tx, &space, subject_id.as_str()).await?;
        let subject_status = subject.as_ref().map(|row| row.status.clone());
        let selection = RecordSelection {
            placement,
            provenance_filter: filter,
            kinds,
            include_tombstoned,
        };
        let visible: Vec<MemoryRecordIr> = self
            .repository
            .list_records(tx, &space, subject_id.as_str(), &selection)
            .await?
            .into_iter()
            .filter(|record| {
                let scope = Scope::from(&record.scope);
                admission::covered(
                    &wire_grants,
                    principal_id,
                    space.as_str(),
                    now,
                    &scope.requirement("records_read", subject_id.as_str()),
                ) || admission::covered(
                    &wire_grants,
                    principal_id,
                    space.as_str(),
                    now,
                    &scope.requirement("recall", subject_id.as_str()),
                )
            })
            .collect();
        let marker = policy::snapshot_marker(
            visible
                .iter()
                .map(|record| {
                    (
                        record.record_id.clone(),
                        record.revision.as_i64().unwrap_or(0),
                    )
                })
                .collect(),
            &grant_revisions(&rows),
            subject_status.as_deref(),
        )?;
        let scope = policy::cursor_scope(
            "memory.records.list",
            Some(space.as_str()),
            Some(subject_id.as_str()),
            None,
        );
        let cursor = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_string());
        let (offset, claimed) = policy::cursor_page(cursor.as_deref(), principal_id, &scope)?;
        let total = visible.len() as u64;
        let (page, next) = policy::page(visible, policy::page_limit(request.limit), offset);
        let bodies: Vec<Value> = page.iter().map(project::record_body).collect();
        let next_cursor =
            next.map(|offset| policy::encode_cursor(principal_id, &scope, offset, &marker));
        policy::verify_cursor(claimed.as_deref(), &marker)?;
        Self::ready::<MemoryRecordsListResponse>(json!({
            "records": bodies,
            "total_authorized": total,
            "next_cursor": next_cursor,
        }))
    }

    pub async fn update_record(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.update_record_body(tx, principal, request).await)
    }

    async fn update_record_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryRecordsUpdateRequest =
            wire::decode(&raw, "memory.records.update request")?;
        validated(validate_memory_records_update(
            &request,
            policy::max_content_bytes(),
        ))?;
        let object = wire::object(&raw, "memory.records.update request")?;
        let principal_id = principal_id(principal);
        let space_id = wire::required(object, "space_id")?.to_string();
        let record_id = wire::required(object, "record_id")?.to_string();
        let expected_revision = Number::from(wire::revision(object, "expected_revision")?);
        let idempotency_key = wire::required(object, "idempotency_key")?.to_string();
        let op = Self::op(
            space_id.as_str(),
            principal_id,
            "memory.records.update",
            Some(idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Record {
                record_id: record_id.as_str(),
                capabilities: &["records_write"],
                target: None,
            },
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryRecordsUpdateResponse>(&receipt);
        }
        let Some(current) =
            admission::record(&self.repository, tx, &op.space, record_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("record {record_id}")));
        };
        let subject_id = current.subject_id.to_string();
        let record_scope = Scope::from(&current.scope);
        let gated = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: admission.coverage,
            issuance: None,
            subject_gate: Some(subject_id.as_str()),
            placement_gate: Some(&record_scope),
            freeze_gated: true,
        };
        admission::gate(&self.repository, tx, &gated).await?;
        if matches!(current.state, RecordState::Tombstoned { .. }) {
            return Err(Reject::terminal(
                SpokeRejectCode::InvalidStatusTransition,
                format!("record {record_id} is tombstoned"),
            ));
        }
        if current.revision != expected_revision {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!("record {record_id} is at revision {}", current.revision),
            ));
        }
        let prior = effects::record_effect(&current, None)?;
        let mut next = project::record_body(&current);
        let members = next
            .as_object_mut()
            .expect("record projection is an object");
        if let Some(content) = object.get("content") {
            members.insert("content".to_string(), content_wire(content)?);
        }
        if let Some(kind) = object.get("kind").and_then(Value::as_str) {
            members.insert("kind".to_string(), json!(kind));
        }
        members.insert("updated_at".to_string(), json!(policy::timestamp(op.now)));
        members.insert("revision".to_string(), json!(bump(&current.revision)));
        let updated = record_from_wire(next)?;
        if !self
            .repository
            .update_record(tx, &updated, &current.revision)
            .await?
        {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                "record revision changed while the update was written",
            ));
        }
        let effect = effects::record_effect(
            &updated,
            Some(effects::projection_digest(
                &effects::record_target(&record_id),
                prior
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )?),
        )?;
        let result = json!({ "record_id": record_id, "revision": updated.revision });
        let receipt_id = mint("rct");
        self.stage_journal(
            tx,
            &op,
            subject_id.as_str(),
            &receipt_id,
            vec![effect],
            &result,
        )
        .await
    }

    pub async fn promote_record(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.promote_body(tx, principal, request).await)
    }

    async fn promote_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryRecordsPromoteRequest =
            wire::decode(&raw, "memory.records.promote request")?;
        let object = wire::object(&raw, "memory.records.promote request")?;
        let principal_id = principal_id(principal);
        let record_id = request.record_id.to_string();
        let expected_revision = Number::from(request.expected_revision.get());
        let target = wire::placement(object, "target_scope")?;
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.records.promote",
            Some(request.idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Record {
                record_id: record_id.as_str(),
                capabilities: &["promote", "records_write"],
                target: Some(&target),
            },
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryRecordsPromoteResponse>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let Some(current) =
            admission::record(&self.repository, tx, &op.space, record_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("record {record_id}")));
        };
        let subject_id = current.subject_id.to_string();
        let source_scope = Scope::from(&current.scope);
        if let Some(subject) =
            admission::subject(&self.repository, tx, &op.space, subject_id.as_str()).await?
        {
            admission::require_write_allowed(&subject, &[&source_scope, &target])?;
        } else {
            return Err(Reject::not_found(format!("subject {subject_id}")));
        }
        if current.revision != expected_revision {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!("record {record_id} is at revision {}", current.revision),
            ));
        }
        let prior = effects::record_effect(&current, None)?;
        let revision = if current.scope == target.ir() {
            current.revision.clone()
        } else {
            let mut next = project::record_body(&current);
            let members = next
                .as_object_mut()
                .expect("record projection is an object");
            members.insert("scope".to_string(), placement_wire(&target));
            members.insert("updated_at".to_string(), json!(policy::timestamp(op.now)));
            members.insert("revision".to_string(), json!(bump(&current.revision)));
            let updated = record_from_wire(next)?;
            if !self
                .repository
                .update_record(tx, &updated, &current.revision)
                .await?
            {
                return Err(Reject::terminal(
                    SpokeRejectCode::RevisionConflict,
                    "record revision changed while the promotion was written",
                ));
            }
            updated.revision.clone()
        };
        let result = json!({
            "record_id": record_id,
            "revision": revision,
            "scope": placement_wire(&target),
        });
        if revision == current.revision {
            let receipt = admission::committed_receipt(&op, &mint("rct"), result);
            self.repository.insert_receipt(tx, &receipt, None).await?;
            return Self::ready::<MemoryRecordsPromoteResponse>(project::receipt_body(&receipt));
        }
        let updated = admission::record(&self.repository, tx, &op.space, record_id.as_str())
            .await?
            .ok_or_else(|| Reject::not_found(format!("record {record_id}")))?;
        let effect = effects::record_effect(
            &updated,
            Some(effects::projection_digest(
                &effects::record_target(&record_id),
                prior
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )?),
        )?;
        let receipt_id = mint("rct");
        self.stage_journal(
            tx,
            &op,
            subject_id.as_str(),
            &receipt_id,
            vec![effect],
            &result,
        )
        .await
    }

    pub async fn delete_record(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.delete_body(tx, principal, request).await)
    }

    async fn delete_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryRecordsDeleteRequest =
            wire::decode(&raw, "memory.records.delete request")?;
        let object = wire::object(&raw, "memory.records.delete request")?;
        let principal_id = principal_id(principal);
        let record_id = request.record_id.to_string();
        let expected_revision = Number::from(request.expected_revision.get());
        let reason = wire::optional(object, "reason")?;
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.records.delete",
            Some(request.idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Record {
                record_id: record_id.as_str(),
                capabilities: &["records_write"],
                target: None,
            },
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryRecordsDeleteResponse>(&receipt);
        }
        let Some(current) =
            admission::record(&self.repository, tx, &op.space, record_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("record {record_id}")));
        };
        let subject_id = current.subject_id.to_string();
        let record_scope = Scope::from(&current.scope);
        let gated = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: admission.coverage,
            issuance: None,
            subject_gate: Some(subject_id.as_str()),
            placement_gate: Some(&record_scope),
            freeze_gated: true,
        };
        admission::gate(&self.repository, tx, &gated).await?;
        if matches!(current.state, RecordState::Tombstoned { .. }) {
            return Err(Reject::terminal(
                SpokeRejectCode::InvalidStatusTransition,
                format!("record {record_id} is tombstoned"),
            ));
        }
        if current.revision != expected_revision {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!("record {record_id} is at revision {}", current.revision),
            ));
        }
        let prior = effects::record_effect(&current, None)?;
        let mut next = project::record_body(&current);
        let members = next
            .as_object_mut()
            .expect("record projection is an object");
        members.insert("content".to_string(), json!({ "text": "" }));
        members.remove("import_ref");
        let mut tombstone = json!({
            "deleted_at": policy::timestamp(op.now),
            "deleted_by": principal_id,
        });
        if let Some(reason) = &reason {
            tombstone
                .as_object_mut()
                .expect("tombstone is an object")
                .insert("reason".to_string(), json!(reason));
        }
        members.insert("tombstone".to_string(), tombstone);
        members.insert("updated_at".to_string(), json!(policy::timestamp(op.now)));
        members.insert("revision".to_string(), json!(bump(&current.revision)));
        let updated = record_from_wire(next)?;
        if !self
            .repository
            .update_record(tx, &updated, &current.revision)
            .await?
        {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                "record revision changed while the tombstone was written",
            ));
        }
        let _ = mindcore_sqlite::vectors::delete_record_vectors(&mut **tx, &record_id).await;
        let effect = effects::record_effect(
            &updated,
            Some(effects::projection_digest(
                &effects::record_target(&record_id),
                prior
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )?),
        )?;
        let result = json!({
            "record_id": record_id,
            "tombstoned": true,
        });
        let receipt_id = mint("rct");
        self.stage_journal(
            tx,
            &op,
            subject_id.as_str(),
            &receipt_id,
            vec![effect],
            &result,
        )
        .await
    }

    pub async fn recall(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.recall_body(tx, principal, request).await)
    }

    async fn recall_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        wire::reject_cognition_filter(&raw)?;
        let request: MemoryRecallRequest = wire::decode(&raw, "memory.recall request")?;
        validated(validate_memory_recall(
            &request,
            policy::MAX_PAGE_SIZE_BOUND,
        ))?;
        let object = wire::object(&raw, "memory.recall request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let subject_id = request.subject_id.to_string();
        let (placement, filter) = read_selection(object)?;
        let now = Utc::now();
        let rows = admission::admit_mind(tx, principal_id, &space).await?;
        let wire_grants = admission::live_wire_grants(&rows, now)?;
        if !admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &admission::selection_requirement("recall", subject_id.as_str(), &placement),
        ) {
            return Err(Reject::not_authorized(format!(
                "principal {principal_id} holds no recall grant covering the selection"
            )));
        }
        self.withhold_if_unsettled(tx, &space, subject_id.as_str())
            .await?;
        let subject = admission::subject(&self.repository, tx, &space, subject_id.as_str()).await?;
        let subject_status = subject.as_ref().map(|row| row.status.clone());
        let archived = subject_status.as_deref() == Some("archived");
        let selection = RecordSelection {
            placement,
            provenance_filter: filter,
            kinds: Vec::new(),
            include_tombstoned: false,
        };
        let records: Vec<MemoryRecordIr> = if archived {
            Vec::new()
        } else {
            self.repository
                .list_records(tx, &space, subject_id.as_str(), &selection)
                .await?
                .into_iter()
                .filter(|record| {
                    let scope = Scope::from(&record.scope);
                    admission::covered(
                        &wire_grants,
                        principal_id,
                        space.as_str(),
                        now,
                        &scope.requirement("recall", subject_id.as_str()),
                    )
                })
                .collect()
        };
        let terms = request
            .query
            .as_deref()
            .map(policy::query_terms)
            .unwrap_or_default();
        let mut ranked: Vec<(usize, &MemoryRecordIr)> = records
            .iter()
            .map(|record| (policy::recall_score(&record_text(record), &terms), record))
            .collect();
        ranked.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| right.created_at.cmp(&left.created_at))
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        let marker = policy::snapshot_marker(
            records
                .iter()
                .map(|record| {
                    (
                        record.record_id.clone(),
                        record.revision.as_i64().unwrap_or(0),
                    )
                })
                .collect(),
            &grant_revisions(&rows),
            subject_status.as_deref(),
        )?;
        let scope = policy::cursor_scope(
            "memory.recall",
            Some(space.as_str()),
            Some(subject_id.as_str()),
            None,
        );
        let cursor = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_string());
        let (offset, claimed) = policy::cursor_page(cursor.as_deref(), principal_id, &scope)?;
        policy::verify_cursor(claimed.as_deref(), &marker)?;
        let total = ranked.len() as u64;
        let start = usize::try_from(offset)
            .unwrap_or(ranked.len())
            .min(ranked.len());
        let remaining = &ranked[start..];
        let max_items = request.budget.max_items.get();
        let max_bytes = request.budget.max_bytes.map(|bound| bound.get());
        let mut bodies = Vec::new();
        let mut bytes = 0u64;
        let mut overflow_record_id = None;
        for (_, record) in remaining {
            if bodies.len() as u64 >= max_items {
                overflow_record_id = Some(record.record_id.clone());
                break;
            }
            let body = project::record_body(record);
            if let Some(budget) = max_bytes {
                let size = u64::try_from(body["content"].to_string().len()).unwrap_or(u64::MAX);
                if bytes.saturating_add(size) > budget {
                    overflow_record_id = Some(record.record_id.clone());
                    if bodies.is_empty() {
                        bodies.push(body);
                    }
                    break;
                }
                bytes = bytes.saturating_add(size);
            }
            bodies.push(body);
        }
        let consumed = start + bodies.len();
        let truncated = consumed < ranked.len();
        let next_cursor = truncated
            .then(|| policy::encode_cursor(principal_id, &scope, consumed as u64, &marker));
        Self::ready::<MemoryRecallResponse>(json!({
            "records": bodies,
            "total_authorized": total,
            "truncated": truncated,
            "overflow_record_id": overflow_record_id,
            "next_cursor": next_cursor,
            "engine_policy_version": policy::ENGINE_POLICY_VERSION,
        }))
    }

    pub async fn get_profile(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.get_profile_body(tx, principal, request).await)
    }

    async fn get_profile_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryProfileGetRequest = wire::decode(&raw, "memory.profile.get request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let subject_id = request.subject_id.to_string();
        let rows = admission::admit_mind(tx, principal_id, &space).await?;
        if !admission::profile_visible(
            &rows,
            principal_id,
            space.as_str(),
            subject_id.as_str(),
            Utc::now(),
        ) {
            return Err(Reject::not_authorized(format!(
                "principal {principal_id} holds no profile-visible grant on subject {subject_id}"
            )));
        }
        self.withhold_if_unsettled(tx, &space, subject_id.as_str())
            .await?;
        let kinds: Vec<String> = request.profile_kinds.clone();
        let profiles = self
            .repository
            .list_profiles(tx, &space, subject_id.as_str(), &kinds)
            .await?;
        let bodies: Vec<Value> = profiles.iter().map(project::profile_body).collect();
        Self::ready::<MemoryProfileGetResponse>(json!({ "profiles": bodies }))
    }

    pub async fn update_profile(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.update_profile_body(tx, principal, request).await)
    }

    async fn update_profile_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryProfileUpdateRequest =
            wire::decode(&raw, "memory.profile.update request")?;
        let object = wire::object(&raw, "memory.profile.update request")?;
        let principal_id = principal_id(principal);
        let subject_id = request.subject_id.to_string();
        let profile_kind = request.profile_kind.to_string();
        let document = object
            .get("document")
            .cloned()
            .ok_or_else(|| Reject::invalid("document is required"))?;
        let expected = object
            .get("expected_revision")
            .and_then(Value::as_u64)
            .map(Number::from);
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.profile.update",
            Some(request.idempotency_key.as_str()),
            &raw,
        )?;
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(vec![GrantRequirement {
                capability: "profile_write",
                subject_id: Some(subject_id.as_str()),
                context_ids: Vec::new(),
                reads_shared: false,
                subject_all: false,
            }]),
            issuance: None,
            subject_gate: Some(subject_id.as_str()),
            placement_gate: None,
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryProfileUpdateResponse>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let kinds = [profile_kind.clone()];
        let existing = self
            .repository
            .list_profiles(tx, &op.space, subject_id.as_str(), &kinds)
            .await?
            .into_iter()
            .next();
        let revision = match existing {
            Some(current) => {
                if expected.as_ref() != Some(&current.revision) {
                    return Err(Reject::terminal(
                        SpokeRejectCode::RevisionConflict,
                        format!(
                            "profile {profile_kind} of subject {subject_id} is at revision {}",
                            current.revision
                        ),
                    ));
                }
                let mut updated = current.clone();
                updated.document = document;
                updated.updated_by = CallerId::new(principal_id);
                updated.updated_at = policy::timestamp(op.now);
                updated.revision = bump(&current.revision);
                if !self
                    .repository
                    .update_profile(tx, &updated, &current.revision)
                    .await?
                {
                    return Err(Reject::terminal(
                        SpokeRejectCode::RevisionConflict,
                        "profile revision changed while the document was written",
                    ));
                }
                updated
            }
            None => {
                if let Some(expected) = &expected {
                    return Err(Reject::terminal(
                        SpokeRejectCode::RevisionConflict,
                        format!(
                            "profile {profile_kind} of subject {subject_id} is absent (expected revision {expected})"
                        ),
                    )
                    .detail("current", json!(null)));
                }
                let row = MemoryProfileRow {
                    space_id: op.space.clone(),
                    subject_id: SubjectId::new(subject_id.as_str()),
                    profile_kind: profile_kind.clone(),
                    document,
                    updated_by: CallerId::new(principal_id),
                    revision: Number::from(1),
                    created_at: policy::timestamp(op.now),
                    updated_at: policy::timestamp(op.now),
                    extensions: Map::new(),
                };
                self.repository.insert_profile(tx, &row).await?;
                row
            }
        };
        let effect = effects::profile_effect(&revision)?;
        let result = json!({
            "profile_kind": profile_kind,
            "revision": revision.revision,
        });
        let receipt_id = mint("rct");
        self.stage_journal(
            tx,
            &op,
            subject_id.as_str(),
            &receipt_id,
            vec![effect],
            &result,
        )
        .await
    }

    pub async fn get_receipt(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> SpokeResult<MemoryStage> {
        settle(self.get_receipt_body(tx, principal, request).await)
    }

    async fn get_receipt_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryReceiptsGetRequest = wire::decode(&raw, "memory.receipts.get request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        let receipt_id = request.receipt_id.to_string();
        let rows = admission::admit_mind(tx, principal_id, &space).await?;
        let now = Utc::now();
        let wire_grants = admission::live_wire_grants(&rows, now)?;
        let admin = admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &GrantRequirement {
                capability: "admin",
                subject_id: None,
                context_ids: Vec::new(),
                reads_shared: false,
                subject_all: false,
            },
        );
        match self
            .repository
            .receipt(tx, &space, receipt_id.as_str())
            .await?
        {
            None => Err(Reject::not_found(format!("receipt {receipt_id}"))),
            Some(ReceiptRead::Retained(receipt)) => {
                if receipt.requester != principal_id && !admin {
                    return Err(Reject::not_authorized(format!(
                        "receipt {receipt_id} belongs to another requester"
                    )));
                }
                if matches!(receipt.state, ReceiptState::Pending) {
                    return Err(Reject::uncertain(format!(
                        "receipt {receipt_id} is still pending"
                    )));
                }
                Self::ready::<MemoryReceiptsGetResponse>(
                    json!({ "receipt": project::receipt_body(&receipt) }),
                )
            }
            Some(ReceiptRead::Evicted {
                requester, status, ..
            }) => {
                if requester != principal_id && !admin {
                    return Err(Reject::not_authorized(format!(
                        "receipt {receipt_id} belongs to another requester"
                    )));
                }
                Err(Reject::terminal(
                    SpokeRejectCode::ReceiptExpired,
                    format!("receipt {receipt_id} body was reclaimed"),
                )
                .detail("last_known_status", json!(status)))
            }
        }
    }

    /// §7.14/§7.15 consumption: authorize, replay, gate, stage records, scrub
    /// the candidate and leave the intent for its publisher.
    async fn consume<T: serde::de::DeserializeOwned>(
        &self,
        tx: &mut Tx<'_>,
        op: &crate::admission::Op<'_>,
        admission: &Admission<'_>,
        candidate_id: &str,
        expected_revision: &Number,
        decision: Option<&Decision>,
    ) -> AResult<MemoryStage> {
        admission::authorize(&self.repository, tx, admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, op).await? {
            return Self::replay::<T>(&receipt);
        }
        let Some(candidate) =
            admission::candidate(&self.repository, tx, &op.space, candidate_id).await?
        else {
            return Err(Reject::not_found(format!("candidate {candidate_id}")));
        };
        let subject_id = candidate.subject_id.to_string();
        let placement = Scope::from(&candidate.capture_scope);
        if let Some(subject) =
            admission::subject(&self.repository, tx, &op.space, subject_id.as_str()).await?
        {
            admission::require_write_allowed(&subject, &[&placement])?;
        }
        if admission::space(&self.repository, tx, &op.space)
            .await?
            .status
            == "frozen"
        {
            return Err(Reject::terminal(
                SpokeRejectCode::SpaceFrozen,
                format!(
                    "space {} is frozen; {} is outside the maintenance allowlist",
                    op.space.as_str(),
                    op.operation
                ),
            ));
        }
        if matches!(candidate.state, CandidateState::Consumed { .. }) {
            return Err(Reject::terminal(
                SpokeRejectCode::CandidateConsumed,
                format!("candidate {candidate_id} was already consumed"),
            ));
        }
        if candidate.revision != *expected_revision {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!(
                    "candidate {candidate_id} is at revision {}",
                    candidate.revision
                ),
            ));
        }
        let receipt_id = mint("rct");
        let outcome = decision.map_or("dismissed", Decision::outcome);
        let (records, record_ids) = match decision {
            Some(decision) => {
                self.stage_review_records(tx, op, &candidate, decision)
                    .await?
            }
            None => (Vec::new(), Vec::new()),
        };
        let consumed_by = json!({
            "op": op.operation,
            "receipt_id": receipt_id,
            "outcome": outcome,
            "record_ids": record_ids,
        });
        let mut effects = records;
        effects.push(
            self.consume_candidate(tx, &candidate, expected_revision, consumed_by)
                .await?,
        );
        let result = match decision {
            Some(decision) => json!({ "outcome": decision.outcome(), "record_ids": record_ids }),
            None => json!({ "candidate_id": candidate_id, "outcome": "dismissed" }),
        };
        self.stage_journal(tx, op, subject_id.as_str(), &receipt_id, effects, &result)
            .await
    }

    /// §7.15: the records one decision yields, inserted in decision order.
    async fn stage_review_records(
        &self,
        tx: &mut Tx<'_>,
        op: &crate::admission::Op<'_>,
        candidate: &MemoryCandidateIr,
        decision: &Decision,
    ) -> AResult<(Vec<Value>, Vec<String>)> {
        let (kind, content) = match &candidate.state {
            CandidateState::Pending { kind, content, .. } => (kind.clone(), content),
            CandidateState::Consumed { .. } => {
                return Err(Reject::terminal(
                    SpokeRejectCode::CandidateConsumed,
                    format!("candidate {} was already consumed", candidate.candidate_id),
                ))
            }
        };
        let mut content_members = Map::new();
        content_members.insert("text".to_string(), json!(content.text));
        if let Some(structured) = &content.structured {
            content_members.insert("structured".to_string(), structured.clone());
        }
        if let Some(cognition) = &content.cognition {
            content_members.insert("cognition".to_string(), Value::Array(cognition.clone()));
        }
        let content = Value::Object(content_members);
        let source_kind = content.get("text").map(|_| kind.clone()).unwrap_or(kind);
        let candidates: Vec<(String, Value)> = match decision {
            Decision::Promote {
                kind: override_kind,
                content: override_content,
            } => vec![(
                override_kind.clone().unwrap_or(source_kind),
                override_content.clone().unwrap_or(content),
            )],
            Decision::Fragment { items } => items.clone(),
            Decision::Drop => Vec::new(),
        };
        let mut effects = Vec::with_capacity(candidates.len());
        let mut record_ids = Vec::with_capacity(candidates.len());
        for (kind, content) in candidates {
            let record_id = mint("rec");
            let mut record_wire = json!({
                "schema_version": project::SCHEMA_VERSION,
                "record_id": record_id,
                "space_id": op.space.as_str(),
                "subject_id": candidate.subject_id.as_str(),
                "kind": kind,
                "content": content,
                "scope": placement_wire(&Scope::from(&candidate.capture_scope)),
                "provenance": {
                    "source_context_ids": candidate.provenance.source_context_ids,
                    "anchors": candidate.provenance.anchors,
                    "captured_by": candidate.provenance.captured_by,
                    "captured_at": candidate.provenance.captured_at,
                },
                "created_at": policy::timestamp(op.now),
                "updated_at": policy::timestamp(op.now),
                "revision": 1,
                "extensions": {},
            });
            if let Some(occurred_at) = &candidate.occurred_at {
                record_wire
                    .as_object_mut()
                    .expect("record wire is an object")
                    .insert("occurred_at".to_string(), json!(occurred_at));
            }
            let record = record_from_wire(record_wire)?;
            self.repository.insert_record(tx, &record).await?;
            effects.push(effects::record_effect(&record, None)?);
            record_ids.push(record_id);
        }
        Ok((effects, record_ids))
    }

    /// §6.7/§8.1: scrub the body, stamp the digest and outcome, bump the
    /// revision under the CAS, then project the consumed shell.
    async fn consume_candidate(
        &self,
        tx: &mut Tx<'_>,
        candidate: &MemoryCandidateIr,
        expected_revision: &Number,
        consumed_by: Value,
    ) -> AResult<Value> {
        let content = candidate_content(candidate)?;
        let digest = policy::digest(&content)?;
        let mut next = project::candidate_body(candidate);
        let members = next
            .as_object_mut()
            .expect("candidate projection is an object");
        let kind = members
            .get("proposed")
            .and_then(|proposed| proposed.get("kind"))
            .cloned()
            .unwrap_or_else(|| json!("note"));
        members.insert("status".to_string(), json!("consumed"));
        members.insert("proposed".to_string(), json!({ "kind": kind }));
        members.insert("consumed_by".to_string(), consumed_by);
        members.insert("content_digest".to_string(), json!(digest));
        members.remove("expires_at");
        members.insert("revision".to_string(), json!(bump(&candidate.revision)));
        let updated = candidate_from_wire(next)?;
        if !self
            .repository
            .update_candidate(tx, &updated, expected_revision)
            .await?
        {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!(
                    "candidate {} revision changed while it was consumed",
                    candidate.candidate_id
                ),
            ));
        }
        effects::candidate_effect(&updated)
    }

    /// Stage the pending receipt and its durable intent; the caller publishes
    /// the effects and then settles.
    async fn stage_journal(
        &self,
        tx: &mut Tx<'_>,
        op: &crate::admission::Op<'_>,
        subject_id: &str,
        receipt_id: &str,
        targets: Vec<Value>,
        result: &Value,
    ) -> AResult<MemoryStage> {
        let now = policy::timestamp(op.now);
        let intent = MemoryJournalRow {
            seq: 0,
            receipt_id: receipt_id.to_string(),
            space_id: op.space.clone(),
            subject_id: SubjectId::new(subject_id),
            requester: CallerId::new(op.requester),
            operation: op.operation.to_string(),
            idempotency_key: op.idempotency_key.clone(),
            state: "pending".to_string(),
            targets,
            result: Some(result.clone()),
            created_at: now.clone(),
            updated_at: now,
        };
        self.repository.insert_journal_intent(tx, &intent).await?;
        let receipt = admission::pending_receipt(op, receipt_id);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Ok(MemoryStage::JournalPending {
            space: op.space.clone(),
            receipt_id: receipt_id.to_string(),
        })
    }

    /// Settle one journaled intent and its pending receipt together in a
    /// fresh caller transaction. The caller invokes this only after it has
    /// confirmed the publication of every effect target the intent staged —
    /// settlement is the point of no return: it marks the intent `applied`,
    /// scrubs its owed targets and commits the receipt under one CAS pair.
    /// A still-pending receipt reports `OUTCOME_UNCERTAIN` on poll/replay
    /// until this method commits.
    pub async fn settle_journal(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
        receipt_id: &str,
    ) -> SpokeResult<Value> {
        settle(
            self.settle_journal_body(tx, space, receipt_id)
                .await
                .map_err(unsettled),
        )
    }

    async fn settle_journal_body(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
        receipt_id: &str,
    ) -> AResult<Value> {
        let receipt = match self.repository.receipt(tx, space, receipt_id).await? {
            None => return Err(Reject::not_found(format!("receipt {receipt_id}"))),
            Some(ReceiptRead::Evicted { status, .. }) => {
                return Err(Reject::terminal(
                    SpokeRejectCode::ReceiptExpired,
                    format!("receipt {receipt_id} body was reclaimed"),
                )
                .detail("last_known_status", json!(status)));
            }
            Some(ReceiptRead::Retained(receipt)) => receipt,
        };
        match &receipt.state {
            ReceiptState::Committed { .. } => {
                return Ok(project::receipt_body(&receipt));
            }
            ReceiptState::Failed { error, .. } => {
                return Err(recorded_failure(error));
            }
            ReceiptState::Pending => {}
        }
        let Some(mut intent) = self
            .repository
            .journal_intent(tx, space, receipt_id)
            .await?
        else {
            return Err(Reject::invalid(format!(
                "receipt {receipt_id} has no journaled intent to settle"
            )));
        };
        if intent.state != "pending" {
            return Err(Reject::uncertain(format!(
                "journal intent for receipt {receipt_id} is {}",
                intent.state
            )));
        }
        let settled_at = policy::timestamp(Utc::now());
        intent.state = "applied".to_string();
        intent.targets = Vec::new();
        intent.updated_at = settled_at.clone();
        if !self
            .repository
            .update_journal_intent(tx, &intent, "pending")
            .await?
        {
            return Err(Reject::uncertain(format!(
                "journal intent for receipt {receipt_id} moved before settlement"
            )));
        }
        let result = intent.result.clone().unwrap_or(Value::Null);
        let settled = ReviewReceiptIr {
            state: ReceiptState::Committed {
                completed_at: settled_at,
                result,
            },
            ..*receipt
        };
        if !self
            .repository
            .update_receipt(tx, &settled, "pending", None)
            .await?
        {
            return Err(Reject::uncertain(format!(
                "receipt {receipt_id} moved before settlement"
            )));
        }
        Ok(project::receipt_body(&settled))
    }
}

/// The candidate's pending content, as the digest and the fragment source read it.
fn candidate_content(candidate: &MemoryCandidateIr) -> AResult<Value> {
    match &candidate.state {
        CandidateState::Pending { content, .. } => Ok(json!({
            "text": content.text,
            "structured": content.structured,
            "cognition": content.cognition,
        })),
        CandidateState::Consumed { .. } => Err(Reject::terminal(
            SpokeRejectCode::CandidateConsumed,
            format!("candidate {} was already consumed", candidate.candidate_id),
        )),
    }
}

/// §9.2 ranking input: the record's untrusted text, never re-interpreted.
fn record_text(record: &MemoryRecordIr) -> String {
    match &record.state {
        RecordState::Live { content, .. } => content.text.clone(),
        RecordState::Tombstoned { .. } => String::new(),
    }
}
