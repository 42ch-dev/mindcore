//! C0–C9: self-description, space lifecycle, subjects and grants.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Number, Value};
use spoke_operations::{
    validate_memory_grants_list, validate_memory_spaces_list, validate_memory_subjects_fence,
    GrantRequirement, IssuedGrant, SpokeRejectCode,
};
use spoke_schemas::{
    MemoryDescribeRequest, MemoryDescribeResponse, MemoryGrantsGrantRequest,
    MemoryGrantsGrantResponse, MemoryGrantsListRequest, MemoryGrantsListResponse,
    MemoryGrantsRevokeRequest, MemoryGrantsRevokeResponse, MemorySpacesFreezeRequest,
    MemorySpacesFreezeResponse, MemorySpacesListRequest, MemorySpacesListResponse,
    MemorySpacesUnfreezeRequest, MemorySpacesUnfreezeResponse, MemorySubjectsEnsureRequest,
    MemorySubjectsEnsureResponse, MemorySubjectsFenceRequest, MemorySubjectsFenceResponse,
    MemorySubjectsGetRequest, MemorySubjectsGetResponse, Principal,
};

use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::{MemoryGrantRow, MemoryRepository, MemorySubjectRow};

use crate::admission::{self, Admission, Coverage};
use crate::policy;
use crate::project;
use crate::reject::{settle, AResult, Reject};
use crate::wire;
use crate::{bump, mint, principal_id, MemoryAdapter, MemoryStage, Tx};

fn space_wide(capability: &str) -> GrantRequirement<'_> {
    GrantRequirement {
        capability,
        subject_id: None,
        context_ids: Vec::new(),
        reads_shared: false,
        subject_all: false,
    }
}

fn subject_scoped<'a>(capability: &'a str, subject_id: &'a str) -> GrantRequirement<'a> {
    GrantRequirement {
        capability,
        subject_id: Some(subject_id),
        context_ids: Vec::new(),
        reads_shared: false,
        subject_all: false,
    }
}

impl MemoryAdapter {
    pub async fn describe(
        &self,
        _tx: &mut Tx<'_>,
        _principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.describe_body(request))
    }

    fn describe_body(&self, raw: Value) -> AResult<MemoryStage> {
        let _request: MemoryDescribeRequest = wire::decode(&raw, "memory.describe request")?;
        Self::ready::<MemoryDescribeResponse>(policy::describe_body(self.reflections))
    }

    pub async fn list_spaces(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.list_spaces_body(tx, principal, request).await)
    }

    async fn list_spaces_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemorySpacesListRequest = wire::decode(&raw, "memory.spaces.list request")?;
        match validate_memory_spaces_list(&request, policy::MAX_PAGE_SIZE_BOUND) {
            spoke_operations::SpokeResult::Ok(()) => {}
            spoke_operations::SpokeResult::Reject(reject) => {
                return Err(Reject::terminal(reject.code, reject.message));
            }
        }
        let principal_id = principal_id(principal);
        let now = Utc::now();
        let mut visible_space_ids: HashSet<String> = HashSet::new();
        self.repository
            .scan_principal_grants(tx, principal_id, |grant| {
                if admission::live_grant(&grant, now) {
                    visible_space_ids.insert(grant.space_id.as_str().to_string());
                }
            })
            .await?;
        let visible_spaces: Vec<&str> = visible_space_ids.iter().map(String::as_str).collect();
        let grant_revisions = self
            .repository
            .principal_grant_revisions(tx, principal_id, &visible_spaces)
            .await?;
        let rows = self.repository.list_spaces(tx).await?;
        let mut visible = Vec::with_capacity(rows.len());
        for row in rows {
            if visible_space_ids.contains(row.space_id.as_str()) {
                visible.push(row);
            }
        }
        let marker = policy::snapshot_marker(
            visible
                .iter()
                .map(|row| {
                    (
                        row.space_id.as_str().to_string(),
                        row.revision.as_i64().unwrap_or(0),
                    )
                })
                .collect(),
            &grant_revisions,
            None,
        )?;
        let scope = policy::cursor_scope("memory.spaces.list", None, None, None);
        let cursor = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_string());
        let (offset, claimed) = policy::cursor_page(cursor.as_deref(), principal_id, &scope)?;
        let (page, next) = policy::page(visible, policy::page_limit(request.limit), offset);
        let bodies: Vec<Value> = page.iter().map(project::space_body).collect();
        let next_cursor =
            next.map(|offset| policy::encode_cursor(principal_id, &scope, offset, &marker));
        let body = json!({ "spaces": bodies, "next_cursor": next_cursor });
        policy::verify_cursor(claimed.as_deref(), &marker)?;
        Self::ready::<MemorySpacesListResponse>(body)
    }

    pub async fn freeze_space(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.freeze_body(tx, principal, request).await)
    }

    pub async fn unfreeze_space(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.unfreeze_body(tx, principal, request).await)
    }

    async fn freeze_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemorySpacesFreezeRequest =
            wire::decode(&raw, "memory.spaces.freeze request")?;
        let stage = self
            .space_status::<MemorySpacesFreezeResponse>(
                tx,
                principal,
                "memory.spaces.freeze",
                "frozen",
                false,
                request.space_id.as_str(),
                request.expected_revision.get(),
                request.idempotency_key.as_str(),
                &raw,
            )
            .await?;
        Self::recheck::<MemorySpacesFreezeResponse>(stage)
    }

    async fn unfreeze_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemorySpacesUnfreezeRequest =
            wire::decode(&raw, "memory.spaces.unfreeze request")?;
        let stage = self
            .space_status::<MemorySpacesUnfreezeResponse>(
                tx,
                principal,
                "memory.spaces.unfreeze",
                "active",
                false,
                request.space_id.as_str(),
                request.expected_revision.get(),
                request.idempotency_key.as_str(),
                &raw,
            )
            .await?;
        Self::recheck::<MemorySpacesUnfreezeResponse>(stage)
    }

    #[allow(clippy::too_many_arguments)]
    async fn space_status<T: serde::de::DeserializeOwned>(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        operation: &'static str,
        target: &'static str,
        freeze_gated: bool,
        space_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        request: &Value,
    ) -> AResult<MemoryStage> {
        let principal_id = principal_id(principal);
        let op = Self::op(
            space_id,
            principal_id,
            operation,
            Some(idempotency_key),
            request,
        )?;
        let requirements = vec![space_wide("admin")];
        let admission = Admission {
            operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(requirements),
            issuance: None,
            subject_gate: None,
            placement_gate: None,
            freeze_gated,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<T>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let current = admission::space(&self.repository, tx, &op.space).await?;
        if current.revision.as_u64() != Some(expected_revision) {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!(
                    "space {} is at revision {}",
                    current.space_id.as_str(),
                    current.revision
                ),
            ));
        }
        let revision = if current.status == target {
            current.revision.clone()
        } else {
            let mut updated = current.clone();
            updated.status = target.to_string();
            updated.revision = bump(&current.revision);
            if !self
                .repository
                .update_space(tx, &updated, &current.revision)
                .await?
            {
                return Err(Reject::terminal(
                    SpokeRejectCode::RevisionConflict,
                    "space revision changed while the status was written",
                ));
            }
            updated.revision
        };
        let result = json!({
            "status": target,
            "pending_receipts": current.pending_receipts,
            "revision": revision,
        });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Ok(MemoryStage::Ready(project::receipt_body(&receipt)))
    }

    pub async fn ensure_subject(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.ensure_subject_body(tx, principal, request).await)
    }

    async fn ensure_subject_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemorySubjectsEnsureRequest =
            wire::decode(&raw, "memory.subjects.ensure request")?;
        let principal_id = principal_id(principal);
        let subject_id = request.subject_id.as_str();
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.subjects.ensure",
            None,
            &raw,
        )?;
        let requirements = vec![subject_scoped("capture", subject_id)];
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(requirements),
            issuance: None,
            subject_gate: Some(subject_id),
            placement_gate: None,
            freeze_gated: true,
        };
        admission::authorize(&self.repository, tx, &admission, op.now).await?;
        admission::gate(&self.repository, tx, &admission).await?;
        let (created, status, revision) =
            match admission::subject(&self.repository, tx, &op.space, subject_id).await? {
                Some(row) => (false, row.status, row.revision),
                None => {
                    let row = MemorySubjectRow {
                        space_id: op.space.clone(),
                        subject_id: SubjectId::new(subject_id),
                        kind: request
                            .kind
                            .as_ref()
                            .map(|kind| (kind.ns.to_string(), kind.id.to_string())),
                        status: "active".to_string(),
                        fenced_contexts: Vec::new(),
                        revision: Number::from(1),
                        created_at: policy::timestamp(op.now),
                        extensions: Map::new(),
                    };
                    self.repository.insert_subject(tx, &row).await?;
                    (true, row.status, row.revision)
                }
            };
        let result = json!({
            "subject_id": subject_id,
            "created": created,
            "status": status,
            "revision": revision,
        });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemorySubjectsEnsureResponse>(project::receipt_body(&receipt))
    }

    pub async fn get_subject(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.get_subject_body(tx, principal, request).await)
    }

    async fn get_subject_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemorySubjectsGetRequest = wire::decode(&raw, "memory.subjects.get request")?;
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        admission::space(&self.repository, tx, &space).await?;
        let subject_id = request.subject_id.as_str();
        let rows = admission::grants(&self.repository, tx, &space).await?;
        if !admission::any_grant_for_subject(
            &rows,
            principal_id,
            space.as_str(),
            subject_id,
            Utc::now(),
        ) {
            return Err(Reject::not_authorized(format!(
                "principal {principal_id} holds no live grant on subject {subject_id}"
            )));
        }
        let Some(row) = admission::subject(&self.repository, tx, &space, subject_id).await? else {
            return Err(Reject::not_found(format!("subject {subject_id}")));
        };
        Self::ready::<MemorySubjectsGetResponse>(json!({ "subject": project::subject_body(&row) }))
    }

    pub async fn fence_subject(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.fence_subject_body(tx, principal, request).await)
    }

    async fn fence_subject_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemorySubjectsFenceRequest =
            wire::decode(&raw, "memory.subjects.fence request")?;
        match validate_memory_subjects_fence(&request) {
            spoke_operations::SpokeResult::Ok(()) => {}
            spoke_operations::SpokeResult::Reject(reject) => {
                return Err(Reject::terminal(reject.code, reject.message));
            }
        }
        let object = wire::object(&raw, "memory.subjects.fence request")?;
        let action = wire::required(object, "action")?.to_string();
        let target = object
            .get("target")
            .ok_or_else(|| Reject::invalid("target is required"))?
            .clone();
        let expected_revision = request.expected_revision.get();
        let subject_id = request.subject_id.to_string();
        let principal_id = principal_id(principal);
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.subjects.fence",
            Some(request.idempotency_key.as_str()),
            &raw,
        )?;
        let requirements = vec![subject_scoped("admin", subject_id.as_str())];
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
            return Self::replay::<MemorySubjectsFenceResponse>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let Some(current) =
            admission::subject(&self.repository, tx, &op.space, subject_id.as_str()).await?
        else {
            return Err(Reject::not_found(format!("subject {subject_id}")));
        };
        if current.revision.as_u64() != Some(expected_revision) {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!("subject {subject_id} is at revision {}", current.revision),
            ));
        }
        let (status, fenced, changed) = next_fence_state(&current, &target, &action)?;
        let revision = if changed {
            let mut updated = current.clone();
            updated.status = status.clone();
            updated.fenced_contexts = fenced
                .iter()
                .map(|context| ContextId::new(context.clone()))
                .collect();
            updated.revision = bump(&current.revision);
            if !self
                .repository
                .update_subject(tx, &updated, &current.revision)
                .await?
            {
                return Err(Reject::terminal(
                    SpokeRejectCode::RevisionConflict,
                    "subject revision changed while the fence was written",
                ));
            }
            updated.revision
        } else {
            current.revision.clone()
        };
        let result = json!({
            "subject_id": subject_id,
            "status": status,
            "fenced_contexts": fenced,
            "revision": revision,
        });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemorySubjectsFenceResponse>(project::receipt_body(&receipt))
    }

    pub async fn list_grants(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.list_grants_body(tx, principal, request).await)
    }

    async fn list_grants_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryGrantsListRequest = wire::decode(&raw, "memory.grants.list request")?;
        match validate_memory_grants_list(&request, policy::MAX_PAGE_SIZE_BOUND) {
            spoke_operations::SpokeResult::Ok(()) => {}
            spoke_operations::SpokeResult::Reject(reject) => {
                return Err(Reject::terminal(reject.code, reject.message));
            }
        }
        let principal_id = principal_id(principal);
        let space = SpaceId::new(request.space_id.to_string());
        admission::space(&self.repository, tx, &space).await?;
        let now = Utc::now();
        let rows = admission::grants(&self.repository, tx, &space).await?;
        let wire_grants = admission::wire_grants(&rows)?;
        let admin = admission::covered(
            &wire_grants,
            principal_id,
            space.as_str(),
            now,
            &space_wide("admin"),
        );
        let requested = request
            .principal_id
            .as_ref()
            .map(|principal| principal.as_str().to_string());
        let applied = if admin {
            requested
        } else {
            if !rows.iter().any(|row| {
                row.principal_id.as_str() == principal_id && admission::live_grant(row, now)
            }) {
                return Err(Reject::not_authorized(format!(
                    "principal {principal_id} holds no grant in space {}",
                    space.as_str()
                )));
            }
            Some(principal_id.to_string())
        };
        let subject_filter = request
            .subject_id
            .as_ref()
            .map(|subject| subject.as_str().to_string());
        let marker = policy::snapshot_marker(
            rows.iter()
                .map(|row| (row.grant_id.clone(), row.revision.as_i64().unwrap_or(0)))
                .collect(),
            &[],
            None,
        )?;
        let scope = policy::cursor_scope(
            "memory.grants.list",
            Some(space.as_str()),
            subject_filter.as_deref(),
            applied.as_deref(),
        );
        let cursor = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_string());
        let (offset, claimed) = policy::cursor_page(cursor.as_deref(), principal_id, &scope)?;
        let page: Vec<&MemoryGrantRow> = rows
            .iter()
            .filter(|row| {
                applied
                    .as_deref()
                    .is_none_or(|principal| row.principal_id.as_str() == principal)
            })
            .filter(|row| {
                subject_filter.as_deref().is_none_or(|subject| {
                    row.subject_id
                        .as_ref()
                        .is_some_and(|scoped| scoped.as_str() == subject)
                })
            })
            .collect();
        let (page, next) = policy::page(page, policy::page_limit(request.limit), offset);
        let bodies: Vec<Value> = page.iter().map(|row| project::grant_body(row)).collect();
        let next_cursor =
            next.map(|offset| policy::encode_cursor(principal_id, &scope, offset, &marker));
        policy::verify_cursor(claimed.as_deref(), &marker)?;
        Self::ready::<MemoryGrantsListResponse>(
            json!({ "grants": bodies, "next_cursor": next_cursor }),
        )
    }

    pub async fn grant(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.grant_body(tx, principal, request).await)
    }

    async fn grant_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let _request: MemoryGrantsGrantRequest = wire::decode(&raw, "memory.grants.grant request")?;
        let object = wire::object(&raw, "memory.grants.grant request")?;
        let space_id = wire::required(object, "space_id")?.to_string();
        let grantee = wire::required(object, "principal_id")?.to_string();
        let capabilities = wire::strings(object, "capabilities")?;
        let context_ids = wire::strings(object, "context_ids")?;
        let shared_access = object
            .get("shared_access")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let subject_id = wire::optional(object, "subject_id")?;
        let expires_at = wire::optional(object, "expires_at")?;
        let idempotency_key = wire::required(object, "idempotency_key")?.to_string();
        let principal_id = principal_id(principal);
        let op = Self::op(
            space_id.as_str(),
            principal_id,
            "memory.grants.grant",
            Some(idempotency_key.as_str()),
            &raw,
        )?;
        let issued_capabilities: Vec<&str> = capabilities.iter().map(String::as_str).collect();
        let issued_contexts: Vec<String> = context_ids.clone();
        let issued_expiry: Option<DateTime<Utc>> = match expires_at.as_deref() {
            Some(raw) => Some(
                DateTime::parse_from_rfc3339(raw)
                    .map_err(|_| Reject::invalid("expires_at must be an RFC 3339 instant"))?
                    .with_timezone(&Utc),
            ),
            None => None,
        };
        let requirements = vec![space_wide("admin")];
        let issued = IssuedGrant {
            capabilities: &issued_capabilities,
            subject_id: subject_id.as_deref(),
            context_ids: &issued_contexts,
            shared_access,
            expires_at: issued_expiry,
        };
        let admission = Admission {
            operation: op.operation,
            space: &op.space,
            principal: principal_id,
            coverage: Coverage::Requirements(requirements),
            issuance: Some(issued),
            subject_gate: None,
            placement_gate: None,
            freeze_gated: true,
        };
        let rows = admission::authorize(&self.repository, tx, &admission, op.now).await?;
        if let Some(receipt) = admission::probe(&self.repository, tx, &op).await? {
            return Self::replay::<MemoryGrantsGrantResponse>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        admission::authorize_issuance(&rows, &admission, op.now)?;
        let grant_id = mint("grt");
        let row = MemoryGrantRow {
            grant_id: grant_id.clone(),
            space_id: op.space.clone(),
            subject_id: subject_id.map(SubjectId::new),
            principal_id: mindcore_base::boundary::CallerId::new(grantee),
            capabilities,
            context_ids: context_ids
                .iter()
                .map(|context| ContextId::new(context.clone()))
                .collect(),
            shared_access,
            expires_at,
            revoked_at: None,
            revision: Number::from(1),
            created_at: policy::timestamp(op.now),
            extensions: Map::new(),
        };
        self.repository.insert_grant(tx, &row).await?;
        let result = json!({ "grant_id": grant_id, "revision": 1 });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemoryGrantsGrantResponse>(project::receipt_body(&receipt))
    }

    pub async fn revoke_grant(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: Value,
    ) -> crate::SpokeResult<MemoryStage> {
        settle(self.revoke_grant_body(tx, principal, request).await)
    }

    async fn revoke_grant_body(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        raw: Value,
    ) -> AResult<MemoryStage> {
        let request: MemoryGrantsRevokeRequest =
            wire::decode(&raw, "memory.grants.revoke request")?;
        let principal_id = principal_id(principal);
        let expected_revision = request.expected_revision.get();
        let grant_id = request.grant_id.to_string();
        let op = Self::op(
            request.space_id.as_str(),
            principal_id,
            "memory.grants.revoke",
            Some(request.idempotency_key.as_str()),
            &raw,
        )?;
        let requirements = vec![space_wide("admin")];
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
            return Self::replay::<MemoryGrantsRevokeResponse>(&receipt);
        }
        admission::gate(&self.repository, tx, &admission).await?;
        let Some(current) = self
            .repository
            .grant(tx, &op.space, grant_id.as_str())
            .await?
        else {
            return Err(Reject::not_found(format!("grant {grant_id}")));
        };
        if current.revision.as_u64() != Some(expected_revision) {
            return Err(Reject::terminal(
                SpokeRejectCode::RevisionConflict,
                format!("grant {grant_id} is at revision {}", current.revision),
            ));
        }
        let revision = if current.revoked_at.is_some() {
            current.revision.clone()
        } else {
            let mut updated = current.clone();
            updated.revoked_at = Some(policy::timestamp(op.now));
            updated.revision = bump(&current.revision);
            if !self
                .repository
                .update_grant(tx, &updated, &current.revision)
                .await?
            {
                return Err(Reject::terminal(
                    SpokeRejectCode::RevisionConflict,
                    "grant revision changed while the revoke was written",
                ));
            }
            updated.revision
        };
        let result = json!({ "grant_id": grant_id, "revoked": true, "revision": revision });
        let receipt = admission::committed_receipt(&op, &mint("rct"), result);
        self.repository.insert_receipt(tx, &receipt, None).await?;
        Self::ready::<MemoryGrantsRevokeResponse>(project::receipt_body(&receipt))
    }
}

/// §7.7 transition outcome: resulting status, fenced-context set, and whether
/// anything changed (a genuine no-op keeps the revision, §7.0.4).
fn next_fence_state(
    current: &MemorySubjectRow,
    target: &Value,
    action: &str,
) -> AResult<(String, Vec<String>, bool)> {
    let mut fenced: Vec<String> = current
        .fenced_contexts
        .iter()
        .map(ToString::to_string)
        .collect();
    let invalid = || {
        Reject::terminal(
            SpokeRejectCode::InvalidStatusTransition,
            format!("{action} is not a valid transition from {}", current.status),
        )
    };
    let target = wire::object(target, "target")?;
    let context = match wire::required(target, "type")? {
        "subject" => {
            wire::closed(target, "type", &[], "target")?;
            None
        }
        "context" => {
            wire::closed(target, "type", &["context_id"], "target")?;
            Some(wire::required(target, "context_id")?.to_string())
        }
        _ => return Err(Reject::invalid("target must name a subject or a context")),
    };
    match (context, action) {
        (None, "fence") => {
            if current.status == "fenced" {
                Ok((current.status.clone(), fenced, false))
            } else {
                Ok(("fenced".to_string(), fenced, true))
            }
        }
        (None, "unfence") => {
            if current.status == "fenced" {
                Ok(("active".to_string(), fenced, true))
            } else {
                Err(invalid())
            }
        }
        (None, "archive") => {
            if current.status == "archived" {
                Ok((current.status.clone(), fenced, false))
            } else {
                Ok(("archived".to_string(), fenced, true))
            }
        }
        (Some(context_id), "fence") => {
            if fenced.contains(&context_id) {
                Ok((current.status.clone(), fenced, false))
            } else {
                fenced.push(context_id);
                Ok((current.status.clone(), fenced, true))
            }
        }
        (Some(context_id), "unfence") => {
            if !fenced.contains(&context_id) {
                return Err(invalid());
            }
            fenced.retain(|known| known != &context_id);
            Ok((current.status.clone(), fenced, true))
        }
        (Some(_), _) => Err(invalid()),
        _ => Err(Reject::invalid("action is invalid")),
    }
}
