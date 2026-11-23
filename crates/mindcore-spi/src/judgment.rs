//! The `spoke-judgment` projection of one deliberation attempt.
//!
//! [`deliberate_judgment`] builds the portable [`Judgment`] — target,
//! revision-pinned evidence, outcome and basis — from values the attempt
//! already holds, then runs the released validator on it. It reads no store:
//! the cited revisions come from the prompted page, so a projection defect is
//! refused here, before the `settled` branch commits anything.
//!
//! `judgment_id` is `jd_` plus the JCS SHA-256 of the attempt's identity
//! members and its verdict, so equal attempt content yields an equal id and
//! `created_at` — the workflow clock, read once — stays outside the digest.

use std::num::NonZeroU64;

use chrono::{SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use spoke_operations::{
    jcs_sha256_hex, validate_judgment_typed, SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{Judgment, Principal};

use crate::workflows::MEMORY_RECORD_IDENTITY;
use crate::{CitedRecord, DeliberateRequest, SpiError};

/// The released `spoke-judgment` schema this projection emits.
const JUDGMENT_SCHEMA_VERSION: u64 = 1;
/// The one judgment kind this workflow produces.
const JUDGMENT_KIND: &str = "mindcore:deliberate";
/// `details.kind` of the refusal a projection defect answers with.
const PROJECTION_KIND: &str = "judgment_projection";

/// Builds the judgment of one deliberation attempt and validates it.
///
/// A projection defect answers [`SpiError::Settlement`] with `InternalError`
/// and `details.kind` `judgment_projection`; the existing variant is reused,
/// so `SpiError` gains none.
pub(crate) fn deliberate_judgment(
    principal: &Principal,
    request: &DeliberateRequest<'_>,
    status: &'static str,
    verdict: &Value,
    cited: &[CitedRecord],
) -> Result<Judgment, SpiError> {
    let created_at = Utc::now();
    let digest = match jcs_sha256_hex(&json!({
        "principal_id": principal.principal_id,
        "space_id": request.space_id,
        "subject_id": request.subject_id,
        "holder": request.holder,
        "target": request.target,
        "status": status,
        "verdict": verdict,
    })) {
        SpokeResult::Ok(digest) => digest,
        SpokeResult::Reject(reject) => return Err(projection_refused(&reject.message)),
    };
    let mut evidence = Vec::with_capacity(cited.len());
    for record in cited {
        let Some(revision) = NonZeroU64::new(record.revision) else {
            return Err(projection_refused(
                "a cited record served no stored revision",
            ));
        };
        evidence.push(json!({
            "kind": MEMORY_RECORD_IDENTITY.0,
            "namespace": MEMORY_RECORD_IDENTITY.1,
            "id": record.record_id,
            "revision": revision,
        }));
    }
    let basis = verdict
        .get("rationale")
        .and_then(Value::as_str)
        .ok_or_else(|| projection_refused("the verdict carries no rationale to state as basis"))?;
    let wire = json!({
        "schema_version": JUDGMENT_SCHEMA_VERSION,
        "judgment_id": format!("jd_{digest}"),
        "judgment_kind": JUDGMENT_KIND,
        "target": {
            "kind": request.target.kind.to_string(),
            "id": request.target.target_id,
            "namespace": request.target.carrier.namespace,
        },
        "evidence": evidence,
        "verdict": {
            "outcome": format!("mindcore:{status}"),
            "detail": detail(verdict),
        },
        "basis": basis,
        "created_at": created_at.to_rfc3339_opts(SecondsFormat::Nanos, true),
        "extensions": {},
    });
    let judgment: Judgment = serde_json::from_value(wire)
        .map_err(|error| projection_refused(&format!("the projection does not decode: {error}")))?;
    match validate_judgment_typed(&judgment) {
        SpokeResult::Ok(()) => Ok(judgment),
        SpokeResult::Reject(reject) => Err(projection_refused(&reject.message)),
    }
}

/// The verdict members the judgment carries as portable `detail`: the
/// recommendation and the citations stay inspectable, while the rationale is
/// the judgment's own `basis` and the record is not portable.
fn detail(verdict: &Value) -> Map<String, Value> {
    verdict
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(member, _)| {
            matches!(
                member.as_str(),
                "recommendation" | "evidence_ids" | "record_ids"
            )
        })
        .map(|(member, value)| (member.clone(), value.clone()))
        .collect()
}

/// The refusal a projection defect answers with: an internal failure the
/// caller cannot repair by resending the same request.
fn projection_refused(reason: &str) -> SpiError {
    SpiError::Settlement(SpokeReject {
        code: SpokeRejectCode::InternalError,
        message: format!("the deliberation judgment could not be built: {reason}"),
        details: Some(Map::from_iter([(
            "kind".to_string(),
            Value::String(PROJECTION_KIND.to_string()),
        )])),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Evidence;
    use serde_json::json;
    use spoke_schemas::data::cognition::{CognitionReference, CognitionTarget};

    fn principal() -> Principal {
        serde_json::from_value(json!({ "principal_id": "prn_1", "extensions": {} }))
            .expect("principal")
    }

    fn holder() -> CognitionReference {
        serde_json::from_value(json!({
            "kind": "knowledge_entry",
            "namespace": "mindcore",
            "id": "ke_wren"
        }))
        .expect("holder")
    }

    fn target(kind: &str) -> CognitionTarget {
        serde_json::from_value(json!({
            "carrier": {
                "kind": "knowledge_entry",
                "namespace": "mindcore",
                "id": "ke_shared_gate"
            },
            "target_id": "prop_gate_closed",
            "kind": kind,
        }))
        .expect("target")
    }

    fn verdict() -> Value {
        json!({
            "recommendation": { "kind": "promote", "target_kind": "adr" },
            "rationale": "the gate test passed on the supplied evidence",
            "evidence_ids": ["ev_test"],
            "record_ids": ["rec_gate"],
            "record": { "kind": "belief" }
        })
    }

    fn cited() -> [CitedRecord; 1] {
        [CitedRecord {
            record_id: "rec_gate".to_string(),
            revision: 3,
        }]
    }

    fn request<'a>(
        holder: &'a CognitionReference,
        target: &'a CognitionTarget,
        evidence: &'a [Evidence<'a>],
    ) -> DeliberateRequest<'a> {
        DeliberateRequest {
            space_id: "spc_1",
            subject_id: "subj_1",
            assessment_context_id: None,
            holder,
            target,
            evidence,
            timeout: std::time::Duration::from_secs(1),
        }
    }

    #[test]
    fn every_status_maps_onto_a_valid_judgment() {
        let (holder, target, evidence) = (holder(), target("proposition"), []);
        let request = request(&holder, &target, &evidence);
        for (status, outcome) in [
            ("no_change", "mindcore:no_change"),
            ("needs_review", "mindcore:needs_review"),
            ("settled", "mindcore:settled"),
        ] {
            let judgment =
                deliberate_judgment(&principal(), &request, status, &verdict(), &cited())
                    .expect("the projection validates");
            assert!(
                validate_judgment_typed(&judgment).is_ok(),
                "{status} must pass the released validator"
            );
            let wire = serde_json::to_value(&judgment).expect("wire");
            assert_eq!(wire["schema_version"], json!(1));
            assert_eq!(wire["judgment_kind"], json!("mindcore:deliberate"));
            assert_eq!(wire["verdict"]["outcome"], json!(outcome));
            assert!(wire["verdict"].get("confidence").is_none());
            assert_eq!(
                wire["basis"],
                json!("the gate test passed on the supplied evidence")
            );
            assert_eq!(
                wire["verdict"]["detail"],
                json!({
                    "recommendation": { "kind": "promote", "target_kind": "adr" },
                    "evidence_ids": ["ev_test"],
                    "record_ids": ["rec_gate"],
                }),
                "detail carries the portable verdict members, never the record"
            );
            assert_eq!(wire["extensions"], json!({}));
            assert!(judgment.judgment_id.as_str().starts_with("jd_"));
            assert!(
                judgment.created_at.expect("stamped").to_rfc3339() != "",
                "created_at is a real clock read"
            );
        }
    }

    #[test]
    fn the_target_projects_its_kind_id_and_scope() {
        let (holder, evidence) = (holder(), []);
        for kind in ["proposition", "question", "perceived_content", "episode"] {
            let target = target(kind);
            let judgment = deliberate_judgment(
                &principal(),
                &request(&holder, &target, &evidence),
                "settled",
                &verdict(),
                &cited(),
            )
            .expect("the projection validates");
            let wire = serde_json::to_value(&judgment).expect("wire");
            assert_eq!(
                wire["target"],
                json!({ "kind": kind, "id": "prop_gate_closed", "namespace": "mindcore" }),
                "a cognition target carries no revision"
            );
        }
    }

    #[test]
    fn cited_records_pin_their_stored_revisions_and_none_leave_evidence_empty() {
        let (holder, target, evidence) = (holder(), target("proposition"), []);
        let request = request(&holder, &target, &evidence);
        let judgment = deliberate_judgment(&principal(), &request, "settled", &verdict(), &cited())
            .expect("the projection validates");
        assert_eq!(
            serde_json::to_value(&judgment).expect("wire")["evidence"],
            json!([{
                "kind": "memory_record",
                "namespace": "mindcore",
                "id": "rec_gate",
                "revision": 3,
            }])
        );

        let judgment = deliberate_judgment(&principal(), &request, "no_change", &verdict(), &[])
            .expect("an uncited judgment validates");
        assert!(judgment.evidence.is_empty());
    }

    #[test]
    fn a_zero_cited_revision_is_refused_rather_than_invented() {
        let (holder, target, evidence) = (holder(), target("proposition"), []);
        let cited = [CitedRecord {
            record_id: "rec_gate".to_string(),
            revision: 0,
        }];
        let error = deliberate_judgment(
            &principal(),
            &request(&holder, &target, &evidence),
            "settled",
            &verdict(),
            &cited,
        )
        .expect_err("a zero revision cannot pin evidence");
        assert!(
            matches!(
                &error,
                SpiError::Settlement(reject)
                    if reject.code == SpokeRejectCode::InternalError
                        && reject
                            .details
                            .as_ref()
                            .and_then(|details| details.get("kind"))
                            .and_then(Value::as_str)
                            == Some(PROJECTION_KIND)
            ),
            "the projection defect is an internal refusal, got {error:?}"
        );
    }

    #[test]
    fn the_identity_is_deterministic_and_tracks_the_status() {
        let (holder, target, evidence) = (holder(), target("proposition"), []);
        let request = request(&holder, &target, &evidence);
        let settled = deliberate_judgment(&principal(), &request, "settled", &verdict(), &cited())
            .expect("the projection validates");
        let again = deliberate_judgment(&principal(), &request, "settled", &verdict(), &cited())
            .expect("the projection validates");
        let held =
            deliberate_judgment(&principal(), &request, "needs_review", &verdict(), &cited())
                .expect("the projection validates");
        let other_verdict = {
            let mut verdict = verdict();
            verdict["rationale"] = json!("a different rationale");
            verdict
        };
        let different =
            deliberate_judgment(&principal(), &request, "settled", &other_verdict, &cited())
                .expect("the projection validates");

        assert_eq!(
            settled.judgment_id, again.judgment_id,
            "equal attempt content yields one id"
        );
        assert_ne!(settled.judgment_id, held.judgment_id);
        assert_ne!(settled.judgment_id, different.judgment_id);
    }
}
