use mindcore_base::ir::{
    wire::{
        knowledge_entry_from_wire, knowledge_entry_to_wire, memory_candidate_from_wire,
        memory_candidate_to_wire, memory_record_from_wire, memory_record_to_wire,
        review_receipt_from_wire, review_receipt_to_wire, WireError, WireVersion,
    },
    CandidateState, KnowledgeBody, KnowledgeEntryIr, Placement, ReceiptState, RecordState,
};
use serde_json::{json, Value};

const TIME: &str = "2026-10-28T09:20:01.123400+00:00";
const OTHER_TIME: &str = "2026-10-28T09:24:00.000Z";
const DIGEST: &str = "034a5c6d74a6c8fa3fa25d0e41dd2b10d1a110464c5e9e3f56e7c1a22cf48696";

fn anchor() -> Value {
    json!({
        "schema_version": 3,
        "source_id": "run://manuscript/chapter-8",
        "span": {"start": 17.25, "end": 82},
        "label": "",
        "mime_type": "text/plain",
        "extensions": {"source_vendor": {"positions": [20, 11, 20], "extra": null}}
    })
}

fn provenance() -> Value {
    json!({
        "source_context_ids": ["ctx_z", "ctx_a", "ctx_z"],
        "anchors": [anchor(), {"schema_version": 1, "source_id": "source-two", "extensions": {}}],
        "captured_by": "prn_writer",
        "captured_at": TIME
    })
}

fn pending_candidate() -> Value {
    json!({
        "schema_version": 9,
        "candidate_id": "cand_81",
        "space_id": "spc_workshop",
        "subject_id": "sub_mira",
        "capture_scope": {"visibility": "context", "context_id": "ctx_admitted"},
        "proposed": {"kind": "local_note", "content": {"text": "Harbor closes at dusk", "structured": null}},
        "classification": {"disposition": "promote", "rationale_codes": ["z", "a", "z"], "confidence": 0},
        "status": "pending",
        "provenance": provenance(),
        "created_at": TIME,
        "occurred_at": "2026-10-25T09:24:00Z",
        "expires_at": OTHER_TIME,
        "revision": 1,
        "extensions": {"capture_vendor": {"future": [false, null, {"Deep+Key": ""}]}}
    })
}

fn live_record() -> Value {
    let mut cognition = belief("recorded-2");
    cognition["view"] = json!("recorded_at");
    json!({
        "schema_version": 4,
        "record_id": "rec_live",
        "space_id": "spc_workshop",
        "subject_id": "sub_mira",
        "kind": "local_note",
        "content": {"text": "", "structured": {"product": [false, 0, null, []]}, "cognition": [cognition]},
        "scope": {"visibility": "shared"},
        "provenance": provenance(),
        "import_ref": {"import_id": "import_a", "source_record_id": "other_record", "source_revision": 7},
        "created_at": TIME,
        "updated_at": OTHER_TIME,
        "occurred_at": TIME,
        "revision": 2,
        "extensions": {"record_vendor": {"rank": 0}}
    })
}

fn pending_receipt() -> Value {
    json!({
        "schema_version": 2,
        "receipt_id": "rcp_first",
        "op": "memory.subjects.ensure",
        "space_id": "spc_workshop",
        "requester_principal_id": "prn_mira",
        "payload_digest": DIGEST,
        "status": "pending",
        "created_at": TIME,
        "extensions": {}
    })
}

fn failed_receipt() -> Value {
    let mut receipt = pending_receipt();
    receipt["status"] = json!("failed");
    receipt["completed_at"] = json!(OTHER_TIME);
    receipt["error"] = json!({
        "code": "PRODUCT_SPECIFIC",
        "message": "",
        "details": {"retry_class": "terminal", "unexpected_by_ir": {"values": ["z", "a", null, false, 0]}},
        "extensions": {"error_vendor": {"extra": [2, 1, 2]}}
    });
    receipt
}

fn knowledge_entry() -> Value {
    json!({
        "schema_version": 17,
        "entry_id": "ke_mira",
        "entry_type": "product_character",
        "canonical_name": "Mira",
        "status": "product_verified",
        "body": {},
        "extensions": {}
    })
}

fn reference(kind: &str, id: &str) -> Value {
    json!({"kind": kind, "namespace": "fixture", "id": id})
}

fn target() -> Value {
    json!({
        "carrier": reference("knowledge_entry", "ke_harbor"),
        "target_id": "prop_harbor_closed",
        "kind": "proposition"
    })
}

fn belief(id: &str) -> Value {
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": id,
        "holder": reference("knowledge_entry", "ke_mira"),
        "target": target(),
        "assessor": reference("knowledge_entry", "ke_mira"),
        "assessor_role": "holder_self_report",
        "basis": {"kind": "evidence", "evidence": [reference("memory_record", "rec_z"), reference("memory_record", "rec_a")]},
        "assessment_context": {"context_id": "ctx_dusk"},
        "effective_at": TIME,
        "assessed_at": OTHER_TIME,
        "view": "current",
        "q": {"value": 0.9, "assertion": "belief_attribution", "method": "self_report", "calibration": "uncalibrated"},
        "extensions": {"belief_vendor": {"nested": [2, {"q": null}, 1]}}
    })
}

fn world_fact(id: &str) -> Value {
    json!({
        "fact_id": id,
        "holder": reference("world", "world_dusk"),
        "target": target(),
        "order": 0,
        "truth": "True",
        "assessment_context": {"context_id": "ctx_dusk"},
        "effective_at": TIME,
        "recorded_at": OTHER_TIME,
        "extensions": {"fact_vendor": {"certainty_note": null}}
    })
}

fn cognition_module() -> Value {
    json!({
        "records": [belief("belief_z"), belief("belief_a")],
        "world_facts": [world_fact("fact_z"), world_fact("fact_a")],
        "extensions": {"module_vendor": {"uninterpreted": [false, null, ""]}}
    })
}

#[test]
fn memory_candidate_preserves_pending_content_and_consumed_shell() {
    let mut fixture = pending_candidate();
    let (version, ir) = memory_candidate_from_wire(fixture.clone()).unwrap();
    assert_eq!(version.as_number().as_u64(), Some(9));
    assert_eq!(ir.candidate_id, "cand_81");
    assert_eq!(ir.capture_scope, Placement::Context("ctx_admitted".into()));
    assert_eq!(
        ir.provenance.source_context_ids,
        ["ctx_z", "ctx_a", "ctx_z"]
    );
    assert!(
        matches!(&ir.state, CandidateState::Pending { kind, content, expires_at }
        if kind == "local_note" && content.structured == Some(Value::Null)
          && content.text == "Harbor closes at dusk" && expires_at.as_deref() == Some(OTHER_TIME))
    );
    assert_eq!(memory_candidate_to_wire(version, ir), fixture);

    fixture["status"] = json!("consumed");
    fixture["revision"] = json!(2);
    fixture["content_digest"] = json!(DIGEST);
    fixture["consumed_by"] = json!({
        "op": "memory.candidates.review", "receipt_id": "rcp_first", "outcome": "promoted", "record_ids": ["rec_z", "rec_a", "rec_z"]
    });
    fixture["proposed"] = json!({"kind": "local_note"});
    fixture.as_object_mut().unwrap().remove("expires_at");
    let (version, ir) = memory_candidate_from_wire(fixture.clone()).unwrap();
    assert!(
        matches!(&ir.state, CandidateState::Consumed { proposed_kind, consumed_by, content_digest }
        if proposed_kind.as_deref() == Some("local_note") && content_digest == DIGEST
          && consumed_by.record_ids == ["rec_z", "rec_a", "rec_z"])
    );
    assert_eq!(memory_candidate_to_wire(version, ir), fixture);
    fixture.as_object_mut().unwrap().remove("proposed");
    let (version, ir) = memory_candidate_from_wire(fixture.clone()).unwrap();
    assert!(matches!(
        ir.state,
        CandidateState::Consumed {
            proposed_kind: None,
            ..
        }
    ));
    assert_eq!(memory_candidate_to_wire(version, ir), fixture);
}

#[test]
fn memory_record_preserves_import_and_scrubbed_tombstone() {
    let mut fixture = live_record();
    let (version, ir) = memory_record_from_wire(fixture.clone()).unwrap();
    assert_eq!(ir.record_id, "rec_live");
    assert_eq!(ir.kind, "local_note");
    assert_eq!(ir.created_at, TIME);
    assert!(
        matches!(&ir.state, RecordState::Live { import_ref: Some(import), content }
        if import.source_record_id == "other_record" && content.text.is_empty()
          && content.cognition.as_ref().unwrap()[0]["record_id"] == "recorded-2")
    );
    assert_eq!(memory_record_to_wire(version, ir), fixture);

    fixture.as_object_mut().unwrap().remove("import_ref");
    fixture["content"] = json!({"text": ""});
    let (version, ir) = memory_record_from_wire(fixture.clone()).unwrap();
    assert_eq!(memory_record_to_wire(version, ir), fixture);

    fixture["tombstone"] =
        json!({"deleted_at": OTHER_TIME, "deleted_by": "prn_writer", "reason": ""});
    let (version, ir) = memory_record_from_wire(fixture.clone()).unwrap();
    assert!(
        matches!(&ir.state, RecordState::Tombstoned { deleted_by, reason, .. }
        if deleted_by == "prn_writer" && reason.as_deref() == Some(""))
    );
    assert_eq!(memory_record_to_wire(version, ir), fixture);
}

#[test]
fn receipt_preserves_state_presence_and_open_outcomes() {
    let mut fixture = pending_receipt();
    let (version, ir) = review_receipt_from_wire(fixture.clone()).unwrap();
    assert_eq!(ir.receipt_id, "rcp_first");
    assert_eq!(ir.requester, "prn_mira");
    assert_eq!(ir.idempotency_key, None);
    assert_eq!(ir.state, ReceiptState::Pending);
    assert_eq!(review_receipt_to_wire(version, ir), fixture);

    fixture["op"] = json!("memory.capture");
    fixture["status"] = json!("committed");
    fixture["idempotency_key"] = json!("attempt-7");
    fixture["completed_at"] = json!(OTHER_TIME);
    for result in [
        json!({"candidate_ids": ["cand_z", "cand_a"], "classified": 0, "product": [false, null]}),
        Value::Null,
    ] {
        fixture["result"] = result.clone();
        let (version, ir) = review_receipt_from_wire(fixture.clone()).unwrap();
        assert!(
            matches!(&ir.state, ReceiptState::Committed { result: decoded, completed_at }
            if decoded == &result && completed_at == OTHER_TIME)
        );
        assert_eq!(review_receipt_to_wire(version, ir), fixture);
    }

    let fixture = failed_receipt();
    let (version, ir) = review_receipt_from_wire(fixture.clone()).unwrap();
    assert!(matches!(&ir.state, ReceiptState::Failed { error, .. }
        if error.code == "PRODUCT_SPECIFIC" && error.details["unexpected_by_ir"]["values"][2].is_null()));
    assert_eq!(review_receipt_to_wire(version, ir), fixture);
}

#[test]
fn knowledge_preserves_omission_and_explicit_empty_members() {
    let mut fixture = knowledge_entry();
    let (version, ir) = knowledge_entry_from_wire(fixture.clone()).unwrap();
    assert_eq!(version.as_number().as_u64(), Some(17));
    assert_eq!(ir.revision, None);
    assert_eq!(ir.modules, None);
    assert_eq!(ir.body, KnowledgeBody::default());
    assert_eq!(knowledge_entry_to_wire(version, ir), fixture);

    fixture["revision"] = json!(0);
    fixture["modules"] = json!({});
    fixture["body"] =
        json!({"summary": "", "tags": [], "attributes": [], "state": {}, "computable": {}});
    let (version, ir) = knowledge_entry_from_wire(fixture.clone()).unwrap();
    assert_eq!(
        ir.revision.as_ref().and_then(|value| value.as_u64()),
        Some(0)
    );
    assert!(ir.modules.as_ref().unwrap().other.is_empty());
    assert_eq!(ir.body.summary.as_deref(), Some(""));
    assert_eq!(ir.body.tags.as_deref(), Some([].as_slice()));
    assert_eq!(knowledge_entry_to_wire(version, ir), fixture);
}

#[test]
fn knowledge_preserves_cognition_and_open_payloads_in_carrier_order() {
    let mut fixture = knowledge_entry();
    fixture["body"] = json!({
        "summary": "Evening report",
        "tags": ["z", "a", "z"],
        "attributes": [{"trait_type": "private", "value": false}, {"trait_type": "private", "value": 0, "display_type": "", "max_value": 0}],
        "state": {"key.With.Any.Spelling": [null, {"nested": false}, 0]},
        "computable": {"product": {"unknown": true}}
    });
    fixture["source_anchor"] = anchor();
    fixture["created_at"] = json!(TIME);
    fixture["updated_at"] = json!(OTHER_TIME);
    fixture["owner"] = json!("ke_owner");
    fixture["disclosure"] = json!("owner-product-rule");
    fixture["revision"] = json!(7.0);
    fixture["extensions"] =
        json!({"unknown_product": {"aD!": [false, null, 0, {"Anything!": ""}]}});
    fixture["modules"] = json!({
        "unknown_object": {"anything!": [2, {"null": null}, 1]},
        "unknown_array": [true, ["z", "a"], null],
        "cognition": cognition_module()
    });
    let (version, mut ir) = knowledge_entry_from_wire(fixture.clone()).unwrap();
    assert_eq!(ir.entry_id, "ke_mira");
    assert_eq!(ir.body.tags.as_deref().unwrap(), ["z", "a", "z"]);
    assert_eq!(ir.owner.as_deref(), Some("ke_owner"));
    let modules = ir.modules.as_ref().unwrap();
    assert_eq!(modules.other["unknown_array"][1], json!(["z", "a"]));
    let cognition = modules.cognition.as_ref().unwrap();
    assert_eq!(cognition.records[0]["record_id"], "belief_z");
    assert_eq!(cognition.records[1]["basis"]["evidence"][0]["id"], "rec_z");
    assert_eq!(cognition.world_facts[0]["fact_id"], "fact_z");
    assert_eq!(
        knowledge_entry_to_wire(version.clone(), ir.clone()),
        fixture
    );

    ir.canonical_name = "Mira Vale".into();
    fixture["canonical_name"] = json!("Mira Vale");
    assert_eq!(knowledge_entry_to_wire(version, ir), fixture);
}

#[test]
fn ir_can_be_constructed_without_wire_or_storage() {
    let ir = KnowledgeEntryIr {
        entry_id: "ke_mira".into(),
        entry_type: "product_character".into(),
        canonical_name: "Mira".into(),
        status: "product_verified".into(),
        body: KnowledgeBody::default(),
        source_anchor: None,
        revision: None,
        created_at: None,
        updated_at: None,
        owner: None,
        disclosure: None,
        modules: None,
        extensions: Default::default(),
    };
    assert_eq!(
        knowledge_entry_to_wire(WireVersion::new(17.into()).unwrap(), ir),
        knowledge_entry()
    );
}

fn assert_unknown_field<T>(result: Result<T, WireError>, expected: &str) {
    assert!(matches!(result, Err(WireError::UnknownField { field, .. }) if field == expected));
}

#[test]
fn rejects_unknown_fields_in_closed_objects() {
    let mut candidate = pending_candidate();
    candidate["proposed"]["content"]["typo"] = json!(1);
    assert_unknown_field(memory_candidate_from_wire(candidate), "typo");

    let mut record = live_record();
    record["candidate_id"] = json!("not-inferred");
    assert_unknown_field(memory_record_from_wire(record), "candidate_id");
    let mut record = live_record();
    record["provenance"]["anchors"][0]["span"]["typo"] = json!(1);
    assert_unknown_field(memory_record_from_wire(record), "typo");

    let mut receipt = failed_receipt();
    receipt["error"]["typo"] = json!(1);
    assert_unknown_field(review_receipt_from_wire(receipt), "typo");
    let mut receipt = pending_receipt();
    receipt["result"] = Value::Null;
    assert_unknown_field(review_receipt_from_wire(receipt), "result");

    let mut entry = knowledge_entry();
    entry["body"]["typo"] = json!(1);
    assert_unknown_field(knowledge_entry_from_wire(entry), "typo");
    let mut entry = knowledge_entry();
    entry["body"]["attributes"] = json!([{"trait_type": "role", "value": "scribe", "typo": 1}]);
    assert_unknown_field(knowledge_entry_from_wire(entry), "typo");

    let mut entry = knowledge_entry();
    entry["modules"] = json!({"cognition": cognition_module()});
    let mut bad_record = entry.clone();
    bad_record["modules"]["cognition"]["records"][0]["q"]["typo"] = json!(1);
    assert!(matches!(
        knowledge_entry_from_wire(bad_record),
        Err(WireError::Cognition(..))
    ));
    entry["modules"]["cognition"]["typo"] = json!(1);
    assert!(matches!(
        knowledge_entry_from_wire(entry),
        Err(WireError::Cognition(..))
    ));
}

#[test]
fn rejects_null_for_typed_optionals_and_nonpositive_version() {
    for field in ["modules", "revision", "owner", "source_anchor"] {
        let mut entry = knowledge_entry();
        entry[field] = Value::Null;
        assert!(knowledge_entry_from_wire(entry).is_err(), "{field}");
    }
    let mut receipt = pending_receipt();
    receipt["idempotency_key"] = Value::Null;
    assert!(review_receipt_from_wire(receipt).is_err());
    for version in [json!(0), json!(-1), json!(2.5), json!("2")] {
        let mut entry = knowledge_entry();
        entry["schema_version"] = version;
        assert!(knowledge_entry_from_wire(entry).is_err());
    }
}

#[test]
fn cognition_module_extensions_share_the_extension_rule() {
    let mut entry = knowledge_entry();
    entry["modules"] = json!({"cognition": cognition_module()});
    entry["modules"]["cognition"]["extensions"] = json!({"Vendor.Key": {"note": 1}});
    assert!(matches!(
        knowledge_entry_from_wire(entry),
        Err(WireError::InvalidField { .. })
    ));

    let mut entry = knowledge_entry();
    entry["modules"] = json!({"cognition": cognition_module()});
    entry["modules"]["cognition"]["extensions"] = json!({"vendor": "not-an-object"});
    assert!(matches!(
        knowledge_entry_from_wire(entry),
        Err(WireError::Cognition(..))
    ));

    let mut entry = knowledge_entry();
    entry["modules"] = json!({"cognition": cognition_module()});
    entry["modules"]["cognition"]["extensions"] = json!([{"vendor": {}}]);
    assert!(matches!(
        knowledge_entry_from_wire(entry),
        Err(WireError::InvalidField { .. })
    ));

    let mut entry = knowledge_entry();
    entry["modules"] = json!({"cognition": cognition_module()});
    entry["modules"]["cognition"]["extensions"] = json!({
        "module_vendor": {"nested": [null, {"k": false}, 0]},
        "other-ns": {}
    });
    let (version, ir) = knowledge_entry_from_wire(entry.clone()).unwrap();
    let cognition = ir.modules.as_ref().unwrap().cognition.as_ref().unwrap();
    assert_eq!(cognition.extensions["module_vendor"]["nested"][2], json!(0));
    assert_eq!(knowledge_entry_to_wire(version, ir), entry);
}

#[test]
fn cognition_member_extensions_share_the_extension_rule() {
    for site in ["records", "world_facts"] {
        let mut entry = knowledge_entry();
        entry["modules"] = json!({"cognition": cognition_module()});
        entry["modules"]["cognition"][site][0]["extensions"] = json!({"Bad.Key": {"v": 1}});
        assert!(
            matches!(
                knowledge_entry_from_wire(entry),
                Err(WireError::InvalidField {
                    field: "extensions",
                    ..
                })
            ),
            "{site} member namespace key"
        );

        let mut entry = knowledge_entry();
        entry["modules"] = json!({"cognition": cognition_module()});
        entry["modules"]["cognition"][site][0]["extensions"] = json!({"vendor": [1, 2]});
        assert!(
            matches!(
                knowledge_entry_from_wire(entry),
                Err(WireError::Cognition(..))
            ),
            "{site} member non-object value"
        );
    }

    let mut candidate = pending_candidate();
    candidate["proposed"]["content"]["cognition"] =
        json!([{"kind": "arbitrary", "extensions": {"Bad.Key": {}}}]);
    assert!(matches!(
        memory_candidate_from_wire(candidate),
        Err(WireError::InvalidField {
            field: "extensions",
            ..
        })
    ));

    let mut entry = knowledge_entry();
    entry["modules"] = json!({"cognition": cognition_module()});
    let (version, ir) = knowledge_entry_from_wire(entry.clone()).unwrap();
    assert_eq!(knowledge_entry_to_wire(version, ir), entry);
}

#[test]
fn modules_is_an_object_with_object_or_array_values() {
    let mut entry = knowledge_entry();
    entry["modules"] = json!([{"vendor": {}}]);
    assert!(matches!(
        knowledge_entry_from_wire(entry),
        Err(WireError::ExpectedObject("knowledge_entry.modules"))
    ));

    let mut entry = knowledge_entry();
    entry["modules"] = json!({
        "object_ns": {"opaque": [null, {"Deep!": ""}]},
        "array_ns": [true, ["z", "a"], null]
    });
    let (version, ir) = knowledge_entry_from_wire(entry.clone()).unwrap();
    let modules = ir.modules.as_ref().unwrap();
    assert_eq!(modules.other["array_ns"], json!([true, ["z", "a"], null]));
    assert_eq!(knowledge_entry_to_wire(version, ir), entry);
}

#[test]
fn extension_values_are_opaque_and_round_trip() {
    for value in [json!(5), json!([1, "two", null]), json!("text")] {
        let mut entry = knowledge_entry();
        entry["extensions"] = json!({"md_note": value.clone()});
        entry["source_anchor"] = anchor();
        entry["source_anchor"]["extensions"] = json!({"md_note": value.clone()});
        let (version, ir) = knowledge_entry_from_wire(entry.clone()).unwrap();
        assert_eq!(knowledge_entry_to_wire(version, ir), entry);

        let mut candidate = pending_candidate();
        candidate["extensions"] = json!({"md_note": value.clone()});
        candidate["provenance"]["anchors"][0]["extensions"] = json!({"md_note": value.clone()});
        let (version, ir) = memory_candidate_from_wire(candidate.clone()).unwrap();
        assert_eq!(memory_candidate_to_wire(version, ir), candidate);

        let mut receipt = failed_receipt();
        receipt["extensions"] = json!({"md_note": value.clone()});
        receipt["error"]["extensions"] = json!({"md_note": value.clone()});
        let (version, ir) = review_receipt_from_wire(receipt.clone()).unwrap();
        assert_eq!(review_receipt_to_wire(version, ir), receipt);
    }

    let mut entry = knowledge_entry();
    entry["extensions"] = json!({"md.note": 1});
    assert!(matches!(
        knowledge_entry_from_wire(entry),
        Err(WireError::InvalidField {
            field: "extensions",
            ..
        })
    ));
    let mut candidate = pending_candidate();
    candidate["provenance"]["anchors"][1]["extensions"] = json!({"md.note": 1});
    assert!(matches!(
        memory_candidate_from_wire(candidate),
        Err(WireError::InvalidField {
            field: "extensions",
            ..
        })
    ));
}

#[test]
fn timestamps_keep_the_shared_strict_rfc_3339_grammar() {
    // `chrono` alone admits a space where RFC 3339 requires `T`; a record accepted
    // on that spelling would be refused by the published DTO gate when re-sent.
    let mut candidate = pending_candidate();
    candidate["created_at"] = json!("2026-10-25 09:24:00Z");
    assert!(matches!(
        memory_candidate_from_wire(candidate),
        Err(WireError::InvalidField {
            field: "created_at",
            ..
        })
    ));

    let mut candidate = pending_candidate();
    candidate["occurred_at"] = json!("2026-10-25T09:24:00Z");
    assert!(memory_candidate_from_wire(candidate).is_ok());
}
