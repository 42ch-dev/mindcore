use serde_json::Value;

use super::{
    array, as_object, check_cognition, check_cognition_members, extensions, nonempty, number,
    object, put, revision, string, strings, timestamp, validate_anchor, Fields, WireError,
    WireVersion,
};
use crate::ir::{
    CandidateState, Classification, Consumption, ImportReference, MemoryCandidateIr, MemoryContent,
    MemoryRecordIr, Placement, Provenance, ReceiptFailure, ReceiptState, RecordState,
    ReviewReceiptIr,
};

pub fn memory_candidate_from_wire(
    value: Value,
) -> Result<(WireVersion, MemoryCandidateIr), WireError> {
    let mut fields = Fields::new("memory_candidate", value)?;
    let version = fields.version()?;
    let state = match fields.read("status", string)?.as_str() {
        "pending" => {
            let mut proposed = Fields::new("memory_candidate.proposed", fields.take("proposed")?)?;
            let state = CandidateState::Pending {
                kind: proposed.read("kind", nonempty)?,
                content: content_from_wire(proposed.take("content")?)?,
                expires_at: fields.optional("expires_at", timestamp)?,
            };
            proposed.finish()?;
            state
        }
        "consumed" => {
            let proposed_kind = fields
                .values
                .remove("proposed")
                .map(|value| {
                    let mut proposed = Fields::new("memory_candidate.proposed", value)?;
                    let kind = proposed.read("kind", nonempty)?;
                    proposed.finish()?;
                    Ok(kind)
                })
                .transpose()?;
            CandidateState::Consumed {
                proposed_kind,
                consumed_by: consumption_from_wire(fields.take("consumed_by")?)?,
                content_digest: fields.read("content_digest", digest)?,
            }
        }
        _ => return Err(fields.invalid("status")),
    };
    let candidate = MemoryCandidateIr {
        candidate_id: fields.read("candidate_id", nonempty)?,
        space_id: fields.read("space_id", nonempty)?,
        subject_id: fields.read("subject_id", nonempty)?,
        capture_scope: placement_from_wire(fields.take("capture_scope")?)?,
        classification: classification_from_wire(fields.take("classification")?)?,
        provenance: provenance_from_wire(fields.take("provenance")?)?,
        state,
        occurred_at: fields.optional("occurred_at", timestamp)?,
        created_at: fields.read("created_at", timestamp)?,
        revision: fields.read("revision", revision)?,
        extensions: fields.read("extensions", extensions)?,
    };
    fields.finish()?;
    Ok((version, candidate))
}

pub fn memory_candidate_to_wire(version: WireVersion, candidate: MemoryCandidateIr) -> Value {
    let mut fields = object([
        ("schema_version", version.0.into()),
        ("candidate_id", candidate.candidate_id.into()),
        ("space_id", candidate.space_id.into()),
        ("subject_id", candidate.subject_id.into()),
        ("capture_scope", placement_to_wire(candidate.capture_scope)),
        (
            "classification",
            classification_to_wire(candidate.classification),
        ),
        ("provenance", provenance_to_wire(candidate.provenance)),
        ("created_at", candidate.created_at.into()),
        ("revision", candidate.revision.into()),
        ("extensions", candidate.extensions.into()),
    ]);
    put(&mut fields, "occurred_at", candidate.occurred_at);
    match candidate.state {
        CandidateState::Pending {
            kind,
            content,
            expires_at,
        } => {
            fields.insert("status".into(), "pending".into());
            fields.insert(
                "proposed".into(),
                object([("kind", kind.into()), ("content", content_to_wire(content))]).into(),
            );
            put(&mut fields, "expires_at", expires_at);
        }
        CandidateState::Consumed {
            proposed_kind,
            consumed_by,
            content_digest,
        } => {
            fields.insert("status".into(), "consumed".into());
            put(
                &mut fields,
                "proposed",
                proposed_kind.map(|kind| object([("kind", kind.into())])),
            );
            fields.insert(
                "consumed_by".into(),
                object([
                    ("op", consumed_by.operation.into()),
                    ("receipt_id", consumed_by.receipt_id.into()),
                    ("outcome", consumed_by.outcome.into()),
                    ("record_ids", consumed_by.record_ids.into()),
                ])
                .into(),
            );
            fields.insert("content_digest".into(), content_digest.into());
        }
    }
    fields.into()
}

fn consumption_from_wire(value: Value) -> Result<Consumption, WireError> {
    let mut fields = Fields::new("memory_candidate.consumed_by", value)?;
    let consumption = Consumption {
        operation: fields.read("op", |value| {
            string(value).filter(|op| {
                matches!(
                    op.as_str(),
                    "memory.candidates.review" | "memory.candidates.dismiss"
                )
            })
        })?,
        receipt_id: fields.read("receipt_id", nonempty)?,
        outcome: fields.read("outcome", |value| {
            string(value).filter(|outcome| {
                matches!(
                    outcome.as_str(),
                    "promoted" | "fragmented" | "dropped" | "dismissed"
                )
            })
        })?,
        record_ids: fields.read("record_ids", strings)?,
    };
    fields.finish()?;
    Ok(consumption)
}

fn classification_from_wire(value: Value) -> Result<Classification, WireError> {
    let mut fields = Fields::new("memory_candidate.classification", value)?;
    let classification = Classification {
        disposition: fields.read("disposition", |value| {
            string(value).filter(|disposition| {
                matches!(disposition.as_str(), "promote" | "fragment" | "drop")
            })
        })?,
        rationale_codes: fields.read("rationale_codes", strings)?,
        confidence: fields.optional("confidence", |value| {
            number(value).filter(|value| {
                value
                    .as_f64()
                    .is_some_and(|value| (0.0..=1.0).contains(&value))
            })
        })?,
    };
    fields.finish()?;
    Ok(classification)
}

fn classification_to_wire(classification: Classification) -> Value {
    let mut fields = object([
        ("disposition", classification.disposition.into()),
        ("rationale_codes", classification.rationale_codes.into()),
    ]);
    put(&mut fields, "confidence", classification.confidence);
    fields.into()
}

fn placement_from_wire(value: Value) -> Result<Placement, WireError> {
    let mut fields = Fields::new("memory placement", value)?;
    let scope = match fields.read("visibility", string)?.as_str() {
        "shared" => Placement::Shared,
        "context" => Placement::Context(fields.read("context_id", nonempty)?),
        _ => return Err(fields.invalid("visibility")),
    };
    fields.finish()?;
    Ok(scope)
}

fn placement_to_wire(scope: Placement) -> Value {
    match scope {
        Placement::Shared => object([("visibility", "shared".into())]).into(),
        Placement::Context(context_id) => object([
            ("visibility", "context".into()),
            ("context_id", context_id.into()),
        ])
        .into(),
    }
}

fn content_from_wire(value: Value) -> Result<MemoryContent, WireError> {
    const NAME: &str = "memory content";
    let mut fields = Fields::new(NAME, value)?;
    let content = MemoryContent {
        text: fields.read("text", string)?,
        structured: fields.values.remove("structured"),
        cognition: fields.optional("cognition", array)?,
    };
    fields.finish()?;
    if let Some(records) = &content.cognition {
        check_cognition_members(NAME, records)?;
    }
    for record in content.cognition.iter().flatten() {
        check_cognition(NAME, spoke_operations::validate_cognition_record(record))?;
    }
    Ok(content)
}

fn content_to_wire(content: MemoryContent) -> Value {
    let mut fields = object([("text", content.text.into())]);
    put(&mut fields, "structured", content.structured);
    put(&mut fields, "cognition", content.cognition);
    fields.into()
}

fn provenance_from_wire(value: Value) -> Result<Provenance, WireError> {
    let mut fields = Fields::new("memory provenance", value)?;
    let provenance = Provenance {
        source_context_ids: fields.read("source_context_ids", strings)?,
        anchors: fields.read("anchors", array)?,
        captured_by: fields.read("captured_by", nonempty)?,
        captured_at: fields.read("captured_at", timestamp)?,
    };
    fields.finish()?;
    for anchor in &provenance.anchors {
        validate_anchor(anchor)?;
    }
    Ok(provenance)
}

fn provenance_to_wire(provenance: Provenance) -> Value {
    object([
        ("source_context_ids", provenance.source_context_ids.into()),
        ("anchors", provenance.anchors.into()),
        ("captured_by", provenance.captured_by.into()),
        ("captured_at", provenance.captured_at.into()),
    ])
    .into()
}

pub fn memory_record_from_wire(value: Value) -> Result<(WireVersion, MemoryRecordIr), WireError> {
    let mut fields = Fields::new("retained_record", value)?;
    let version = fields.version()?;
    let content = fields.take("content")?;
    let state = if let Some(tombstone) = fields.values.remove("tombstone") {
        let mut scrubbed = Fields::new("retained_record.content", content)?;
        scrubbed.read("text", |value| string(value).filter(String::is_empty))?;
        scrubbed.finish()?;
        let mut tombstone = Fields::new("retained_record.tombstone", tombstone)?;
        let state = RecordState::Tombstoned {
            deleted_at: tombstone.read("deleted_at", timestamp)?,
            deleted_by: tombstone.read("deleted_by", nonempty)?,
            reason: tombstone.optional("reason", string)?,
        };
        tombstone.finish()?;
        state
    } else {
        RecordState::Live {
            content: content_from_wire(content)?,
            import_ref: fields
                .values
                .remove("import_ref")
                .map(import_ref_from_wire)
                .transpose()?,
        }
    };
    let record = MemoryRecordIr {
        record_id: fields.read("record_id", nonempty)?,
        space_id: fields.read("space_id", nonempty)?,
        subject_id: fields.read("subject_id", nonempty)?,
        kind: fields.read("kind", nonempty)?,
        scope: placement_from_wire(fields.take("scope")?)?,
        provenance: provenance_from_wire(fields.take("provenance")?)?,
        state,
        occurred_at: fields.optional("occurred_at", timestamp)?,
        created_at: fields.read("created_at", timestamp)?,
        updated_at: fields.read("updated_at", timestamp)?,
        revision: fields.read("revision", revision)?,
        extensions: fields.read("extensions", extensions)?,
    };
    fields.finish()?;
    Ok((version, record))
}

pub fn memory_record_to_wire(version: WireVersion, record: MemoryRecordIr) -> Value {
    let mut fields = object([
        ("schema_version", version.0.into()),
        ("record_id", record.record_id.into()),
        ("space_id", record.space_id.into()),
        ("subject_id", record.subject_id.into()),
        ("kind", record.kind.into()),
        ("scope", placement_to_wire(record.scope)),
        ("provenance", provenance_to_wire(record.provenance)),
        ("created_at", record.created_at.into()),
        ("updated_at", record.updated_at.into()),
        ("revision", record.revision.into()),
        ("extensions", record.extensions.into()),
    ]);
    put(&mut fields, "occurred_at", record.occurred_at);
    match record.state {
        RecordState::Live {
            content,
            import_ref,
        } => {
            fields.insert("content".into(), content_to_wire(content));
            put(
                &mut fields,
                "import_ref",
                import_ref.map(|value| {
                    object([
                        ("import_id", value.import_id.into()),
                        ("source_record_id", value.source_record_id.into()),
                        ("source_revision", value.source_revision.into()),
                    ])
                }),
            );
        }
        RecordState::Tombstoned {
            deleted_at,
            deleted_by,
            reason,
        } => {
            fields.insert("content".into(), object([("text", "".into())]).into());
            let mut tombstone = object([
                ("deleted_at", deleted_at.into()),
                ("deleted_by", deleted_by.into()),
            ]);
            put(&mut tombstone, "reason", reason);
            fields.insert("tombstone".into(), tombstone.into());
        }
    }
    fields.into()
}

fn import_ref_from_wire(value: Value) -> Result<ImportReference, WireError> {
    let mut fields = Fields::new("retained_record.import_ref", value)?;
    let reference = ImportReference {
        import_id: fields.read("import_id", nonempty)?,
        source_record_id: fields.read("source_record_id", nonempty)?,
        source_revision: fields.read("source_revision", revision)?,
    };
    fields.finish()?;
    Ok(reference)
}

fn digest(value: Value) -> Option<String> {
    string(value).filter(|value| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

pub fn review_receipt_from_wire(value: Value) -> Result<(WireVersion, ReviewReceiptIr), WireError> {
    let mut fields = Fields::new("memory_receipt", value)?;
    let version = fields.version()?;
    let state = match fields.read("status", string)?.as_str() {
        "pending" => ReceiptState::Pending,
        "committed" => ReceiptState::Committed {
            completed_at: fields.read("completed_at", timestamp)?,
            result: fields.take("result")?,
        },
        "failed" => ReceiptState::Failed {
            completed_at: fields.read("completed_at", timestamp)?,
            error: failure_from_wire(fields.take("error")?)?,
        },
        _ => return Err(fields.invalid("status")),
    };
    let receipt = ReviewReceiptIr {
        receipt_id: fields.read("receipt_id", nonempty)?,
        space_id: fields.read("space_id", nonempty)?,
        requester: fields.read("requester_principal_id", nonempty)?,
        operation: fields.read("op", nonempty)?,
        idempotency_key: fields.optional("idempotency_key", |value| {
            nonempty(value).filter(|value| value.chars().take(129).count() <= 128)
        })?,
        payload_digest: fields.read("payload_digest", digest)?,
        created_at: fields.read("created_at", timestamp)?,
        state,
        extensions: fields.read("extensions", extensions)?,
    };
    fields.finish()?;
    Ok((version, receipt))
}

pub fn review_receipt_to_wire(version: WireVersion, receipt: ReviewReceiptIr) -> Value {
    let mut fields = object([
        ("schema_version", version.0.into()),
        ("receipt_id", receipt.receipt_id.into()),
        ("space_id", receipt.space_id.into()),
        ("requester_principal_id", receipt.requester.into()),
        ("op", receipt.operation.into()),
        ("payload_digest", receipt.payload_digest.into()),
        ("created_at", receipt.created_at.into()),
        ("extensions", receipt.extensions.into()),
    ]);
    put(&mut fields, "idempotency_key", receipt.idempotency_key);
    let status = match receipt.state {
        ReceiptState::Pending => "pending",
        ReceiptState::Committed {
            completed_at,
            result,
        } => {
            fields.insert("completed_at".into(), completed_at.into());
            fields.insert("result".into(), result);
            "committed"
        }
        ReceiptState::Failed {
            completed_at,
            error,
        } => {
            fields.insert("completed_at".into(), completed_at.into());
            fields.insert(
                "error".into(),
                object([
                    ("code", error.code.into()),
                    ("message", error.message.into()),
                    ("details", error.details.into()),
                    ("extensions", error.extensions.into()),
                ])
                .into(),
            );
            "failed"
        }
    };
    fields.insert("status".into(), status.into());
    fields.into()
}

fn failure_from_wire(value: Value) -> Result<ReceiptFailure, WireError> {
    let mut fields = Fields::new("memory_receipt.error", value)?;
    let failure = ReceiptFailure {
        code: fields.read("code", string)?,
        message: fields.read("message", string)?,
        details: fields.read("details", as_object)?,
        extensions: fields.read("extensions", extensions)?,
    };
    fields.finish()?;
    if !matches!(
        failure.details.get("retry_class").and_then(Value::as_str),
        Some("retryable" | "terminal" | "uncertain")
    ) {
        return Err(WireError::InvalidField {
            object: "memory_receipt.error.details",
            field: "retry_class",
        });
    }
    Ok(failure)
}
