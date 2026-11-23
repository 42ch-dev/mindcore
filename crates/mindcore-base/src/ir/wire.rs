//! SPOKE 0.17.0 structural mapping. Decode the original JSON, not a generated DTO:
//! deserialization defaults can erase presence, and typed timestamps can change spelling.
//! Encoders consume valid IR values. Protocol version admission and operation-specific validation
//! remain the adapter's responsibility.

use serde_json::{Map, Number, Value};

use super::{CognitionModule, KnowledgeBody, KnowledgeEntryIr, KnowledgeModules};

mod memory;

pub use memory::{
    memory_candidate_from_wire, memory_candidate_to_wire, memory_record_from_wire,
    memory_record_to_wire, review_receipt_from_wire, review_receipt_to_wire,
};

/// The original positive schema version, separate from the persisted record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireVersion(Number);

impl WireVersion {
    pub fn new(value: Number) -> Option<Self> {
        is_integer_at_least(&value, 1.0).then_some(Self(value))
    }

    pub fn as_number(&self) -> &Number {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("{0} must be an object")]
    ExpectedObject(&'static str),
    #[error("{object}.{field} is required")]
    MissingField {
        object: &'static str,
        field: &'static str,
    },
    #[error("{object}.{field} has an invalid value")]
    InvalidField {
        object: &'static str,
        field: &'static str,
    },
    #[error("{object} has unknown field {field:?}")]
    UnknownField { object: &'static str, field: String },
    #[error("{0} failed SPOKE cognition validation: {1}")]
    Cognition(&'static str, String),
    #[error("invalid module namespace or value: {0:?}")]
    InvalidModule(String),
}

type Object = Map<String, Value>;

struct Fields {
    object: &'static str,
    values: Object,
}

impl Fields {
    fn new(object: &'static str, value: Value) -> Result<Self, WireError> {
        let values = as_object(value).ok_or(WireError::ExpectedObject(object))?;
        Ok(Self { object, values })
    }

    fn invalid(&self, field: &'static str) -> WireError {
        WireError::InvalidField {
            object: self.object,
            field,
        }
    }

    fn take(&mut self, field: &'static str) -> Result<Value, WireError> {
        self.values.remove(field).ok_or(WireError::MissingField {
            object: self.object,
            field,
        })
    }

    fn read<T>(
        &mut self,
        field: &'static str,
        convert: impl FnOnce(Value) -> Option<T>,
    ) -> Result<T, WireError> {
        convert(self.take(field)?).ok_or_else(|| self.invalid(field))
    }

    fn optional<T>(
        &mut self,
        field: &'static str,
        convert: impl FnOnce(Value) -> Option<T>,
    ) -> Result<Option<T>, WireError> {
        self.values
            .remove(field)
            .map(|value| convert(value).ok_or_else(|| self.invalid(field)))
            .transpose()
    }

    fn version(&mut self) -> Result<WireVersion, WireError> {
        self.read("schema_version", |value| WireVersion::new(number(value)?))
    }

    fn finish(self) -> Result<(), WireError> {
        if let Some((field, _)) = self.values.into_iter().next() {
            return Err(WireError::UnknownField {
                object: self.object,
                field,
            });
        }
        Ok(())
    }
}

fn object<const N: usize>(fields: [(&str, Value); N]) -> Object {
    fields
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

fn put<T: Into<Value>>(object: &mut Object, key: &str, value: Option<T>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), value.into());
    }
}

fn as_object(value: Value) -> Option<Object> {
    match value {
        Value::Object(value) => Some(value),
        _ => None,
    }
}

fn string(value: Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value),
        _ => None,
    }
}

fn nonempty(value: Value) -> Option<String> {
    string(value).filter(|value| !value.is_empty())
}

fn timestamp(value: Value) -> Option<String> {
    string(value).filter(|value| chrono::DateTime::parse_from_rfc3339(value).is_ok())
}

fn number(value: Value) -> Option<Number> {
    match value {
        Value::Number(value) => Some(value),
        _ => None,
    }
}

fn is_integer_at_least(value: &Number, minimum: f64) -> bool {
    value
        .as_f64()
        .is_some_and(|value| value >= minimum && value.fract() == 0.0)
}

fn revision(value: Value) -> Option<Number> {
    number(value).filter(|value| is_integer_at_least(value, 1.0))
}

fn array(value: Value) -> Option<Vec<Value>> {
    match value {
        Value::Array(value) => Some(value),
        _ => None,
    }
}

fn strings(value: Value) -> Option<Vec<String>> {
    array(value)?.into_iter().map(string).collect()
}

fn namespace(key: &str) -> bool {
    let mut bytes = key.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn is_extension_map(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|map| map.keys().all(|key| namespace(key)))
}

fn extensions(value: Value) -> Option<Object> {
    is_extension_map(&value).then(|| as_object(value)).flatten()
}

fn only_keys(object: &Object, keys: &[&str], name: &'static str) -> Result<(), WireError> {
    if let Some(field) = object.keys().find(|key| !keys.contains(&key.as_str())) {
        return Err(WireError::UnknownField {
            object: name,
            field: field.clone(),
        });
    }
    Ok(())
}

fn validate_anchor(value: &Value) -> Result<(), WireError> {
    const NAME: &str = "source_anchor";
    let anchor = value.as_object().ok_or(WireError::ExpectedObject(NAME))?;
    only_keys(
        anchor,
        &[
            "schema_version",
            "source_id",
            "span",
            "label",
            "mime_type",
            "extensions",
        ],
        NAME,
    )?;
    let checks = [
        (
            "schema_version",
            anchor
                .get("schema_version")
                .and_then(Value::as_number)
                .is_some_and(|value| is_integer_at_least(value, 1.0)),
        ),
        (
            "source_id",
            anchor.get("source_id").is_some_and(Value::is_string),
        ),
        (
            "extensions",
            anchor.get("extensions").is_some_and(is_extension_map),
        ),
        ("label", anchor.get("label").is_none_or(Value::is_string)),
        (
            "mime_type",
            anchor.get("mime_type").is_none_or(Value::is_string),
        ),
    ];
    for (field, valid) in checks {
        if !valid {
            return Err(WireError::InvalidField {
                object: NAME,
                field,
            });
        }
    }
    if let Some(span) = anchor.get("span") {
        let span = span
            .as_object()
            .ok_or(WireError::ExpectedObject("source_anchor.span"))?;
        only_keys(span, &["start", "end"], "source_anchor.span")?;
        for field in ["start", "end"] {
            if !span.get(field).is_some_and(Value::is_number) {
                return Err(WireError::InvalidField {
                    object: "source_anchor.span",
                    field,
                });
            }
        }
    }
    Ok(())
}

pub fn knowledge_entry_from_wire(
    value: Value,
) -> Result<(WireVersion, KnowledgeEntryIr), WireError> {
    let mut fields = Fields::new("knowledge_entry", value)?;
    let version = fields.version()?;
    let entry = KnowledgeEntryIr {
        entry_id: fields.read("entry_id", string)?,
        entry_type: fields.read("entry_type", string)?,
        canonical_name: fields.read("canonical_name", nonempty)?,
        status: fields.read("status", string)?,
        body: body_from_wire(fields.take("body")?)?,
        source_anchor: fields.values.remove("source_anchor"),
        revision: fields.optional("revision", |value| {
            number(value).filter(|value| is_integer_at_least(value, 0.0))
        })?,
        created_at: fields.optional("created_at", timestamp)?,
        updated_at: fields.optional("updated_at", timestamp)?,
        owner: fields.optional("owner", nonempty)?,
        disclosure: fields.optional("disclosure", nonempty)?,
        modules: fields
            .values
            .remove("modules")
            .map(modules_from_wire)
            .transpose()?,
        extensions: fields.read("extensions", extensions)?,
    };
    fields.finish()?;
    if let Some(anchor) = &entry.source_anchor {
        validate_anchor(anchor)?;
    }
    Ok((version, entry))
}

pub fn knowledge_entry_to_wire(version: WireVersion, entry: KnowledgeEntryIr) -> Value {
    let mut fields = object([
        ("schema_version", version.0.into()),
        ("entry_id", entry.entry_id.into()),
        ("entry_type", entry.entry_type.into()),
        ("canonical_name", entry.canonical_name.into()),
        ("status", entry.status.into()),
        ("body", body_to_wire(entry.body)),
        ("extensions", entry.extensions.into()),
    ]);
    put(&mut fields, "source_anchor", entry.source_anchor);
    put(&mut fields, "revision", entry.revision);
    put(&mut fields, "created_at", entry.created_at);
    put(&mut fields, "updated_at", entry.updated_at);
    put(&mut fields, "owner", entry.owner);
    put(&mut fields, "disclosure", entry.disclosure);
    put(&mut fields, "modules", entry.modules.map(modules_to_wire));
    fields.into()
}

fn body_from_wire(value: Value) -> Result<KnowledgeBody, WireError> {
    let mut fields = Fields::new("knowledge_entry.body", value)?;
    let body = KnowledgeBody {
        summary: fields.optional("summary", string)?,
        tags: fields.optional("tags", strings)?,
        attributes: fields.optional("attributes", array)?,
        state: fields.optional("state", as_object)?,
        computable: fields.optional("computable", as_object)?,
    };
    fields.finish()?;
    for value in body.attributes.iter().flatten() {
        const NAME: &str = "knowledge_entry.body.attributes[]";
        let attribute = value.as_object().ok_or(WireError::ExpectedObject(NAME))?;
        only_keys(
            attribute,
            &["trait_type", "value", "display_type", "max_value"],
            NAME,
        )?;
        let checks = [
            (
                "trait_type",
                attribute
                    .get("trait_type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty()),
            ),
            (
                "value",
                attribute.get("value").is_some_and(|value| {
                    value.is_string() || value.is_number() || value.is_boolean()
                }),
            ),
            (
                "display_type",
                attribute.get("display_type").is_none_or(Value::is_string),
            ),
            (
                "max_value",
                attribute.get("max_value").is_none_or(Value::is_number),
            ),
        ];
        for (field, valid) in checks {
            if !valid {
                return Err(WireError::InvalidField {
                    object: NAME,
                    field,
                });
            }
        }
    }
    Ok(body)
}

fn body_to_wire(body: KnowledgeBody) -> Value {
    let mut fields = Map::new();
    put(&mut fields, "summary", body.summary);
    put(&mut fields, "tags", body.tags);
    put(&mut fields, "attributes", body.attributes);
    put(&mut fields, "state", body.state);
    put(&mut fields, "computable", body.computable);
    fields.into()
}

fn modules_from_wire(value: Value) -> Result<KnowledgeModules, WireError> {
    const NAME: &str = "knowledge_entry.modules";
    let mut other = as_object(value).ok_or(WireError::ExpectedObject(NAME))?;
    if let Some((field, _)) = other
        .iter()
        .find(|(key, value)| !namespace(key) || !(value.is_object() || value.is_array()))
    {
        return Err(WireError::InvalidModule(field.clone()));
    }
    let cognition = other
        .remove("cognition")
        .map(cognition_from_wire)
        .transpose()?;
    Ok(KnowledgeModules { other, cognition })
}

fn cognition_from_wire(value: Value) -> Result<CognitionModule, WireError> {
    const NAME: &str = "knowledge_entry.modules.cognition";
    let mut fields = Fields::new(NAME, value.clone())?;
    let module = CognitionModule {
        records: fields.read("records", array)?,
        world_facts: fields.read("world_facts", array)?,
        extensions: fields.read("extensions", extensions)?,
    };
    check_cognition_members("cognition record", &module.records)?;
    check_cognition_members("cognition world_fact", &module.world_facts)?;
    check_cognition(NAME, spoke_operations::validate_cognition_module(&value))?;
    Ok(module)
}

fn check_cognition(
    name: &'static str,
    result: spoke_operations::SpokeResult<()>,
) -> Result<(), WireError> {
    match result {
        spoke_operations::SpokeResult::Ok(()) => Ok(()),
        spoke_operations::SpokeResult::Reject(error) => {
            Err(WireError::Cognition(name, error.message))
        }
    }
}

/// I-000024: every schema-declared cognition extension map — module, record and world
/// fact members and memory-content cognition records — follows the same namespace-keyed
/// rule. Checks only the `extensions` member itself; opaque extension values are never
/// descended into.
pub(super) fn check_cognition_members(
    object_name: &'static str,
    members: &[Value],
) -> Result<(), WireError> {
    for member in members {
        let Some(raw) = member.get("extensions") else {
            continue;
        };
        if extensions(raw.clone()).is_none() {
            return Err(WireError::InvalidField {
                object: object_name,
                field: "extensions",
            });
        }
    }
    Ok(())
}

fn modules_to_wire(modules: KnowledgeModules) -> Value {
    let mut fields = modules.other;
    put(
        &mut fields,
        "cognition",
        modules.cognition.map(|module| {
            Value::Object(object([
                ("records", module.records.into()),
                ("world_facts", module.world_facts.into()),
                ("extensions", module.extensions.into()),
            ]))
        }),
    );
    fields.into()
}
