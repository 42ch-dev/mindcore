//! Request admission helpers. The pinned DTO proves the original JSON matches
//! the operation contract; every value the adapter stores or digests comes from
//! the original `Value`, never from the DTO's re-serialization.

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::admission::Scope;
use crate::reject::{AResult, Reject};

/// Deserialize the original request into its pinned DTO.
pub(crate) fn decode<T: DeserializeOwned>(raw: &Value, what: &str) -> AResult<T> {
    serde_json::from_value(raw.clone()).map_err(|error| Reject::invalid(format!("{what}: {error}")))
}

pub(crate) fn object<'a>(value: &'a Value, what: &str) -> AResult<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| Reject::invalid(format!("{what} must be an object")))
}

/// A required non-empty string member.
pub(crate) fn required<'a>(object: &'a Map<String, Value>, key: &str) -> AResult<&'a str> {
    match object.get(key).and_then(Value::as_str) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(Reject::invalid(format!("{key} is required"))),
    }
}

pub(crate) fn optional(object: &Map<String, Value>, key: &str) -> AResult<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        _ => Err(Reject::invalid(format!("{key} must be a non-empty string"))),
    }
}

/// A closed object member set, minus `tag`: every other key must be in
/// `allowed`, mirroring the pinned DTO's `deny_unknown_fields`.
pub(crate) fn closed(
    object: &Map<String, Value>,
    tag: &str,
    allowed: &[&str],
    what: &str,
) -> AResult<()> {
    for key in object.keys() {
        if key != tag && !allowed.contains(&key.as_str()) {
            return Err(Reject::invalid(format!("{what} must not carry {key}")));
        }
    }
    Ok(())
}

pub(crate) fn strings(object: &Map<String, Value>, key: &str) -> AResult<Vec<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| Reject::invalid(format!("{key} must name strings")))
            })
            .collect(),
        Some(_) => Err(Reject::invalid(format!("{key} must be an array"))),
    }
}

/// A required revision (`minItems: 1` style positive integer).
pub(crate) fn revision(object: &Map<String, Value>, key: &str) -> AResult<u64> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .filter(|value| *value >= 1)
        .ok_or_else(|| Reject::invalid(format!("{key} must be a positive integer")))
}

/// A placement member (`scope` / `capture_scope` / `target_scope`).
pub(crate) fn placement(object: &Map<String, Value>, key: &str) -> AResult<Scope> {
    let value = object
        .get(key)
        .ok_or_else(|| Reject::invalid(format!("{key} is required")))?;
    Scope::from_json(value, key)
}

/// The §6.12 read selection from its wire member.
pub(crate) fn selection(object: &Map<String, Value>, key: &str) -> AResult<PlacementSelectionWire> {
    let value = object
        .get(key)
        .ok_or_else(|| Reject::invalid(format!("{key} is required")))?;
    PlacementSelectionWire::from_json(value)
}

/// The wire selection shape, decoded once and reused by admission and storage.
pub(crate) struct PlacementSelectionWire {
    pub(crate) context_ids: Vec<String>,
    pub(crate) include_shared: bool,
    pub(crate) mode: SelectionMode,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectionMode {
    SubjectAll,
    SharedOnly,
    SelectedContexts,
}

impl PlacementSelectionWire {
    pub(crate) fn from_json(value: &Value) -> AResult<Self> {
        let object = object(value, "selection")?;
        match object.get("mode").and_then(Value::as_str) {
            Some("subject_all") => Ok(Self {
                context_ids: Vec::new(),
                include_shared: false,
                mode: SelectionMode::SubjectAll,
            }),
            Some("shared_only") => Ok(Self {
                context_ids: Vec::new(),
                include_shared: false,
                mode: SelectionMode::SharedOnly,
            }),
            Some("selected_contexts") => Ok(Self {
                context_ids: strings(object, "context_ids")?,
                include_shared: object
                    .get("include_shared")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                mode: SelectionMode::SelectedContexts,
            }),
            _ => Err(Reject::invalid("selection.mode is invalid")),
        }
    }

    pub(crate) fn ir(&self) -> mindcore_base::ir::PlacementSelection {
        use mindcore_base::ir::PlacementSelection;
        match self.mode {
            SelectionMode::SubjectAll => PlacementSelection::SubjectAll,
            SelectionMode::SharedOnly => PlacementSelection::SharedOnly,
            SelectionMode::SelectedContexts => PlacementSelection::SelectedContexts {
                context_ids: self
                    .context_ids
                    .iter()
                    .map(|id| mindcore_base::ids::ContextId::new(id.clone()))
                    .collect(),
                include_shared: self.include_shared,
            },
        }
    }

    /// The provenance filter (`source_context_ids`) a read carries, when present.
    pub(crate) fn filter(object: &Map<String, Value>, key: &str) -> AResult<Option<Vec<String>>> {
        match object.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => {
                let filter = object_of(value, key)?;
                Ok(Some(strings(filter, "source_context_ids")?))
            }
        }
    }
}

fn object_of<'a>(value: &'a Value, what: &str) -> AResult<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| Reject::invalid(format!("{what} must be an object")))
}
