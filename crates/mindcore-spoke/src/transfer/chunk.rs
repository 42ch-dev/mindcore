//! §6.13 chunking: element-atomic slices in the fixed array order, the ordinal-zero
//! header carrier, and the per-chunk digest.
//!
//! Elements are never split, so an element larger than `export_chunk_bytes` travels
//! alone in a chunk whose `bytes` exceeds the limit. A chunk is sized by the JCS
//! (RFC 8785) length of the whole chunk object, `bytes` and `chunk_digest` included.

use serde_json::{json, Map, Value};

use crate::reject::{AResult, Reject};

use super::bundle::digest_excluding;
use super::canonical_len;

/// Fixed concatenation order of the bundle arrays (§6.13, decision D8).
pub(super) const BUNDLE_ARRAY_FIELDS: [&str; 7] = [
    "subjects",
    "records",
    "profiles",
    "reflections",
    "candidates",
    "receipt_summaries",
    "grants",
];

/// Logical bundle scalars carried by the ordinal-zero header, `extensions` included
/// so a reassembled bundle hashes over exactly what was partitioned (decision D8).
const BUNDLE_HEADER_FIELDS: [&str; 9] = [
    "schema_version",
    "bundle_format_version",
    "memory_protocol_version",
    "export_id",
    "space_id",
    "created_at",
    "source",
    "digest",
    "extensions",
];

/// Partition one sealed bundle into ordered wire chunks.
pub(super) fn partition(bundle: &Value, limit: u64) -> AResult<Vec<Value>> {
    let object = bundle
        .as_object()
        .ok_or_else(|| Reject::internal("the export bundle is not an object"))?;
    let export_id = object
        .get("export_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Reject::internal("the export bundle carries no export_id"))?
        .to_string();
    // The sealed digest is recomputed here: partitioning an unsealed or tampered
    // bundle would emit chunks whose reassembly cannot verify.
    let bundle_digest = digest_excluding(bundle, "digest")?;
    if object.get("digest").and_then(Value::as_str) != Some(bundle_digest.as_str()) {
        return Err(Reject::internal("the export bundle digest does not verify"));
    }
    for field in BUNDLE_HEADER_FIELDS {
        if !object.contains_key(field) {
            return Err(Reject::internal(format!(
                "the export bundle omits header field {field}"
            )));
        }
    }
    let header: Map<String, Value> = BUNDLE_HEADER_FIELDS
        .iter()
        .map(|field| ((*field).to_string(), object[*field].clone()))
        .collect();

    // An array present but empty stays visible on ordinal zero; an absent one is
    // never emitted. `reflections` is the only member an export may omit.
    let mut stream: Vec<(&'static str, &Value)> = Vec::new();
    let mut empty_present: Vec<&'static str> = Vec::new();
    for field in BUNDLE_ARRAY_FIELDS {
        match object.get(field) {
            None => {}
            Some(Value::Array(items)) if items.is_empty() => empty_present.push(field),
            Some(Value::Array(items)) => {
                stream.extend(items.iter().map(|item| (field, item)));
            }
            Some(_) => {
                return Err(Reject::internal(format!(
                    "the export bundle member {field} is not an array"
                )))
            }
        }
    }

    // `chunk_count` is unknown while the last chunk is still being filled, so the
    // frame is measured with the widest count this stream could produce: the
    // measured size is then never smaller than the emitted one.
    let widest_count = decimal_digits(stream.len() as u64 + 1);
    let count_placeholder = 10u64.pow(widest_count as u32).saturating_sub(1);
    let widest_ordinal = decimal_digits(stream.len() as u64);
    let mut frames: Vec<Option<u64>> = vec![None; widest_ordinal as usize + 1];

    let element_bytes = stream
        .iter()
        .map(|(_, element)| canonical_len(element).map(|length| length as u64))
        .collect::<AResult<Vec<u64>>>()?;

    let mut groups: Vec<Vec<(&'static str, &Value)>> = Vec::new();
    let mut current: Vec<(&'static str, &Value)> = Vec::new();
    let mut counts = [0usize; BUNDLE_ARRAY_FIELDS.len()];
    let mut sums = [0u64; BUNDLE_ARRAY_FIELDS.len()];
    for ((field, element), element_len) in stream.into_iter().zip(element_bytes) {
        let index = array_field_index(field);
        current.push((field, element));
        counts[index] += 1;
        sums[index] += element_len;
        // The header chunk is measured even for its first element: the header alone
        // can push an element that fits an ordinary chunk past the limit. A group
        // past ordinal zero is measured only once it holds two elements, so an
        // oversized single element still travels alone.
        if current.len() > 1 || groups.is_empty() {
            let frame = frame_for(
                &mut frames,
                groups.len() as u64,
                &export_id,
                &bundle_digest,
                &header,
                &empty_present,
                count_placeholder,
            )?;
            if settle(frame + field_overhead(&counts, &sums)) > limit {
                current.pop();
                groups.push(std::mem::take(&mut current));
                counts = [0; BUNDLE_ARRAY_FIELDS.len()];
                sums = [0; BUNDLE_ARRAY_FIELDS.len()];
                current.push((field, element));
                counts[index] = 1;
                sums[index] = element_len;
            }
        }
    }
    // A bundle whose arrays are all empty still gets its one header-only chunk.
    groups.push(current);

    let chunk_count = groups.len() as u64;
    groups
        .iter()
        .enumerate()
        .map(|(ordinal, group)| {
            build_chunk(
                &export_id,
                &bundle_digest,
                &header,
                &empty_present,
                ordinal as u64,
                chunk_count,
                group,
            )
        })
        .collect()
}

/// JCS length of a chunk frame — scalars, the ordinal-zero header and empty arrays,
/// plus the placeholder `chunk_count` — with its own `bytes` digits excluded, so a
/// candidate group can settle them itself.
fn frame_for(
    frames: &mut [Option<u64>],
    ordinal: u64,
    export_id: &str,
    bundle_digest: &str,
    header: &Map<String, Value>,
    empty_present: &[&'static str],
    count: u64,
) -> AResult<u64> {
    // Only the digit width of the ordinal is serialized into the frame, so the
    // frame for ordinal 0 (which carries the header) and one per wider width
    // covers every chunk this bundle can produce.
    let slot = if ordinal == 0 {
        0
    } else {
        decimal_digits(ordinal) as usize
    };
    if let Some(frame) = frames[slot] {
        return Ok(frame);
    }
    let probe = if ordinal == 0 {
        0
    } else {
        10u64.pow(slot as u32 - 1)
    };
    let chunk = build_chunk(
        export_id,
        bundle_digest,
        header,
        empty_present,
        probe,
        count,
        &[],
    )?;
    let bytes = chunk["bytes"]
        .as_u64()
        .ok_or_else(|| Reject::internal("the export chunk carries no byte size"))?;
    let frame = bytes - decimal_digits(bytes);
    frames[slot] = Some(frame);
    Ok(frame)
}

/// JCS bytes one present array adds to the frame: the separating comma, the quoted
/// key, `:[`, the elements, their separators and `]`.
fn field_overhead(counts: &[usize], sums: &[u64]) -> u64 {
    BUNDLE_ARRAY_FIELDS
        .iter()
        .enumerate()
        .filter(|(index, _)| counts[*index] > 0)
        .map(|(index, field)| (field.len() as u64) + 5 + sums[index] + counts[index] as u64)
        .sum()
}

/// The chunk's own `bytes` includes its digits, so settle the fixed point.
fn settle(base: u64) -> u64 {
    let mut bytes = 0u64;
    loop {
        let length = base + decimal_digits(bytes);
        if length == bytes {
            return bytes;
        }
        bytes = length;
    }
}

/// Decimal width of a `bytes` or `chunk_ordinal` value; `bytes` stays below the
/// ECMAScript plain-decimal threshold, so its canonical form is plain digits.
fn decimal_digits(value: u64) -> u64 {
    if value == 0 {
        1
    } else {
        u64::from(value.ilog10()) + 1
    }
}

fn array_field_index(field: &'static str) -> usize {
    BUNDLE_ARRAY_FIELDS
        .iter()
        .position(|candidate| *candidate == field)
        .expect("every stream element names a bundle array field")
}

#[allow(clippy::too_many_arguments)]
fn build_chunk(
    export_id: &str,
    bundle_digest: &str,
    header: &Map<String, Value>,
    empty_present: &[&'static str],
    ordinal: u64,
    count: u64,
    group: &[(&'static str, &Value)],
) -> AResult<Value> {
    let mut chunk = Map::new();
    chunk.insert("export_id".to_string(), json!(export_id));
    chunk.insert("chunk_ordinal".to_string(), json!(ordinal));
    chunk.insert("chunk_count".to_string(), json!(count));
    chunk.insert("bundle_digest".to_string(), json!(bundle_digest));
    if ordinal == 0 {
        chunk.insert("header".to_string(), Value::Object(header.clone()));
    }
    for field in BUNDLE_ARRAY_FIELDS {
        let items: Vec<Value> = group
            .iter()
            .filter(|(element_field, _)| *element_field == field)
            .map(|(_, element)| (*element).clone())
            .collect();
        if !items.is_empty() {
            chunk.insert(field.to_string(), Value::Array(items));
        } else if ordinal == 0 && empty_present.contains(&field) {
            chunk.insert(field.to_string(), Value::Array(Vec::new()));
        }
    }
    // The digest is a fixed 64 characters, so its placeholder keeps the measured
    // size constant while `bytes` settles.
    chunk.insert("chunk_digest".to_string(), json!("0".repeat(64)));

    let mut bytes = 0u64;
    loop {
        chunk.insert("bytes".to_string(), json!(bytes));
        let length = canonical_len(&Value::Object(chunk.clone()))? as u64;
        if length == bytes {
            break;
        }
        bytes = length;
    }
    let digest = digest_excluding(&Value::Object(chunk.clone()), "chunk_digest")?;
    chunk.insert("chunk_digest".to_string(), json!(digest));
    Ok(Value::Object(chunk))
}
