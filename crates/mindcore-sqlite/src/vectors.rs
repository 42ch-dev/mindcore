//! Retained Memory vectors under one embedding identity: identity normalization, the vector
//! codec, and the scoped lookup/upsert plus bulk retirement over `memory_embedding_vectors`.

use std::mem::size_of;

use serde_json::json;
use spoke_operations::{jcs_sha256_hex, spoke_reject, SpokeRejectCode, SpokeResult};
use sqlx::{Executor, Sqlite};

use crate::knowledge::internal;

/// The storage identity of one serving embedder: RFC 8785 JCS SHA-256 over the four identity
/// fields, as 64 lowercase hex.
///
/// The strings are the host's exact bytes — no trimming, case folding or Unicode normalization —
/// so only a byte-identical identity shares a partition.
pub fn identity_hash(
    provider_id: &str,
    model_id: &str,
    model_version: &str,
    dim: usize,
) -> SpokeResult<String> {
    if dim == 0 {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "embedding identity has no dimension",
            None,
        );
    }
    if dim.checked_mul(size_of::<f32>()).is_none() {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            format!("dimension {dim} has no representable vector blob length"),
            None,
        );
    }
    jcs_sha256_hex(&json!({
        "dim": dim,
        "model_id": model_id,
        "model_version": model_version,
        "provider_id": provider_id,
    }))
}

/// Encodes finite `f32` components as little-endian IEEE 754 binary32, with no header and no
/// quantization.
pub fn encode_vector(vector: &[f32]) -> SpokeResult<Vec<u8>> {
    if let SpokeResult::Reject(reject) = checked_components(vector) {
        return SpokeResult::Reject(reject);
    }
    let mut blob = Vec::with_capacity(vector.len() * 4);
    for component in vector {
        blob.extend_from_slice(&component.to_le_bytes());
    }
    SpokeResult::Ok(blob)
}

/// Decodes one stored vector, requiring `dim` components and well-formed values.
pub fn decode_vector(blob: &[u8], dim: usize) -> SpokeResult<Vec<f32>> {
    let expected = dim.checked_mul(4);
    if expected != Some(blob.len()) {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            format!(
                "vector blob holds {} bytes, not the {dim} little-endian f32 components expected",
                blob.len()
            ),
            None,
        );
    }
    let (components, _) = blob.as_chunks::<4>();
    let vector: Vec<f32> = components
        .iter()
        .map(|component| f32::from_le_bytes(*component))
        .collect();
    if let SpokeResult::Reject(reject) = checked_components(&vector) {
        return SpokeResult::Reject(reject);
    }
    SpokeResult::Ok(vector)
}

/// A vector must be nonempty, finite and not all zero.
fn checked_components(vector: &[f32]) -> SpokeResult<()> {
    if vector.is_empty() {
        return spoke_reject(SpokeRejectCode::InvalidInput, "vector is empty", None);
    }
    if vector.iter().any(|component| !component.is_finite()) {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "vector holds a non-finite component",
            None,
        );
    }
    if vector.iter().all(|component| *component == 0.0) {
        return spoke_reject(SpokeRejectCode::InvalidInput, "vector is all zero", None);
    }
    SpokeResult::Ok(())
}

/// One record's vector under one identity, replacing whatever revision that row held: the
/// revision is a freshness field, not part of the key.
pub async fn upsert_record_vector<'e, E>(
    executor: E,
    record_id: &str,
    identity_hash: &str,
    record_revision: u64,
    vector: &[f32],
) -> SpokeResult<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    let revision = match i64::try_from(record_revision) {
        Ok(revision) if revision > 0 => revision,
        _ => {
            return spoke_reject(
                SpokeRejectCode::InvalidInput,
                format!("record revision {record_revision} is not a positive integer"),
                None,
            )
        }
    };
    let blob = match encode_vector(vector) {
        SpokeResult::Ok(blob) => blob,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let written = sqlx::query(
        "INSERT INTO memory_embedding_vectors \
         (record_id, identity_hash, record_revision, dim, vector) VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT (record_id, identity_hash) DO UPDATE SET \
         record_revision = excluded.record_revision, dim = excluded.dim, vector = excluded.vector",
    )
    .bind(record_id)
    .bind(identity_hash)
    .bind(revision)
    .bind(i64::try_from(vector.len()).expect("a slice length fits an i64"))
    .bind(blob)
    .execute(executor)
    .await;
    match written {
        Ok(_) => SpokeResult::Ok(()),
        Err(error) => internal(format!("record vector could not be written: {error}")),
    }
}

/// The stored vectors for a page's `(record_id, revision)` pairs under one identity, in the same
/// order.
///
/// One bounded statement reads the whole page through the identity index; a row is a hit only when
/// it still carries the pair's revision and the serving dimension — any mismatch is a miss, not a
/// usable approximation.
pub async fn read_record_vectors<'e, E>(
    executor: E,
    identity_hash: &str,
    page: &[(&str, u64)],
    dim: usize,
) -> SpokeResult<Vec<Option<Vec<f32>>>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let Some(stored_dim) = i64::try_from(dim).ok().filter(|dim| *dim > 0) else {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            format!("dimension {dim} is not a positive integer"),
            None,
        );
    };
    let mut revisions = Vec::with_capacity(page.len());
    for (_, revision) in page {
        match i64::try_from(*revision) {
            Ok(revision) if revision > 0 => revisions.push(revision),
            _ => {
                return spoke_reject(
                    SpokeRejectCode::InvalidInput,
                    format!("record revision {revision} is not a positive integer"),
                    None,
                )
            }
        }
    }
    if page.is_empty() {
        return SpokeResult::Ok(Vec::new());
    }
    let ids: Vec<&str> = page.iter().map(|(record_id, _)| *record_id).collect();
    let ids = match serde_json::to_string(&ids) {
        Ok(ids) => ids,
        Err(error) => return internal(format!("page ids could not be encoded: {error}")),
    };
    let stored: Result<Vec<(String, i64, Vec<u8>)>, sqlx::Error> = sqlx::query_as(
        "SELECT record_id, record_revision, vector FROM memory_embedding_vectors \
         WHERE identity_hash = ?1 AND dim = ?2 \
         AND record_id IN (SELECT value FROM json_each(?3))",
    )
    .bind(identity_hash)
    .bind(stored_dim)
    .bind(ids)
    .fetch_all(executor)
    .await;
    let stored = match stored {
        Ok(rows) => rows,
        Err(error) => return internal(format!("record vectors could not be read: {error}")),
    };
    let mut found: std::collections::HashMap<String, (i64, Vec<u8>)> = stored
        .into_iter()
        .map(|(record_id, revision, blob)| (record_id, (revision, blob)))
        .collect();
    let mut vectors = Vec::with_capacity(page.len());
    for ((record_id, _), revision) in page.iter().zip(revisions) {
        let vector = match found.remove(*record_id) {
            None => None,
            Some((stored_revision, _)) if stored_revision != revision => None,
            Some((_, blob)) => match decode_vector(&blob, dim) {
                SpokeResult::Ok(vector) => Some(vector),
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            },
        };
        vectors.push(vector);
    }
    SpokeResult::Ok(vectors)
}

/// Deletes one retired identity's whole partition through the identity index.
///
/// The storage layer neither drains nor blocks a fill already in flight for that identity: a fill
/// that commits afterwards re-creates rows, so a caller that needs the partition to stay gone
/// quiesces those fills first. Pass the trusted retired hash, never a request value; other
/// identities and their rows are untouched.
pub async fn delete_identity_vectors<'e, E>(executor: E, identity_hash: &str) -> SpokeResult<u64>
where
    E: Executor<'e, Database = Sqlite>,
{
    let deleted = sqlx::query("DELETE FROM memory_embedding_vectors WHERE identity_hash = ?1")
        .bind(identity_hash)
        .execute(executor)
        .await;
    match deleted {
        Ok(deleted) => SpokeResult::Ok(deleted.rows_affected()),
        Err(error) => internal(format!("identity vectors could not be deleted: {error}")),
    }
}

/// Deletes every stored vector of one record: a tombstone keeps no vector, so the record's rows
/// are pruned inside the tombstone's own transaction.
///
/// One statement over the `(record_id, identity_hash)` primary key; other records' rows and the
/// other identities of this record are untouched.
pub async fn delete_record_vectors<'e, E>(executor: E, record_id: &str) -> SpokeResult<u64>
where
    E: Executor<'e, Database = Sqlite>,
{
    let deleted = sqlx::query("DELETE FROM memory_embedding_vectors WHERE record_id = ?1")
        .bind(record_id)
        .execute(executor)
        .await;
    match deleted {
        Ok(deleted) => SpokeResult::Ok(deleted.rows_affected()),
        Err(error) => internal(format!("record vectors could not be deleted: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
        match result {
            SpokeResult::Ok(value) => value,
            SpokeResult::Reject(reject) => panic!("{context}: rejected with {reject:?}"),
        }
    }

    fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>) -> SpokeRejectCode {
        match result {
            SpokeResult::Ok(value) => panic!("expected a rejection, got {value:?}"),
            SpokeResult::Reject(reject) => reject.code,
        }
    }

    fn hash(provider_id: &str, model_id: &str, model_version: &str, dim: usize) -> String {
        expect_ok(
            identity_hash(provider_id, model_id, model_version, dim),
            "identity hash",
        )
    }

    #[test]
    fn identity_hash_is_stable_and_separates_every_field() {
        let base = hash("provider", "model", "version", 384);
        assert_eq!(base.len(), 64);
        // Pinned to the storage contract's sample identity: the digest of the exact JCS bytes
        // `{"dim":384,"model_id":"model","model_version":"version","provider_id":"provider"}`.
        assert_eq!(
            base,
            "c7080bec0470e074e297630e4f9124cf56d6510dad8d726152ba1b8ebcdf4fc4"
        );
        assert!(base
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)));
        assert_eq!(base, hash("provider", "model", "version", 384));

        for other in [
            hash("provider2", "model", "version", 384),
            hash("provider", "model2", "version", 384),
            hash("provider", "model", "version2", 384),
            hash("provider", "model", "version", 1024),
            // Exact bytes: no trimming, case folding or normalization.
            hash(" provider", "model", "version", 384),
        ] {
            assert_ne!(base, other);
        }
        assert_eq!(
            expect_reject(identity_hash("provider", "model", "version", 0)),
            SpokeRejectCode::InvalidInput
        );
    }

    #[test]
    fn identity_hash_bounds_dimension_by_representable_blob_length() {
        let largest = usize::MAX / size_of::<f32>();
        assert_eq!(largest.checked_mul(4), Some(largest * 4));
        let bounded = hash("provider", "model", "version", largest);
        assert_eq!(bounded.len(), 64);

        // One past the bound: `4 * dim` no longer fits a `usize`, so no vector can hold it.
        let beyond = largest + 1;
        assert_eq!(beyond.checked_mul(4), None);
        assert_eq!(
            expect_reject(identity_hash("provider", "model", "version", beyond)),
            SpokeRejectCode::InvalidInput
        );
    }

    #[test]
    fn vector_codec_round_trips_component_bits() {
        let vector = [0.5f32, -1.0, f32::MIN_POSITIVE, 3.25];
        let blob = expect_ok(encode_vector(&vector), "encode");
        assert_eq!(blob.len(), vector.len() * 4);
        assert_eq!(blob[0..4], 0.5f32.to_le_bytes());

        let decoded = expect_ok(decode_vector(&blob, vector.len()), "decode");
        assert_eq!(decoded, vector);
        for (left, right) in decoded.iter().zip(&vector) {
            assert_eq!(left.to_bits(), right.to_bits());
        }
    }

    #[test]
    fn vector_codec_rejects_malformed_vectors() {
        for vector in [
            vec![],
            vec![0.0, 0.0],
            vec![1.0, f32::NAN],
            vec![f32::INFINITY],
        ] {
            assert_eq!(
                expect_reject(encode_vector(&vector)),
                SpokeRejectCode::InvalidInput,
                "encode {vector:?}"
            );
        }
        let blob = expect_ok(encode_vector(&[1.0, 2.0]), "encode");
        assert_eq!(
            expect_reject(decode_vector(&blob, 3)),
            SpokeRejectCode::InvalidInput
        );
        assert_eq!(
            expect_reject(decode_vector(&blob[..7], 2)),
            SpokeRejectCode::InvalidInput
        );
        assert_eq!(
            expect_reject(decode_vector(&[0u8; 8], 2)),
            SpokeRejectCode::InvalidInput
        );
    }

    /// One page read through the batch lookup, as the binding uses it.
    async fn page_vectors(
        store: &crate::MindStore,
        identity: &str,
        page: &[(&str, u64)],
        dim: usize,
    ) -> Vec<Option<Vec<f32>>> {
        expect_ok(
            read_record_vectors(&store.pool(), identity, page, dim).await,
            "read page",
        )
    }

    /// The table holds one row per record and identity, replaces the revision, and the identity
    /// index clears exactly one retired partition.
    #[tokio::test]
    async fn store_replaces_revisions_and_retires_one_identity() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = crate::MindStore::open(&dir.path().join("mindcore.db"))
            .await
            .expect("open store");
        let active = hash("provider", "model", "version", 2);
        let retired = hash("provider", "model", "old-version", 2);

        expect_ok(
            upsert_record_vector(&store.pool(), "rec_1", &active, 1, &[1.0, 2.0]).await,
            "first write",
        );
        assert_eq!(
            page_vectors(&store, &active, &[("rec_1", 1)], 2).await,
            vec![Some(vec![1.0, 2.0])]
        );
        // A stale revision is a miss, not an approximation.
        assert_eq!(
            page_vectors(&store, &active, &[("rec_1", 2)], 2).await,
            vec![None]
        );

        expect_ok(
            upsert_record_vector(&store.pool(), "rec_1", &active, 2, &[3.0, 4.0]).await,
            "replace revision",
        );
        assert_eq!(
            page_vectors(&store, &active, &[("rec_1", 2)], 2).await,
            vec![Some(vec![3.0, 4.0])]
        );
        assert_eq!(
            page_vectors(&store, &active, &[("rec_1", 1)], 2).await,
            vec![None]
        );

        // Another identity keeps its own partition.
        expect_ok(
            upsert_record_vector(&store.pool(), "rec_1", &retired, 2, &[3.0, 4.0]).await,
            "write retired identity",
        );
        assert_eq!(
            expect_ok(
                delete_identity_vectors(&store.pool(), &retired).await,
                "retire"
            ),
            1
        );
        assert_eq!(
            page_vectors(&store, &retired, &[("rec_1", 2)], 2).await,
            vec![None]
        );
        assert_eq!(
            page_vectors(&store, &active, &[("rec_1", 2)], 2).await,
            vec![Some(vec![3.0, 4.0])]
        );

        store.close().await;
    }

    /// One page read keeps the exact per-record hit semantics in page order: a current row hits,
    /// a stale revision, another identity, another dimension and an absent row are misses.
    #[tokio::test]
    async fn page_lookup_keeps_exact_hit_semantics() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = crate::MindStore::open(&dir.path().join("mindcore.db"))
            .await
            .expect("open store");
        let active = hash("provider", "model", "version", 2);
        let other = hash("provider", "model", "other", 2);
        for (record_id, revision) in [("rec_hit", 3), ("rec_stale", 1), ("rec_second", 2)] {
            expect_ok(
                upsert_record_vector(&store.pool(), record_id, &active, revision, &[1.0, 0.0])
                    .await,
                "write",
            );
        }
        expect_ok(
            upsert_record_vector(&store.pool(), "rec_other", &other, 3, &[1.0, 0.0]).await,
            "write another identity",
        );

        assert_eq!(
            page_vectors(
                &store,
                &active,
                &[
                    ("rec_hit", 3),
                    ("rec_stale", 2),
                    ("rec_other", 3),
                    ("rec_missing", 1),
                    ("rec_second", 2),
                ],
                2,
            )
            .await,
            vec![Some(vec![1.0, 0.0]), None, None, None, Some(vec![1.0, 0.0])],
            "one entry per page row, exact hits only"
        );
        // Another dimension under the same identity is a miss too.
        assert_eq!(
            page_vectors(&store, &active, &[("rec_hit", 3)], 3).await,
            vec![None]
        );

        store.close().await;
    }

    /// Direct SQL cannot store a non-integral revision or dimension, a non-integer type, or a
    /// blob whose length disagrees with the dimension.
    #[tokio::test]
    async fn schema_rejects_non_integral_and_mismatched_rows() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = crate::MindStore::open(&dir.path().join("mindcore.db"))
            .await
            .expect("open store");
        let rejected = [
            // Fractional revision: affinity keeps REAL 0.5.
            "INSERT INTO memory_embedding_vectors VALUES ('rec_2', 'id', 0.5, 2, x'0000000000000000')",
            // Fractional dimension: `length(vector) = 4 * 1.5` holds for six bytes.
            "INSERT INTO memory_embedding_vectors VALUES ('rec_2', 'id', 1, 1.5, x'000000000000')",
            // A blob whose length disagrees with the dimension.
            "INSERT INTO memory_embedding_vectors VALUES ('rec_2', 'id', 1, 2, x'00000000000000')",
            // Zero revision.
            "INSERT INTO memory_embedding_vectors VALUES ('rec_2', 'id', 0, 2, x'0000000000000000')",
        ];
        for sql in rejected {
            assert!(
                sqlx::query(sql).execute(&store.pool()).await.is_err(),
                "the schema must reject: {sql}"
            );
        }
        store.close().await;
    }
}
