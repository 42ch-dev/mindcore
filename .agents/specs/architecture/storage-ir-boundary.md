# Architecture: storage-independent IR boundary

Status: implemented — `crates/mindcore-base/src/ir.rs`
Source: plan `v02-embed-seam` (iteration `v02-embed`).

## Scope

The contract between Mindcore's storage backends and its protocol surface: the record model every backend reads and writes (Memory candidate, retained record and operation receipt; KnowledgeEntry and the cognition records it carries), and the repository boundary a backend implements. It does not add a port, move production callers, or replace SPOKE validation, canonical hashing or authorization.

The protocol adapter that converts wire types to this model and back is written once, above the repository; a backend must never re-implement reject mapping, idempotency probing, revision CAS or grant evaluation.

## Current state

`mindcore-base::ir` defines the record families and the wire codec; `crates/mindcore-base/tests/ir_roundtrip.rs` proves the fidelity invariant. `mindcore-sqlite` still persists SPOKE payload JSON directly and still implements `KnowledgeEntryPort` itself, so no backend currently reads or writes through the IR and no repository borrows a caller's transaction (`I-000020`). The Nexus memory schema mapping is likewise open (`I-000021`).

## Record and codec boundary

The IR exposes independently usable domain identity, placement, revision, state and payload fields; it does not hold an original wire envelope or a `spoke_schemas` object. Each family carries **every schema-valid field** of its mapped document, not only the fields today's SQLite model uses:

| IR | Wire document and required distinctions |
|---|---|
| `MemoryCandidateIr` | `MemoryCandidate`: pending vs consumed, candidate/space/subject identity, capture placement, proposed content, provenance and classification, all timestamps/revision, and consumed disposition/digest when present. |
| `MemoryRecordIr` | `RetainedRecord`: live vs tombstoned, record/space/subject/kind, scope, content or tombstone, provenance, and all timestamps/revision. SPOKE v0.16.1 permits the optional `import_ref` only on live records; tombstoned records reject it. Do not infer a standalone local candidate id from the retained-record id. |
| `ReviewReceiptIr` | `MemoryReceipt`: receipt, requester, space and operation identity, optional idempotency key, payload digest and timestamps. Pending carries neither result nor error nor completion time; committed carries its opaque content-free result; failed carries its error, not a result. This is distinct from the three-column local `review_receipts` row. |
| `KnowledgeEntryIr` | `KnowledgeEntry`: entry identity/type/name/status, complete body, source anchor, revision, ownership/disclosure, timestamps, extensions and all modules. Cognition records inside `modules.cognition.records` keep their complete assessments, references, provenance/evidence, timestamps and extensions in carrier order; `world_facts` is preserved separately. The derived `MindState` is never persisted. |

**Wire is the original JSON value**, before deserializing a generated struct. Version-specific conversion lives in `mindcore_base::ir::wire`; `schema_version` and schema-generated types stay out of IR records and repository signatures. The original positive `schema_version` is decoded and returned separately as a `WireVersion` codec value and passed back at encode; it is never silently set to `1`. Known fields live in IR fields; JSON values carry opaque/open payloads and fields whose nested representation the IR does not interpret. Reconstruct from the IR fields plus the codec version, never from a saved input object.

Ablation: the pre-typed JSON boundary is required because a generated default map or optional field can erase presence, and typed timestamps can erase the original value spelling. Opaque subtrees retain admitted fields unknown to the IR without adding a field per product namespace.

## Repository boundary

A future SPOKE adapter owns request validation, grant checks, replay decisions, revision CAS orchestration and reject mapping. `MemoryRepository` offers only the record operations used by capture/candidate/retain/recall. Its explicit `&mut Tx<'_>` is a borrow of an already-open backend transaction or read snapshot — not a pool, not optional. The integration caller owns acquisition, begin, commit and rollback.

All reads used to decide a write (authorization evidence, idempotency probe, revision/precondition reads), effect writes and the effect's receipt use that same transaction. Repositories enforce storage constraints but do not reapply protocol policy, start or settle transactions, or migrate. There is no autocommit overload.

A backend that owns its own pool cannot satisfy this contract; choosing the borrowed shape is a commitment that the store must not also open a transaction for the same logical effect.

Ablation: the caller's transaction borrow is required to keep admission, persisted effects and receipt atomic without a second pool or a nested commit.

## Invariants

- Preserve known values and all permitted unknown namespace/member names and nested values, including JSON type, in `extensions`, `modules` and opaque content and receipt result/error details where the schema allows them: `modules`, when present, is a namespace-keyed object; individual module values may be objects or arrays where permitted. Never filter, stringify, rename or reinterpret them. Unknown members of **closed** objects are not valid wire and must be refused, not discarded or silently passed through. Closed cognition record/module shapes still receive their pinned validators; the open module namespace does not make `cognition` itself open.
- Preserve all array order, including duplicates where valid, at every depth: knowledge body tags/attributes, cognition `records`/`world_facts`/evidence, Memory provenance and result arrays, opaque product arrays. No sorting or deduplication. JSON object key order, whitespace, escape spelling and number token formatting are not contractual; compare JSON values, not serialization bytes. Timestamp and identifier **strings** keep their original value, including precision and spelling.
- Preserve absence vs an explicit valid empty map/array/string, zero or false; preserve present JSON null wherever permitted (for example inside opaque JSON), and reject it where the schema forbids it. Do not materialize defaults or use null as a stand-in for omission: absent `modules` is not `modules: {}`, absent `revision` is not `revision: 0`, absent receipt `idempotency_key`/result/error is not a null field.
- Compare original fixture JSON with rebuilt JSON for each wire family and each status branch, with a standalone cognition record exercised through a KnowledgeEntry carrier. Include unknown namespaces and deep sentinel values; ordered non-sorted arrays; paired absent/present-empty/present-value fixtures; a legitimate explicit null; and closed-object negatives. Assert independently that the IR exposes the decoded identity, state and representative payload, and that changing a real IR field changes the corresponding wire member without touching neighbours. Do not build expected JSON by serializing a typed value first. Dropping an open field, defaulting an absent field or reordering an array must turn at least one test red.

## Where it lives

- Contract implementation: `crates/mindcore-base/src/ir.rs`, `crates/mindcore-base/src/ir/wire.rs`, `crates/mindcore-base/src/ir/wire/memory.rs`; round-trip proof: `crates/mindcore-base/tests/ir_roundtrip.rs`.
- The existing `MemoryStore` and the SQLite adapter remain separate: the SQLite path does not read or write through the IR yet, and no repository borrows a caller's transaction. Both are open (`I-000020`, `I-000021`).

## Change obligations

Changing a record family, the codec rule or the repository transaction expectation requires updating this page and the round-trip tests together. Adding a backend requires a repository implementation only; if it needs a rule this page does not state, that is a contract change, not a backend decision.
