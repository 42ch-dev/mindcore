# Concepts

Mindcore separates three kinds of state that agent systems usually blur. This page explains the
model those types encode; the normative contract lives in
[`.agents/specs/architecture/mind-model.md`](../.agents/specs/architecture/mind-model.md).

## Three records, three questions

| Record | Answers | Changes when |
| --- | --- | --- |
| Memory record | What did the agent hear or observe? | A candidate is reviewed into a record |
| `KnowledgeEntry` | What did it adopt for itself, from where? | An explicit create, update or adoption |
| Cognition record | What does it currently judge true, on what basis? | An explicit carrier append |

Each layer cites the one above instead of copying it. A belief cites memory records; an adopted
entry cites a source scope, entry and revision. Nothing is inferred at read time.

Reads do not promote state: reading a memory does not create knowledge, reading a shared entry
does not change an adopted copy, and reading a carrier does not derive a stored judgment. Only an
explicit write moves data between layers.

## Memory

### Candidates and records

`memory.capture` stages a **candidate** under a space, subject and placement. A candidate is
pending: recall does not read it, and it is not yet a record. Review consumes it exactly once:

- `promote` projects one retained record,
- `fragment` projects several,
- `drop` projects none,
- `dismiss` consumes it without a record.

A retained record keeps the candidate's placement (`space`, `subject`, context) and the caller's
attribution. Recall reads live records only, ordered by `created_at DESC, record_id`.

### Idempotency and replay

Every mutation carries an idempotency key. A replay returns the recorded outcome instead of
repeating the effect, and a second review of a consumed candidate is refused. A replayed failure
re-answers its own code, message and retry class; an unrecognized recorded code becomes one
generic terminal `InternalError` — the same key can only replay that failure, so it never suggests
the outcome could differ on a second attempt.

### Recall is a budget

`memory.recall` walks the ranked remaining records once and charges each emitted record's
serialized `content` object only while `max_bytes` is set; `max_items` bounds the count. A record
is never split. Where the overflow lands decides what happens to it:

- A first record that does not fit is served whole on the current page and the cursor advances.
- A later overflow stops the page before it and consumes nothing; the cursor resumes at that
  record, and `overflow_record_id` names it.

## Knowledge

### Collections and scope

A `KnowledgeEntry` lives in a **collection**. A port is built for one collection
(`MindStore::knowledge(KnowledgeScope::new("wren"))`) and every write binds it; no API accepts a
caller-supplied collection, `owner` or `viewpoint` as a selector. Two collections may hold the
same raw `entry_id` and stay resolvable only through their own ports.

`CallerId` is attribution (`requester`, `updated_by`), not mental state and not admission. It is a
plain string the embedding adapter supplies.

### Revisions

A create passes `expected_base_revision = None` only when the entry is absent. An update passes
`Some(revision)` only when the stored revision equals it. The adapter assigns `1` on create and
`stored + 1` on update into both the column and the payload. A refused write leaves the row
untouched.

### History and retirement

`history_floor_revision` is the persisted replay floor: the first revision above the newest
deleted preimage, defaulting to `1` so a fresh row reads as "nothing pruned". Revision replay
consults that floor before checkpoint selection, so a pruned preimage never answers with current
content; a positive request below the floor is refused as truncated (`HISTORY_TRUNCATED`).

Retirement is a marker, not a delete. A retired entry resolves to its live terminal through the
retirement chain, bounded at 32 edges. A chain that cannot resolve — a cycle, an ambiguous merge,
a missing hop, too many edges — refuses with `SOURCE_RETIRED` and `details.retirement_chain`.

### Adoption and provenance

Adoption copies an entry from one shared collection into the collection the knowledge face serves.
The copy carries an **adoption anchor** naming the source scope, entry and revision, and the anchor
is immutable: a put that would detach a stored anchor is refused with `ADOPTION_ANCHOR_IMMUTABLE`.

Adoption resolves the source through its retirement chain before writing, so the anchor names the
live terminal. A repeat is answered `already_present` with the stored copy only when its anchor
names the same source revision; a copy adopted at another revision answers `REVISION_CONFLICT` with
`details.adopted_revision`. The copy inherits the source's governance; the contract's
`owner`/`disclosure` reset is not applied.

## Cognition

### The carrier

Current judgment is a per-record row set under one holder `KnowledgeEntry`, called the **carrier**.
`cognition_records` stores one validated record per row, keyed `(collection, entry_id,
append_seq)`. The carrier header's `cognition_head_seq` is the single current-judgment pointer;
`NULL` means no records yet. Appending inserts one row and advances the head plus the outer
revision in the caller's transaction.

A present cognition module is validated at the single persistence boundary
(`put_knowledge_entry`) before the write reservation, and every appended record is validated by
the storage append on the caller's transaction. No path can commit an invalid carrier. A present
`order` must be written as a JSON integer: the pinned `BeliefOrder` type reads `i64` only, and the
generic IR codec is not narrowed.

### `MindState` is derived

`derive_mind_state` projects the carrier into `MindStateView`. Records are reconstructed from one
SQLite snapshot in `append_seq` order; an absent module stays absent and a present empty array
stays empty. The projection is never a second stored copy, so it cannot drift from the records.

The deliberation workflow turns one attempt into a SPOKE `Judgment`, validated with the released
validator before it is returned. Its `judgment_id` is `jd_` plus the JCS SHA-256 of the attempt's
identity and verdict: equal attempt content yields an equal id, and another branch yields another.

## Trust boundary

- **The host authenticates.** Mindcore never reads a principal from the wire; the host's admission
  maps an authenticated peer to a `Principal`, and a served call names its own Mind, never a
  collection.
- **Admission precedes existence.** A call admits one Mind before it reads anything: an unknown
  Mind id, a Mind the caller holds no live grant in, and a frozen Mind it holds none in answer one
  `NOT_AUTHORIZED` with no details, so the refusal cannot report whether the Mind exists.
- **Ports refuse, they do not filter.** No result mixes two Minds: a call reads and writes only the
  collection its admitted Mind's registry row names, and a refusal names nothing outside it.
- **One store per file.** Each opened `MindStore` owns one pool. Two stores on one file are
  unsupported, not refused.
- **A mutating operation takes the write reservation first.** `BEGIN IMMEDIATE` precedes any claim
  or revision read.

## What Mindcore does not do

It opens no listener, transport or identity; it ships no UI; it does not move existing data
between stores or providers. Those belong to the host, and the runtime refuses to invent them.
