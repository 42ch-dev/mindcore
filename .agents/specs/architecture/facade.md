# Architecture: the in-process facade over a Mind

## Scope

The convenience contract of `mindcore_runtime::Mind`, the library's in-process facade over one
Mind: which operations it offers, what each composes, and what it never does. It excludes the
wire contract of the served surface (`serving-seam.md`), per-principal serving routing
(`serving-principal-routing.md`) and the record and storage invariants behind every operation
(`mind-model.md`).

## Current state

### The handle

`Mind::open(store, caller, mind_id)` is one registry read — `minds::get` under the caller's live
grants — and a Mind the caller cannot see answers the registry's own `NotFound`. The handle keeps
the row that read answers — the id, the knowledge collection, the carrier entry id — and nothing
about grants: it caches no grant and acts on no status from the row, so a frozen Mind opens and
every later operation re-reads the live state on its own transaction. The handle is not a grant:
it confers no access beyond what the caller's live grants admit on every operation.

Two inputs are the host's and the handle never invents them, as on the served surface: the journal
publisher `review` settles through, supplied with `with_journal_publisher` (`journal_dir(root)` is
the one publisher the crate offers), and the providers `deliberate` reasons with, supplied with
`with_providers`. `review` without a publisher and `deliberate` without a reasoner are refused
with the binding's and the workflow's own refusals.

### The operations

| Operation | Composes | Transaction | Grants re-checked |
|---|---|---|---|
| `capture(&Capture)` | the memory adapter's `capture` with the Mind's id as `space_id` | one `BEGIN IMMEDIATE`, committed | the adapter's own, over the caller-named subject and placement |
| `review(candidate_id, decision, key)` | the candidate's revision and status read as metadata; `review_candidate` staged in one `BEGIN IMMEDIATE`; the staged intent read on it; publication off the async worker before the staging commits; `settle_journal` on a second transaction | the fold below | the adapter's own: `review` and `records_write` over the candidate |
| `recall(&Recall)` | the memory adapter's `recall` | one read transaction, rolled back | the adapter's own, over the caller-named selection |
| `knowledge_get(entry_id, revision)` | `read_entry`, or `read_entry_at_revision` when a revision is named, on the row's collection | one read transaction, rolled back | space-wide `knowledge_read` |
| `knowledge_put(entry, expected)` | `write_entry` — the one cognition validation boundary — on the row's collection | one `BEGIN IMMEDIATE`, committed | space-wide `knowledge_write` |
| `knowledge_list()` | `list_entries_in` on the row's collection | one read transaction, rolled back | space-wide `knowledge_read` |
| `cognition_read()` | the carrier `read_entry` on the row's collection, derived in the facade; nothing persisted | one read transaction, rolled back | space-wide `knowledge_read` |
| `deliberate(&Deliberation)` | `SpiMindWorkflows::deliberate` with no subject, the row's carrier as holder and the Mind's id as assessment context | the workflow's own | space-wide `recall`, with the space-level withhold |

No operation accepts a collection, an `owner` or a `viewpoint` from the caller. `Capture.subject`
and `Recall.subject` are the caller's and are never defaulted; `review` reads its subject from the
candidate's row. No step returns a facade-invented refusal: each is the registry's, the store's or
the adapter's own code, and a revision below an entry's floor answers the store's
`HistoryTruncated` and a moved revision its `StoredRevisionStale`.

### The `review` fold

`review` answers the settled receipt, not a stage. Inside one `BEGIN IMMEDIATE` it reads the
candidate's consumption status as metadata and then distinguishes fresh work from a replay.
Fresh work — a pending or absent candidate — reads the candidate through the adapter's
`get_candidate`, whose withhold refuses while any intent of the candidate's subject is
unsettled, the refusal fresh reviews always answered. A consumed candidate can only be a
replay or a retry of a still-pending receipt, and a receipt is not subject content: the
withhold must not gate it, so its `expected_revision` is derived as metadata — exactly one
less than the consumed revision, because consumption is a candidate's only revision bump.
That is what lets a settled key's replay reach the adapter's probe no matter what else in the
space is unsettled, whether another of the same subject's intents or another subject's in the
same space. Admission does not move: `review_candidate` authorizes the write, probes the
receipt first and applies every fresh-work gate on its own, and a replay is digest-identical,
so either path only rebuilds the original `expected_revision`.

The fold reads the staged intent on the same transaction and publishes the owed targets off
the async worker — the publisher writes and fsyncs synchronously, so it runs on the blocking
pool while nothing commits and unrelated executor work stays schedulable. The staging
transaction moves into the blocking task and comes back with the result, so a caller that
drops the review future can never release the writer before the started publication ends: a
same-key retry's transaction queues behind the first one's rollback, and two attempts never
publish side by side — **before the staging is durable**: a refused publication rolls everything back — no record row, no intent,
a pending candidate — and the same idempotency key can be repeated, with the publisher's own
`SpiError::Settlement` refusal traveling unchanged. Only after a successful publication does
the staging commit; settlement then runs on a second transaction, and any failure from there
answers `OUTCOME_UNCERTAIN` because durable work may be outstanding. A receipt already settled
is replayed as it is.

The `OUTCOME_UNCERTAIN` recovery contract: the caller repeats the review with the same
candidate, decision and idempotency key. When the probe meets the key's receipt still
pending, the repeat does not stop at the uncertain answer — it continues the fold: publishes
the intent's targets and runs the settlement leg, so a repeat actually settles the receipt
and answers it committed. `OUTCOME_UNCERTAIN` remains the answer for a settlement that fails
again, and a receipt already committed replays as the settled answer. The ordering differs from the served
surface deliberately: the served caller holds the receipt protocol that resolves
`OUTCOME_UNCERTAIN` over a staged record; the facade's caller does not, so a failed publication
leaves no record to resolve. The cost is an orphan published file when the process dies
between publication and the staging commit.

`journal_dir(root)` confines every target to its canonical root, and the root is the host's:
it must exist — durable — before the first publication, because the entry that names it lives
in its parent and only the host can establish that durability. Creating the root writes an
entry in its parent, so the host makes it durable by syncing the parent, exactly as the
example does. A missing root is refused with
the requirement named, and the publisher never creates it. Everything the publisher creates
is beneath the root. A target path that is absolute or carries `..` is refused before its
first byte; what remains is canonicalized — a parent resolving outside the canonical root
through a symlink below it is refused before anything is created or written. At the target
file itself no symlink can redirect the write: the bytes are written to a temporary file
created exclusively in the same directory and renamed over the target atomically, so a symlink
at the target is replaced rather than followed — refusal applies instead to a symlink at the
temporary name. Durability syncs the target's file, then every directory beneath the root the
publication created or wrote into, deepest first, ending at the root itself — a pure function
of the target's path, so a retry can never be weaker than the attempt that failed. Syncing a
directory opens it for reading, so the host must grant read on the root and everything beneath
it; ancestors above it need only traversal — a write-only journal root cannot be published,
because the sync that must precede settlement would be refused for a reason the write itself
never hit. A residual
window is documented rather than hidden: a hostile writer that replaces a directory beneath
the root with a symlink *after* the canonicalization check but during the write can redirect
that write, because canonicalization is resolution-before-write, not a filesystem lock. The
publisher trusts the host's tree for the duration of one publication; a host that cannot trust
its own journal tree has no filesystem-level defense the publisher could add.

### `deliberate` without a subject

The facade's `deliberate` names a target and evidence only; it takes no `subject_id` and never
chooses one. The request carries no subject, so the snapshot serves no memory page — a verdict can
cite evidence and existing cognition records only, and a `record_ids` entry naming a memory record
is refused as not visible — and the two checks the subject-level path runs over the subject run
over the whole space instead: a space-wide live `recall` grant is required, and any unsettled
intent in the space withholds the commit, whichever subject it belongs to. The record it settles
stamps the Mind's id as `assessment_context.context_id`. The served `deliberate`, which still
carries `subject_id`, takes the subject-level path unchanged.

`create`, `list` and `get` remain the only registry entry points. The facade adds no write path
and moves no validation point: every write still goes through the existing adapter/repository
boundary, and the cognition module keeps its single persistence validation inside `write_entry`.

### `cognition_read`

The carrier is read on the collection the row resolved at open — the read is bound to the
admitted Mind, and nothing the caller says can point it at another collection — and derived in
the facade; nothing is persisted. The derivation runs once: the validated record vector
`derive_mind_state` owns is projected into the wire `MindState` and moved into the answer, so
no second extraction pass copies the carrier's history. A carrier that holds no current-view
record yet — every Mind until its first judgment — answers `None`, not a refusal: a Mind
without a judgment is a state, not an error. After one record, the read answers the carrier's
current-view records and the derived `MindState` named by the Mind's id.

## Non-goals

- The served surface and its many-Mind routing; the wire contract is unchanged — no operation is
  added, removed or renamed and no argument changes.
- The native binding, which will wrap this facade; the daemon; any release before SPOKE 0.19.0.
- Space-level capabilities as a public facade surface; they stay available through the existing
  APIs.
- A second write path, a moved validation point, or any grant widening.
- Subject defaulting: no facade operation chooses a subject for the caller.
- Host provisioning: subject registration, grant administration beyond the `create` row, the
  journal publisher and the model providers are the embedding host's, as on the served surface.

### Admission decisions the spec records

**The facade is convenience, not a sandbox.** An in-process caller already holds the store and
can read any row through it, so the facade deliberately does not reproduce the served path's
per-principal routing and disclosure predicates: a space-wide `knowledge_read` /
`knowledge_write` grant reaches the whole Mind collection, whatever `owner` an entry carries.
The boundary the facade composes is the store's own — `read_entry`, `write_entry` and
`list_entries_in`, the validation and revision checks inside them — not the adoption port
`put_knowledge_entry`, which is a different surface.

**Reflections are off.** Every handle builds its adapter and workflow binding with
`reflections: false` and offers no switch: the reflection surface (§7.25–§7.27 of the SPOKE
spec) is a provider-level capability the facade's convenience contract does not expose, and a
host that needs it embeds the adapter or the served surface directly.

## Invariants

- `open` is the registry's read and nothing more: the handle caches no grant and acts on no status
  from the row.
- Every operation opens its own transaction and admits the caller under its live grants on it
  before reading or writing.
- A failed publication leaves no record, no intent and a pending candidate; a failure after a
  durable staging answers `OUTCOME_UNCERTAIN`.
- No facade operation accepts or derives a caller-supplied collection, owner or viewpoint.
- A subjectless `deliberate` settles only on space-level admission and writes its record with the
  Mind's id as the assessment context.

## Where it lives

- `crates/mindcore-runtime/src/mind.rs` — the handle, its operations and the `review` fold.
- `crates/mindcore-runtime/src/journal.rs` — `journal_dir`, the crate's one journal publisher.
- `crates/mindcore-runtime/examples/mind_facade.rs` — the documented end-to-end example:

  `cargo run --offline -p mindcore-runtime --example mind_facade`.
- Tests: `crates/mindcore-runtime/tests/mind.rs`.

## Change obligations

- Adding, removing or renaming a facade operation, or changing what one composes, which
  transaction it runs on or which grants it re-checks, updates the operations table and the
  `Current state` narrative in the same change.
- Changing the `review` fold — staging, publication ordering, the `OUTCOME_UNCERTAIN` recovery
  contract or settlement — updates § `The `review` fold` in the same change.
- Changing `journal_dir` confinement, durability or refusal behaviour updates the journal
  paragraphs in the same change; changing what `open` caches or refuses updates § `The handle`.
- Changing `deliberate` subject handling or `cognition_read` derivation updates their own
  sections in the same change.
- Moving a listed path or command updates `Where it lives` in the same change; weakening or
  strengthening any invariant updates `Invariants` in the same change.
