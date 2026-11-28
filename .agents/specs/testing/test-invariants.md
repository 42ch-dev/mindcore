# Testing: fixture and ordering invariants

## Scope

The non-obvious premises of the crate test suites: why a fixture is shaped the way it is, which
window a timing test actually bounds, and which equality a cross-face assertion compares. It does
not restate test names or assertions; it records what a future editor must not break while the
test still passes.

## Runtime wire and façade tests

`crates/mindcore-runtime/tests/loopback.rs`:

- SQLite has no read trigger, so a storage fault during `prune_entry_history` is installed on the
  statement the prune itself runs: a view over `knowledge_entry_revisions` calling a missing
  function fails at prepare time, after admission and after the claim, so the fault is the port's
  and not Mind admission's.
- The adoption refusal is compared against the same principal's own knowledge `put`, half by half,
  not only halves against each other: a change to both adoption refusals alone still fails.
- A retired source anchors the stored revision its terminal row names (`load_retirement_entry` reads
  the row's `revision` column), so only the retired branch can produce an above-range value the
  response cannot describe. A live source anchors the caller's requested revision instead.
- A source whose `schema_version` sits above the served ceiling is stopped twice before a copy
  exists — the served put refuses it on the way in and the store's payload decode refuses to hand
  it back — so the fixture pins the end-to-end outcome, not the seam guard
  (`an_unserveable_copy_never_becomes_a_response`). It writes the row raw because a store written by
  another version can hold it.
- A live source whose row `revision` column is out of the served range while its payload's own
  revision is in range is not a store fault: the live branch anchors the caller's requested
  revision, never the column, so the copy is written from the payload.

`crates/mindcore-runtime/tests/mind.rs`:

- The minimal budget request — no query, cursor or `max_bytes` — must decode and answer a page, not
  a validation refusal; `max_items` is non-optional on the façade exactly because the wire requires
  the member.
- A replay test consumes the candidate first (revision 2, asserted) so the replay assertion cannot
  pass on an unchanged row: a façade that re-read the row would send a different
  `expected_revision`, change the payload digest and answer `IdempotencyConflict`.
- The one-connection pool settles FIFO: a task registers on its first poll while the staging
  transaction still holds the connection, and signals through a handshake channel. The settlement
  asks only after the commit, so it is always behind the interference task and reads a moved
  intent. The order is the semaphore's, never the scheduler's.
- The interference handshake moves the intent out of pending the moment the staging commit releases
  the connection, so the first settle is refused — the `OUTCOME_UNCERTAIN` path.
- The crash window the contract covers is exactly "staging durable, intent unsettled"; the fixture
  restores the intent to pending and repeats the review with the same key.
- An imported candidate keeps its own revision (`transfer/activate.rs`); the fixture makes it 5.
- The observer opens before the review starts, so nothing migrates or acquires the write while the
  staging transaction holds it.
- The publication handshake signals `entered` from inside the flush, then parks on a blocking
  channel. Waiting for that signal on a blocking thread is itself the observed state: it completes
  exactly when the publisher is inside its flush, however the scheduler gets there.
- The probe is spawned after the entry signal as a bare unit of executor work; its completion proves
  executor progress while the publisher is parked. The observation loop advances on the handle's own
  state, with `yield_now` as a politeness hint, never as the ordering proof.
- The cancel variant waits for the same entry signal, then cancels. Its witness is a bare pool
  acquisition: it completes exactly when the connection is free, and the connection is free while
  the publication is parked only if the cancelled attempt released its transaction early. The
  sentinel spawned behind it is the observed state that the witness was polled to its acquisition.
- The witness window spans real thread scheduling because sqlx's rollback runs on its connection
  worker thread; it is a detection window for a regression, not an ordering proof, and its length
  bounds nothing the fixed implementation depends on.
- Releasing the first publication ends its flush and sets the finish flag before the connection may
  become free: the correct rollback runs after the blocking task returns the transaction.
- A poisoned record for the first Mind's carrier entry id is written into the second Mind's
  collection; it validates because its holder is the second carrier it actually lives with.

`crates/mindcore-runtime/src/tools/tests.rs`:

- The pre-check is charged against the request's budget, so the probe names a budget a
  process-scheduled pre-check cannot spend: it measures the ABI, not the clock.

## Spoke adapter tests

`crates/mindcore-spoke/tests/memory_adapter.rs`:

- The grant fixture snapshots every row ordered by id, so an in-place edit — a widened
  `capabilities_json`, a moved expiry, a revoked row — differs from the snapshot.
- `memory.receipts.get` and the two control reads are paths `guarded_reads` does not carry.
- The listing fixture creates two Minds for one owner out of id order, so the answer proves its
  sort, and a third for another owner.
- `mind_extra` gets a second live grant so one undecodable row cannot hide it.
- The bad row sorts first because `capabilities_json` holds bytes the driver cannot hand back as
  text; a good row sorts after it.
- The creator's own row is pinned whole so a repeat that widened its capabilities or moved its
  expiry shows up; the seeded admin row lives in another space and is outside this Mind's snapshot.

`crates/mindcore-spoke/tests/workflows.rs`:

- The carrier entry is the collection's first live row, so exactly-at-the-bound means
  `SERVED_KNOWLEDGE_LIST_LIMIT - 1` seeded rows beside it.
- The judgment is stored as a `cognition_records` row the read hydrates into the payload's module.
- `connect_with` releases one checked connection, so the acquire takes an idle connection on its
  first poll — the premise that makes the deadline test meaningful.

## SQLite store tests

- `crates/mindcore-sqlite/tests/knowledge.rs`: an anchor naming no shared entry is unconstructible —
  `AdoptionAnchorSourceEntryId` rejects an empty id on both `TryFrom` and its decode — so the
  absent-anchor case is the only shape the type admits.
- `crates/mindcore-sqlite/tests/minds.rs`: the fixture expires a grant a few tens of milliseconds
  from now, so a liveness decision taken before the read still sees it live while a per-row
  decision must not.
- The same file holds the pool's one connection open with an unrelated borrower, so `list_minds`
  waits while the expiry passes; WAL readers are not lock-blocked, so the wait is purely the
  pool's.

## Change obligations

A change to a fixture's premise, to a test's observed-state discipline, or to a window a timing
test bounds updates this page in the same change. A test that starts depending on scheduler order
instead of an observed state is a defect in the test, not a reason to widen this page.
