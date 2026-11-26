# Architecture: Mindcore serving seam

## Scope

Signed Rust SPOKE sessions over a host-supplied transport: the Memory face, the four Mind tools, and the Knowledge, History and adoption face contracts a restored per-Mind composition must satisfy. The library does not own a listener, accept loop, identity provisioning, publisher, or model provider.

## Current state

### Knowledge

Stage 1 composes no knowledge face: `ServeFaces` carries no scope, collection or knowledge
admission, the manifest advertises neither `spoke-knowledge` nor `ke-ownership`, and every
`port.knowledge.*` op is refused on the responder's dispatch-deny branch — raw wire `op_unsupported`,
which the Rust dialer surfaces as `CapabilityPortMissing` with `details.wire_code`.
`ScopedKnowledgeStore` and `RoutedKnowledgeStore` stay in `mindcore-sqlite` for Stage 2 to bind
per Mind. The paragraphs below state the contract a restored face must satisfy.

A restored face installs a `KnowledgeAdmission` callback in `RemoteKnowledgeFace`. Connect supplies the authenticated peer id and, through A1, the `scope_id` the request declares; the callback admits that one Mind, resolving the collection and carrier through the Mind's registry row on the caller's own transaction, and returns the admitted `Scope` for that Mind or refuses. There is no host-configured route map to consume and no fallback: no collection is guessed from an entry id, `owner`, `viewpoint` or Memory `SpaceId`, and an unknown Mind id and a Mind the peer holds no grant for answer one `NOT_AUTHORIZED` with no `details`, exactly as the memory path's admission answers. Memory space and workflow knowledge grants remain separate and go through the existing `MemoryWorkflowBinding` admission. The `ke-ownership` protocol gate checks the negotiated flag for a list with an explicit `viewpoint`; it neither chooses the collection nor grants the peer entry access.

`list` names the admitted `scope_id` and `viewpoint`, as it does today, and Connect bounds the request by the admitted scope before calling the store. `get` and `put` carry the optional `scope` member A2 adds, with the negotiable marker saying the responder honours it: a request that names a scope on a responder without the marker is refused as unsupported — wire `op_unsupported`, `CapabilityPortMissing` in the Rust dialer — and never served from a default collection, because a dropped scope would land the read or write in a collection the caller never named. The scoped adapter binds the admitted Mind's collection on every statement. Within it, entry-id, entry-type, source-id and viewpoint filters use SPOKE's knowledge predicate (including owner-private disclosure); an explicit empty set matches nothing. The face does not serve cognition, timeline or extension-based selection; refuse unsupported constraints rather than silently ignoring them. List results are in `entry_id` order. The host admission callback validates the scope it returns with `mindcore_sqlite::validate_admission_scope` before returning it: Connect's list merge keeps only the request's cognition selection, so an admitted cognition selection reaches the collection query unless it is refused at admission. A served list reads at most `SERVED_KNOWLEDGE_LIST_LIMIT` (512) **live** rows of the admitted Mind's collection — the live predicate rides in the statement — and refuses a larger live set with `InternalError` naming the bound; the bound applies before disclosure filtering and never truncates silently. A retired row therefore never consumes list budget.

The admitted Mind's grants read/write knowledge access within its scope, not unconstrained access to the database. `put` checks the proposed payload and any existing row against the admitted constraints on the same `BEGIN IMMEDIATE` transaction, before exposing a revision or committing a change. A missing expected revision keeps create-only semantics; a supplied one performs compare-and-swap through the common store. A hidden existing row is never overwritten, and hidden entry data/revisions are never returned. No operation probes another collection.

| Refusal | Result |
|---|---|
| Required negotiated family, conditional ownership flag or serving face missing | Wire `op_unsupported`, exposed by the Rust dialer as `CapabilityPortMissing` with `details.wire_code`. |
| Peer unadmitted, or a request names or widens another Mind's scope | `NotAuthorized` (`NOT_AUTHORIZED`), no target details. |
| Host admission returns an unsupported selection | `NotAuthorized`, no target details: refused at admission before Connect merges the scope, so the store is never reached. |
| `get`/`put` carry a `scope` the responder's negotiated marker does not cover | Wire `op_unsupported` (`CapabilityPortMissing` in the Rust dialer): refused, never silently dropped. |
| Malformed scope/payload, or a caller-declared (request) scope with an unsupported selection | `InvalidInput`; the unsupported-selection case is refused at the store. The responder's earlier payload-dependent gates still apply. |
| Get id absent or hidden within the admitted Mind's collection | One generic `KnowledgeEntryNotFound`. An out-of-scope put is `NotAuthorized`. |
| Admitted create hits an existing visible id; admitted update carries a stale/missing stored revision | `KnowledgeEntryAlreadyExists`; `StoredRevisionStale`, respectively. Preserve backend codes; storage failures are `InternalError`. |

### History

The `spoke-history` family publishes two operations over the same signed session: `history.revisions.get` returns a `RevisionSnapshot`, `history.revisions.prune` returns `HistoryPruneResult { deleted, floor_revision }` where `floor_revision` is the local report's floor.

Stage 1 composes no history face: `ServeFaces` carries no history switch, `face_capabilities` advertises no `spoke-history`, and both operations are refused on the responder's dispatch-deny branch — raw wire `capability_missing`, surfaced by the Rust dialer as `CapabilityPortMissing` with `details.wire_code`. The paragraphs below state the contract a restored face must satisfy, and the Stage 2 handoff at the end of this section names what it must re-implement rather than re-bind.

One store fills two port roles: `RoutedKnowledgeStore` implements `spoke_operations::HistoryPort` beside `ScopedKnowledgePort`, so no second store module exists and the runtime owns no second history implementation — only the dedup record beside it. Admission is the knowledge face's, composed as `HistoryAdmission = (principals(peer)?, knowledge_admission(peer)?)`: a peer outside either callback gets that callback's refusal, and can therefore replay and prune exactly the scope it reads and writes through `port.knowledge.*`. Connect compares a present request `scope` with the admitted scope before calling the port — `NOT_FOUND` on get, `NOT_AUTHORIZED` on prune — and the port re-applies `RoutedKnowledgeStore::admit` as its own guard.

Coverage is `knowledge_entry` alone, because the store keeps no relation revision table. Any other `entity_kind` answers `HISTORY_ENTITY_UNSUPPORTED` after admission and before any statement. The entity's own disclosure rules apply unchanged: a missing, a retired and a peer-hidden row are one `NOT_FOUND` on get and on prune, so neither op tells a peer admitted to the collection which of the three it asked for — the concealment answers reuse the absence constructor, so code, message shape and `details` are equal by construction. Get checks two payloads, because visibility is a payload property: the live row decides existence and retirement, and the replayed entry is then held to the same admitted scope, so a revision whose own disclosure, viewpoint, source or entry-type fields fall outside that scope answers the same `NOT_FOUND` with no snapshot. Both ops run in one transaction — a read for get, one `BEGIN IMMEDIATE` covering the permission read and the delete for prune — and every answer passes through the same storage-failure generalization as the knowledge face, so no table or column name reaches a peer.

`RevisionSnapshot` carries `schema_version` `1`, `entity_kind` `knowledge_entry`, the requested `entity_id` and `revision`, `extensions = {}`, and the replayed entry as `snapshot`: the stored entry travels as entity data, so its timestamps keep their stored spelling and a second replay is byte-equal.

| Refusal | `details` |
|---|---|
| `entity_kind` other than `knowledge_entry` | `HISTORY_ENTITY_UNSUPPORTED` with exactly `entity_kind`, the requested kind |
| Revision below the entry's persisted floor | `HISTORY_TRUNCATED` with exactly `floor_revision`, an integer ≥ 0 |
| Revision `0`, above the live revision, or unassigned to the entry | `NOT_FOUND`, none |
| Missing, retired or peer-hidden row on get or on prune | `NOT_FOUND`, none |
| `keep == 0` | `INVALID_INPUT`, none; refused by the port, since Connect's decoder admits `0` |
| Malformed payload, or a prune integer outside the JCS exact-integer range | `INVALID_INPUT` with `details.op` |
| A failed statement or a contradicted history gap | `INTERNAL_ERROR`, none after generalization |
| Same `idempotency_key` with a different payload digest, when a recorded outcome exists to compare | `IDEMPOTENCY_CONFLICT`, none |
| A claim with no outcome — another caller's live claim, one whose process never settled, or a fresh key its principal had no quota to claim (4096 rows already held by that principal in the collection's namespace) | `RECEIPT_EXPIRED`, none; the prune is not re-executed |

A key that is both a retained claim and a same-key different-payload retry answers `RECEIPT_EXPIRED`: there is no recorded outcome to conflict with, and the claim wins.

`HISTORY_NOT_RETAINED` is never answered: every revision at or above the floor is rebuilt from the next checkpoint or the live row, and a contradicted gap is storage corruption, not a retention gap.

Prune is deduplicated by Connect's recorded-outcome store, keyed by `(op, effective scope, requester principal id, idempotency_key)` and holding the request digest plus the settled outcome. A restored history face composes `crates/mindcore-runtime/src/history_dedup.rs`'s `SqliteHistoryDedupStore` through `HistoryPortService::with_dedup_store`, over the same pool and database file as the knowledge store — one store, one pool, one process. A host therefore gets the durable store, not the released in-memory default, and no `ServeFaces` field selects between them.

`history_dedup` keys each row by the collection its history surface routes to and the retry identity, in two columns under the composite primary key `(namespace, key)` — the namespace is `RoutedKnowledgeStore::collection()`. A framed string cannot carry this: a collection name is arbitrary, so any separator would let two `(collection, key)` pairs collide. Two surfaces on one database can share a wire scope, a principal and an idempotency key while serving different collections; the namespace keeps the second from replaying or refusing the first's prunes, and a same-collection retry replays exactly as before.

A third column, `principal`, carries the requester the key names — the third member of Connect's canonical `[op, effective_scope, principal_id, idempotency_key]` tuple. Each `(namespace, principal)` pair is bounded at 4096 rows, claims included. `lookup` claims the key by inserting the row before the port runs and `record` upserts the digest and the settled outcome into it, so the claim, the port call and the record each take and release the pool's one connection in turn. Within one process Connect's default keyed single-flight still serializes same-key callers, which then replay the recorded outcome.

**Capacity.** At the bound an unseen key is not inserted: `lookup` answers `Evicted`, so Connect answers `RECEIPT_EXPIRED` and no destructive prune runs, and `record` writes no row that would cross the bound — neither the claim path nor the settle path can grow a quota past it. A key already present is unaffected, so recorded outcomes still replay and claims still refuse at the bound.

The bound is per `(namespace, principal)`, not per database and not per collection alone: every surface serving one collection shares the namespace, but each caller holds its own quota inside it, so one high-churn principal can exhaust only its own and never disable prune for another on the same collection. A different collection has its own namespaces and quotas. Nothing is ever deleted and no clock is read, so a recorded outcome replays for the database's lifetime and no forward host-clock jump can age a live record into a re-executed prune. A caller that exhausts its quota gets refusals for fresh keys rather than a prune the store cannot dedup; recorded outcomes for existing keys keep working, a fresh key becomes available only if the table loses rows, which nothing here does, and a host that needs more headroom raises the cap or recreates the database.

Every key that is not the canonical four-member tuple — which this surface never sends, since Connect canonicalizes before it calls — is charged to one shared `unknown` bucket rather than to a principal of its own, so such keys cannot mint fresh quotas. They still claim and replay normally until that bucket is full.

A claim with no outcome answers `RECEIPT_EXPIRED` and never reaches the port. The claim is taken before the port runs, so that state is reached both when the port was never called and when the port call was in flight — or ran, and its process died before `record`. Two processes serving one database therefore turn the later same-key caller into a refusal instead of waiting; one serving process per database stays the host obligation.

**Dedup consequence:** the commit-to-record window no longer re-executes. A prune whose process died between its committed port call and `record` leaves a claim with no outcome, so a same-key retry across a restart — or a reassembled surface on the same file — answers `RECEIPT_EXPIRED` and never reaches the port: no checkpoint the original call never saw is deleted. That refusal is the price of at-most-once: the retry does not replay the lost `{deleted, floor_revision}` either, and closing that needs the retry identity to reach the port, which the released history contract does not pass.

**Quota consequence:** the bound costs headroom, not history. A recorded outcome replays for the database's lifetime, because nothing deletes and no clock is read, so a same-key retry never becomes a new request by aging — the failure a time-based window introduces, where a forward host-clock jump reclaims unexpired records and lets the destructive prune run again. What the bound does cost is a ceiling per caller: a principal that spends its 4096 rows is refused fresh keys until the host raises the cap or recreates the database, which is a refusal rather than a prune the store cannot dedup. Because the quota is per principal, that ceiling is the noisy caller's own and never another caller's on the same collection.

Because the claim is taken before the port runs, `RECEIPT_EXPIRED` reports one thing only: no recorded outcome for that key. It says nothing about whether the original request executed. Two states reach it — a key already present with no outcome behind it, and a key its principal had no quota to claim — and the caller cannot tell them apart, so neither is a usable distinction.

- **What history remains now** is observable: the entry's persisted `history_floor_revision` and which checkpoints still read. That state tells the caller whether another prune is still needed — nothing more; it does not attribute the floor to any particular request.
- **Whether the original request executed** is not observable. The same claim row covers a port that was never called, a port call still in flight, and a port call whose process died before `record` — and the last two may have committed.

So the answer is not a case distinction a caller can act on, and a caller must not read `RECEIPT_EXPIRED` as proof the prune did not commit. The persisted floor and surviving checkpoints are the only evidence available, and they decide whether re-pruning under any `idempotency_key` is still needed.

Adoption keeps the released in-memory default and its own restart exposure. Adoption has no equivalent permanent-loss replay path, but it is not a guaranteed no-write repeat either. While the source still resolves to the terminal the first call copied and the destination already holds the copy, the create-only CAS and the anchor rules make the retry answer `already_present` or `REVISION_CONFLICT` and write nothing. After a reassembly dropped the record, a source since retired to a different terminal resolves the same request to that new terminal; with `destination_entry_id` omitted the copy is then addressed by the new terminal's id, so the retry writes a second copy. The original copy stays intact and nothing is lost — the repeat is simply not guaranteed to write nothing once the terminal changed and no destination id was supplied.

**Stage 2 handoff: the durable dedup is not in the retained port.** `crates/mindcore-sqlite`'s `RoutedKnowledgeStore` still implements `spoke_operations::HistoryPort` — replay and prune over the routed collection — but that port carries no retry record. The claim-before-prune and recorded-outcome store lived in `crates/mindcore-runtime/src/history_dedup.rs` (`SqliteHistoryDedupStore`, composed through `spoke-connect`'s `HistoryPortService::with_dedup_store` over the store's own pool) and was removed with the face. Stage 2 must re-implement that durable dedup namespace — one row per `(namespace, key)`, `key` being Connect's canonical `[op, effective_scope, principal_id, idempotency_key]` retry identity and the namespace the collection the surface routes to, with the principal stored beside the key for the quota, claimed before the port runs and settled with the request digest and the outcome after it, each `(namespace, principal)` holding at most 4096 rows and nothing ever deleted — before it serves prune again. Re-enabling prune over the retained port with Connect's in-memory default instead would let a same-key retry after a restart re-run a destructive prune the first request may already have performed. Records already persisted by the removed store must stay readable: its composite key is `(namespace, key)` with the collection name as the namespace, not a framed string, so a Stage 2 store must keep that column pair rather than re-derive the namespace.

### Workflow tools

`MindcoreServe` derives served capability ids and tool descriptors from installed faces and handlers; the host supplies identity, transport, allowed peers and non-capability manifest metadata. Stage 1 composes the host's memory service and nothing else: the runtime's own `MindPorts` answers `as_memory` and takes the trait's default `None` for the knowledge, history and adoption families, so a host cannot configure a second face beside it. The Memory service consumes a trusted peer-to-principal mapping and implements the advertised family contract; the runtime does not create fake baseline, reflection or transfer providers. For each accepted transport, register handlers before releasing inbound traffic; `connect_responder` starts its own SPOKE receive task and does not install handlers from the manifest.

The memory face advertises `spoke-memory` whenever a service is attached, and `spoke-memory-transfer` only when that service reports all five §7.29–§7.33 operations through `mindcore_spoke::ServedMemory::operations`. `RemoteMemoryService` exposes one `invoke` seam and names no operation, so the service itself carries the answer: `ServedMemory` extends it with an operation list whose default is empty, and a bare `RemoteMemoryService` therefore never claims the transfer family. The derivation is the only advertisement list: `face_capabilities` compares [`TRANSFER_OPERATIONS`] against what the service reports, so no host-supplied string can claim a family the bound service cannot show it dispatches.

`mindcore_spoke::TransferService` is the service this crate binds: `operations()` returns the six operation strings it dispatches — the five §7.29–§7.33 transfers and §7.28 receipt polling (`memory.receipts.get`) — and `invoke` routes each of them to the matching `MemoryAdapter` call over the host's pool. `RemoteMemoryService::invoke` hands the service no transaction, so the service is the caller: each op runs in one transaction — `BEGIN IMMEDIATE` for the four mutations, reads for `exports.get` and `receipts.get` — and commits only after the adapter answers `Ready`; a commit whose acknowledgement is lost answers `OUTCOME_UNCERTAIN` rather than claiming either outcome. Any other `memory.*` op is refused as `CAPABILITY_PORT_MISSING`.

The service binds its own `MemoryAdapter::new(false)`: the transfer family reads no reflection policy, so the adapter's reflection flag serves only `describe` and the reflection operations, and no transfer op consults it. What the transferred bundle and chunks must satisfy is `data/transfer-bundle-chunking.md`.

An export bundle never discloses beyond its declared scope. A receipt row carries no subject or placement, so only a whole-space export summarizes the space's receipts; a subject-scoped export omits them, because any receipt summary there would name unrelated operations. A withheld subject contributes no subject-scoped grant summary either: naming its `subject_id` would disclose the withholding and dangle at the target's own verification.

A stored document whose `content.structured` (or a pending `proposed.content.structured`) is an explicit `null` cannot be exported: the pinned content type carries no present null in that member, so `memory.exports.create` refuses with `INVALID_INPUT` and `details.reason = "explicit_null_content_not_transferable"` rather than sealing a changed document, and final staging verification refuses an arriving bundle with the same reason. A `null` nested inside `structured` or inside a cognition record is carried unchanged.

The host's peer resolver supplies the `Principal` from the authenticated responder session, not from arguments. `ToolHandler` receives only JSON arguments, so a handler binds to its responder session (weak reference, no ownership cycle). Missing peer/principal is `NotAuthorized`. Do not share a handler closure bound to one session with a different session. Provider capabilities and `DomainLens` are host inputs. Workflows continue to enforce their own live memory/knowledge grants and settlement boundaries.

Connect owns the signed `{ "arguments": {…} }` request and `{ "result": … }` success wrappers. The following keys define closed argument objects; `request` and `target` use the corresponding existing SPOKE DTOs. Results use the listed fields and snake-case branch tags:

| Capability id and op | Arguments | Result |
|---|---|---|
| `tools.mindcore.recall_context` | `request` (`MemoryRecallRequest`), positive integer `max_tokens` | `{records, knowledge, tokens_used}` |
| `tools.mindcore.absorb` | `request` (`MemoryCaptureRequest`) | Array of `{status, candidate_id}` with `status` = `pending`, `dropped`, or `settled` (adds `record_ids`) |
| `tools.mindcore.deliberate` | `space_id`, `subject_id`, `target`, `evidence` (`[{id, text}]`), positive integer `timeout_ms` | `{status, verdict, judgment}` with `status` = `no_change`, `needs_review` (adds `draft`), or `settled` (adds `record`, `mind_state_version`) |
| `tools.mindcore.read_cognition` | `space_id` | `{carrier_revision, records, mind_state}`; a Mind that has judged nothing answers `records: []` and a null `mind_state` |
| `knowledge.adopt` (capability `spoke-knowledge-adoption`, which requires `spoke-knowledge`; not composed in Stage 1) | `source_scope_id`, `source_entry_id`, positive integer `source_revision`, `destination_scope_id`, optional `destination_entry_id`, `idempotency_key`, optional `extensions` | `{outcome, entry}` with `outcome` = `adopted` or `already_present`; `entry` is the stored copy either way |

`deliberate` takes no `holder`: the Mind's registry row names the carrier, and a peer that still
sends the retired member is refused `InvalidInput` by the closed object. Every served argument
object names a Mind through `space_id` and never a collection, a carrier or a principal.

An `absorb` run that updates a record and dismisses its candidate stages two intents in one transaction, the record update and the candidate dismissal, then collects the settled revisions from each staged intent's own result: a revision is yielded only for a record id the result names. The dismissal leg's result names no record, so only the update leg's result yields a revision; walking both legs adds nothing from the dismissal.

**The deliberation judgment.** Every successful `deliberate` response carries a required top-level `judgment` beside `status`, `verdict` and that branch's own members. The projection — target, revision-pinned evidence, outcome, basis, `judgment_id` identity and `created_at` — is [`mind-model.md`](mind-model.md)'s; this page owns only what the seam does with it. The workflow builds and validates the judgment in `crates/mindcore-spi/src/judgment.rs` and the tool boundary forwards it unchanged. A judgment the released validator refuses never reaches the wire: the call answers `InternalError` with `details.kind = "judgment_projection"` and no `retry_class`, and on the `settled` branch that check runs before the cognition record is committed, so a projection defect writes nothing. `spoke-judgment` is advertised exactly when `tools.mindcore.deliberate` is in the final registration list: the family has no operation of its own and gates nothing, so a surface that serves no `deliberate` advertises none.

An `absorb` replay reports what an earlier run of the same capture recorded for each consumed candidate. A candidate consumed by the update leg's dismissal is the one consumption whose result names no record, so the binding reads the update leg's own receipt — keyed by the capture key, the candidate id and `update` — for the record id: a committed receipt reports `settled` with that id and an absent one reports `dropped`. Capture and update receipts are rows of one table under one aged policy, and the binding has no per-operation retention branch; the capture receipt is minted first, so an aged-out capture receipt refuses the replay before an aged-out update receipt could be read.

Two of the three workflow tools reach knowledge at all, and only they: `deliberate`'s carrier admission and `recall_context`'s knowledge slot. Deliberation settles onto exactly one carrier entry, so the Mind the request names decides it: `space_id` is resolved through the Mind's registry row on the caller's transaction (`MemoryWorkflowBinding::carrier_collection`), and a binding an in-process facade opened may pin its own Mind first. Both readers — the carrier write and the recall slot — resolve through that one lookup, so no workflow consumer can reach a collection other than the admitted Mind's. `absorb` consults no registry row. Adoption resolves its destination from the same registry row after admission: it is the admitted Mind's registered collection, so a copy it writes is one the same peer reads back through that Mind's `knowledge.get`. A `space_id` with no registry row is the admission refusal, before any carrier access: one `NOT_AUTHORIZED` carrying a fixed message and no `details`, so it never reveals whether a Mind exists. The row never substitutes for a live grant — every carrier write and knowledge read still re-derives the caller's own grants on its own transaction. Recall's slot is independent of the memory page and best effort for the row and the grant: a missing row, or a missing live `knowledge_read` grant in the request's space, returns the memory result with `knowledge: []`, never a refusal. That tolerance covers the row and the grant only — a grant-store read or collection-list failure is not converted into an empty slot; it propagates and fails the recall. With both the row and the grant the slot lists only the admitted Mind's collection under the same disclosure predicate as the served knowledge list, so an `owner`-private entry never reaches the pack. The registry row is the workflow collection allowlist: `create` writes it, it is not read from the wire, and no served argument names a collection, so a peer cannot select one or change a mapping without creating a Mind. `recall_context` keeps serving for a space that is no Mind, with no slot to fill, and `absorb` keeps serving because it never consults the row. Memory authorization, withholding, snapshot revalidation and token budgeting are unchanged by routing and stay separate rules: the page is still the adapter's own ACL and withholding read, so a memory grant revoked while providers run refuses the reread as before, while a knowledge grant revoked mid-flight empties the reread's slot, which makes the snapshot differ and surfaces as the retryable `RevisionConflict` rather than a released pack.

Settlement returns its original `SpokeReject` (code, message and details); malformed arguments and `SpiError::InvalidInput` return `InvalidInput`. A `PolicyRejected` policy veto is also `InvalidInput`, with `details.kind = "policy_rejected"` and the host's reason as the message — never `NotAuthorized`, which stays the lost-admission code. A deadline returns `InternalError` with `details.kind = "deadline_exceeded"` and `details.retry_class = "retryable"`: nothing committed, so the caller may repeat the call. That no-write guarantee now also covers an expiry during the store-connection wait inside `commit_cognition`, and an expiry on a connection already in hand before the begin; it stops at `BEGIN IMMEDIATE`, whose wait for SQLite's write lock is outside the request deadline, and a write that has begun keeps its actual or uncertain outcome. [`mind-model.md`](mind-model.md) owns that boundary. The other provider errors return `InternalError` with `details.kind` set to `embeddings_unavailable`, `embedding_failed`, `reasoner_unavailable`, `reasoner_failed`, or `invalid_reasoner_output` as appropriate and carry no retry class; do not reflect provider internals into the wire message. Errors use the signed error envelope, never a success DTO. The client must negotiate each tool id it intends to invoke; descriptor `capability_id` and `op` match, and the responder owns the `mindcore` namespace.

**The deliberation budget's stated limit.** `deliberate`'s host-side pre-check runs inside the
request's budget, and the timer around it drops the pool wait and starts no workflow when it fires.
A query already inside SQLite's own step cannot be interrupted by that timer — SQLite offers no
cancellation — so its connection stays busy until the step ends, and the time a timed-out call spent
inside the step is not reclaimed. The remaining budget is also sampled after the pre-check and
before the holder and evidence are built, so those builds are charged to the workflow's own deadline
rather than to the sample. No cancellation mechanism beyond the timer is attempted.

### Adoption face

`knowledge.adopt` is the only adoption surface. It is the single registered operation of the
`spoke-knowledge-adoption` family, it requires the co-capability `spoke-knowledge`, and its request
and response are the published 0.18.0 `KnowledgeAdoptRequest` / `KnowledgeAdoptResponse` — no
product request or response type exists. The retired private product adoption tool is removed
rather than kept as a second authority: assembly drops its reserved `tools.mindcore.*` id from every
host registration in both adoption configurations (`crates/mindcore-runtime/src/lib.rs:165`), so no
manifest carries it and no handler answers it even when a host registers the id.

Stage 1 composes no adoption face: `ServeFaces` has no adoption setting, `face_capabilities` names
neither `spoke-knowledge-adoption` nor its `spoke-knowledge` co-capability, and a `knowledge.adopt`
call is refused on the responder's dispatch-deny branch — raw wire `capability_missing`, surfaced by
the Rust dialer as `CapabilityPortMissing` with `details.wire_code`. The paragraphs below state the
contract a restored face must satisfy. A restored face is composed per Mind and bound to one: its destination is that Mind's
registered collection, resolved through the registry row after admission, and there is no
surface-wide destination setting to consume. The source stays host configuration — one collection,
named on the wire by a `scope_id` and fixed by the host — and it is the only host-configured side of
the call. A copy is therefore readable back through the admitting Mind's own
knowledge face whenever the peer's admitted selection admits it. The Stage 2 handoff at the end of
this section names what it must re-implement rather than re-bind.

**Admission is the admitting Mind's knowledge-face admission**, composed as
`(principals(peer)?, knowledge_admission(peer)?)` like the history family's: a peer the knowledge
face does not admit cannot adopt, and a principal the knowledge admission refuses answers the
target-free `NOT_AUTHORIZED` where the removed product tool would have served it. Neither the
principal nor the admitted scope comes from the payload; the admitted scope is the Mind's own.

**Scope mapping.** Every scope member of the request is compared, never used to select anything:

| Request member | Must equal | Otherwise |
|---|---|---|
| `source_scope_id` | the host-configured source's `scope_id` | `NOT_FOUND`, no details — the same generic absence as a missing source |
| `source_entry_id` | a live or retired row of the host-configured source collection | `NOT_FOUND`, no details |
| `source_revision` | the live source's current revision; never compared for a retired source; never `0` | `REVISION_CONFLICT`, no details / `INVALID_INPUT` |
| `destination_scope_id` | the admitted Mind's `Scope.scope_id` | the target-free `NOT_AUTHORIZED`, no details, before the source is read |
| `destination_entry_id` | absent → the resolved terminal's `source_entry_id`; a supplied value may name that same id | — |
| `idempotency_key` | 1–128 code points | `INVALID_INPUT`, refused by Connect's decoder |

The destination collection is the admitted Mind's registered collection and nothing else: the row
the request's Mind id resolves to, read after admission, never a surface-wide field and never a
wire value. It is that Mind's one collection with one slot per `destination_entry_id`: the first
adoption of a given id claims that slot, and a later call naming the same id is answered from the
stored copy's anchor instead of writing a second one. The admitted `Scope`'s `entry_ids`,
`entry_types`, `source_id` and `viewpoint` refinements still apply to it, because the admitted scope
is the host's statement about which entries of that Mind's collection the peer may reach.
Adoption applies them to the destination entry it would return and to the copy it would write: an
entry outside the admitted selection is neither served nor written, and answers the ordinary
occupancy refusal — `REVISION_CONFLICT` with no details, byte-identical to a visible occupant's —
so a hidden occupant, a visible one and a retired one are the same answer. What the caller observes
is one occupancy whatever the row's state: the destination read returns a live occupant, which the
adoption refuses before any write, and it resolves a retired row the store's live-only read reports
as an absence by checking the row including superseded ones and answering the same occupancy. Either
way no revision gate is reached and no put runs. The invariant is the observable answer — code,
message shape and absent `details` — not a shared constructor, and no path reveals the occupant's
content, revision or visibility. `NOT_AUTHORIZED` is reserved for a `destination_scope_id` that is
not the admitted Mind's scope: the target-free admission refusal, never a comparison against a
configured destination, which no longer exists. The registry row selects the destination; the
admitted selection narrows what the call may touch inside it.

**Slot occupancy is a permitted disclosure.** A create-only destination answers occupied for an
occupied id and success for a free one, so the answer reports whether the `destination_entry_id`
slot is taken. That is permitted and discloses nothing wider than the knowledge face the copy lands
on: a create-only `KnowledgeEntryPort::put_knowledge_entry` over that same face already
distinguishes an occupied id from a free one while hiding the entry. Adoption therefore reports
occupancy and nothing about a concealed occupant — never its content, revision or visibility; the
one occupant detail it can return, `details.adopted_revision`, needs the destination port to have
handed that occupant back, so only a copy the caller can already read carries it. An absolute
"existence is never revealed" claim would be false and is not made here.

**Assembly** refuses an empty source scope id, an empty source collection, a source collection equal
to any Mind's registered collection — the assembled Mind's own included, and including a Mind that
is not currently connected — and a source scope id equal to the destination scope, all with
`InvalidInput` before any port or registration exists. A source that is also a destination would let
one Mind's adopted copy become the shared entry another Mind reads, and a source scope equal to the
destination scope would let a Mind adopt from its own destination.

**Terminal anchor.** The requested source resolves through `spoke_operations::resolve_retirement_chain`
inside the same transaction before anything is read for the copy: a live source resolves to itself,
and a retired source resolves to the first live entry along its markers, so the copy's
`adoption_anchor` names that terminal's `(source_scope_id, source_entry_id, source_revision)` while
the copy's own `entry_id` is `destination_entry_id` when supplied and the terminal's id otherwise.
A chain that cannot resolve — a cycle, an ambiguous merge, a missing hop, more than the resolver's
32 edges — answers `SOURCE_RETIRED` with `details.retirement_chain`, the visited ids in order. A
retired source's requested revision is not compared, because the anchor records the terminal's own
revision; a live source that moved off the requested revision answers `REVISION_CONFLICT` with no
details. A resolved terminal whose own revision is above Mindcore's served ceiling of `2^53 - 1` has
no conformant success to report, because the anchor is the only carrier of that revision: the
`adoption_anchor.source_revision` member records the terminal's revision and must satisfy the
IEEE-754 safe-integer bound, which is Mindcore's own check and narrower than the numeric check
Connect applies to the request. The copy's own `revision` is never that value — it is the
destination row's write revision, assigned by the store (`1` on a create) — so the response's
`entry.revision` is checked separately on return. The call refuses before writing anything, with the
fixed fault message and `details.kind = "store"`; the copy is never committed for a response the
wire cannot carry.

**Copy and inherited governance.** `body`, `canonical_name`, `entry_type` and `status` come from the
terminal; `modules` loses `cognition` rather than keeping an emptied envelope; `source_anchor` stays
unset; `adoption_anchor` records the terminal triple; `revision`, `created_at` and `updated_at` are
left for the store to assign; the request's `extensions` are not copied. `owner` and `disclosure` are
inherited from the terminal rather than reset to the destination's, because the destination is
the admitted Mind's registered collection — the collection that Mind's knowledge face serves and the
access boundary itself — and the port refuses every source the viewpoint-free predicate does not see,
so an inherited policy cannot disclose anything the admitted principal could not already read. The
copy's `entry_id` is the caller's `destination_entry_id` when supplied — a caller may supply the
terminal's own id — and the terminal's id otherwise; the anchor names the terminal either way.

**Refusals and dedup.** The call reads only the host-configured source and writes only the admitted
Mind's registered collection, committing on one `BEGIN IMMEDIATE` transaction that commits for
`adopted` and rolls back for `already_present`. `NOT_FOUND`, `NOT_AUTHORIZED`, `REVISION_CONFLICT`,
`SOURCE_RETIRED`, `ADOPTION_ANCHOR_IMMUTABLE` and `INVALID_INPUT` travel as the contract builds
them; a repeat naming the copy's own anchor triple returns `already_present` with the stored copy,
another revision of the same source is `REVISION_CONFLICT` with exactly `details.adopted_revision`,
and any other occupant is `REVISION_CONFLICT` without that member, left byte-identical. A
create-only put that meets an occupancy the preceding read missed re-reads the slot and answers by
the same rule; a re-read answering absence or occupancy is that occupancy, one refusing
`NOT_AUTHORIZED` or `REVISION_CONFLICT` travels as the destination reported it, and one faulting for
storage reasons becomes the fixed `InternalError` below rather than the put's own refusal. Any other
store answer becomes `InternalError` with the fixed message `knowledge adoption could not complete`
and `details.kind` naming the failing stage (`begin`, `commit`, `store` or `response`); the serde,
SQL and stored-payload text stays on the host side and never enters the signed refusal. A stored
`adoption_anchor` revision above `2^53 - 1` is the same fixed `store` fault, never a
`REVISION_CONFLICT` carrying the out-of-range integer — but only when that anchor names the
requested source scope and entry. An unrelated occupant's anchor is never read as this adoption's
revision, so it is the ordinary occupied conflict without `details`. The typed response is built
before the transaction settles, so a copy the response cannot describe rolls the write back instead
of committing one Connect would reject. Connect's `AdoptionPortService` dedups by
`(op, effective scope, requester principal id, idempotency_key)` with the whole-payload digest: a
same-key same-digest replay returns the recorded outcome with `outcome` normalized to
`already_present` and never reaches the port, a same-key different-digest replay answers
`IDEMPOTENCY_CONFLICT`, and a replay of an evicted key answers `RECEIPT_EXPIRED`. The bounded
in-memory default is the shipped store, so its records live for one service instance; see the dedup
consequence above for what a restart exposes.

**Per-peer hiding is a host obligation**, not a runtime setting. Connect signs its hello from one
fixed manifest and the runtime installs the surface's handlers on the responder before inbound
traffic reaches it, while admission learns its peer only from the established session. A host that
must hide adoption from particular peers therefore selects the surface and its matching allowlist
before hello using its own trusted routing, verifies the selected peer against the authenticated
session, and re-evaluates actual admission at call time. The runtime serves one manifest and one
handler set per surface and exposes no per-principal manifest or handler set.

**Stage 2 handoff: the single transaction is not in the retained ports.** `crates/mindcore-sqlite`
implements `ScopedKnowledgePort` and `HistoryPort` but no `spoke_operations::AdoptionPort`, and the
operation's body — the destination read, the retirement-chain resolution, the create-only write, the
anchor and inherited-governance rules and the typed response — lived in the removed
`crates/mindcore-runtime/src/adoption.rs` (`AdoptionService`). Its request checks ran before the
transaction: `source_scope_id` against the configured source (`NOT_FOUND`), `destination_scope_id`
against the admitted scope (`NOT_AUTHORIZED`) and a zero `source_revision` (`INVALID_INPUT`) were
answered with no transaction open, so they held no statement and no snapshot. Its one
`BEGIN IMMEDIATE` transaction then covered everything that reads or writes a row — the destination
read, the source's chain resolution, the create-only write, the anchor and the typed response — and
committed only for `adopted`, rolling back for `already_present` and for every refusal, so no caller
could observe a source read without its destination write and a refused response left no copy.
Stage 2 must re-implement that transaction before composing any per-Mind adoption face: a body
assembled from separate port calls cannot carry it, because the destination read and the create-only
write would no longer share the source read's snapshot or its rollback. The pre-transaction request
checks must stay outside it, where a refusal opens nothing. Connect's `AdoptionPortService` dedup
envelope is unchanged by this — it never carried the transactional guarantee.

### Store shape

The runtime serves a **protocol-shaped** store: the memory tables the workflows read are the Nexus protocol shape, not the pre-release legacy shape. `MindStore::open` applies the one embedded migration, so a freshly opened store already carries that shape and serves `recall_context` and `deliberate`; the loopback fixture opens that same production path and only seeds the rows the faces read, so it is no longer the only protocol-shaped store. Nexus host-only objects — the writer guards, outbox/immutability triggers, `nexus_writer_*` UDFs and the `core_*` tables — stay absent, so `open` needs no writer runtime. This plan's acceptance is therefore runtime reachability of the served tools against a store the production open path produced.

## Invariants

- No caller-provided principal, Mind id, or manifest capability can bypass a host admission or storage guard.
- No baseline provider is required for either face and an absent face/tool is denied. Advertisements and handler registrations describe the same capabilities.
- Adoption's source is the one host-configured Stage 2 route and its destination is the admitted Mind's registered collection, resolved after admission: no wire value selects either, the request's scope members are only compared against those two, a source equal to any Mind's registered collection or to the destination scope is refused before serving, and a `destination_scope_id` that names anything but the admitted Mind's scope answers the target-free refusal.
- Adoption's answer reports `destination_entry_id` slot occupancy — occupied or free — and nothing about the occupant: a concealed occupant is byte-identical to a visible one. That disclosure is permitted and no wider than the knowledge face it lands on, whose create-only `put_knowledge_entry` already distinguishes occupied from free while hiding the entry; entry content, revision and visibility stay undisclosed.
- Ablation: peer admission plus scoped storage are both required. Connect authenticates the peer and bounds the list request; the store applies the admitted collection and entry rules before reading, revealing or mutating data.
- An admitted scope is validated where the face is assembled and again in the store: neither Connect's list merge nor a caller-declared scope may drop or bypass an admitted constraint.
- History grants no access the knowledge face does not: both `history.*` ops admit through the knowledge face's two callbacks and the routed store's own `admit`, so a peer replays and prunes exactly the scope it reads and writes. Stage 1 composes no history face, so `face_capabilities` names no `spoke-history` and both ops are refused on the dispatch-deny branch — raw wire `capability_missing`, surfaced as `CapabilityPortMissing`.
- Every served `deliberate` response carries the judgment its own attempt produced, and every judgment it carries passed the released validator before the call returned; the identity rule [`mind-model.md`](mind-model.md) states holds on the wire, and `spoke-judgment` is advertised exactly with the tool.

## Where it lives

- SQLite scoped store beside `crates/mindcore-sqlite/src/knowledge.rs` and its borrowed-transaction helpers; Stage 2 binds it per Mind. The same file implements `HistoryPort` for `RoutedKnowledgeStore` and projects the replay onto `RevisionSnapshot`.
- `crates/mindcore-spoke/src/workflows.rs` owns workflow storage admission and the settlement transaction; `crates/mindcore-spi` owns the workflow contracts and their orchestration.
- `crates/mindcore-runtime/src/lib.rs` owns `ServeFaces` (manifest metadata, the memory service, the peer-to-principal admission, providers and host tools), the runtime's `MindPorts`, the retired-id filter and `face_capabilities`; Stage 1 composes no knowledge, history or adoption face, so the former `crates/mindcore-runtime/src/history_dedup.rs` (`SqliteHistoryDedupStore`) and `crates/mindcore-runtime/src/adoption.rs` (`AdoptionService`) are removed and a restored face must re-implement them (the two Stage 2 handoff paragraphs above). `spoke-connect`'s `HistoryPortService` and `AdoptionPortService` keep their wire decode, effective-scope check and dedup seams, unused until a face is composed again; `crates/mindcore-runtime/src/tools.rs` owns the served workflow tool registrations and their advertised ABI. `crates/mindcore-runtime/tests/loopback.rs` asserts each advertisement against a signed session, including the three removed families' `CAPABILITY_PORT_MISSING` denial; `crates/mindcore-runtime/tests/surface.rs` asserts their absence from the composed ports and the manifest.

## Change obligations

Changing an advertised face, tool id, argument/result shape, or scope admission rule updates this spec and the signed loopback assertions in the same change. This page is the served history contract's only home: a change to the refusal set, its `details` members, the `RevisionSnapshot` projection, the dedup store, its claiming rule, its namespace, its bound, its retention or its lifetime updates this page; a change to the local replay or retention semantics those answers carry updates `.agents/specs/data/knowledge-lifecycle.md` as well. This page is also the served adoption contract's only home: a change to the scope mapping, the terminal-anchor rule, the refusal vocabulary, the copy's inherited governance or the idempotency envelope updates this page, `serving-principal-routing.md` and the signed loopback assertions in the same change.
