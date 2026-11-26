# Architecture: Mindcore serving seam

## Scope

Signed Rust SPOKE sessions over a host-supplied transport: the Memory face, the four Mind tools, the composed per-Mind Knowledge and History faces, and the adoption face contract a restored per-Mind composition must satisfy. The library does not own a listener, accept loop, identity provisioning, publisher, or model provider.

## Current state

### Knowledge

The knowledge face is composed when `ServeFaces.knowledge` is true: `MindcoreServe::surface`
installs one `RemoteScopedKnowledgeFace { store, admission }` and `MindPorts::as_scoped_knowledge`
answers it, so `port.knowledge.get`, `port.knowledge.put` and
`port.scope.list_knowledge_entries` are served per Mind. The same flag advertises
`spoke-knowledge` and `spoke-knowledge-scoped` together, and with it false the three ops keep the
dispatch-deny answer — raw wire `op_unsupported`, which the Rust dialer surfaces as
`CapabilityPortMissing` with `details.wire_code`.

**`ke-ownership` is not advertised.** The admitted scope never carries a `viewpoint` — the
callback builds it with `viewpoint: None` — so `scope_matches_admission` can never match a request
that names one, and the face cannot honour the flag's one protocol gate. Advertising it would
claim a capability no admitted scope can satisfy; the face advertises only what it serves. A
request naming a `viewpoint` is therefore refused, never silently served: with the flag never
offered, no session can negotiate it, so Connect's ownership conjunct answers wire
`op_unsupported` before admission. Viewpoint support is not implemented in this layer.

`RoutedKnowledgeStore` stays the served storage port and `MindKnowledgePort` the
`ScopedKnowledgePort` the face installs over it; `ScopedKnowledgeStore` remains the in-process
local port beside `MindStore::knowledge`, not a served face.

The face is one composition for every Mind, and its admission is a callback, not a route map.
Connect supplies the authenticated peer id and, through A1, the `scope_id` the request declares or
none at all; the callback maps the peer through the host's own `principals` admission, then admits
that one Mind through `admitted_mind` — the caller's own live grant rows, the space row, then the
registry row that names the collection — and returns a `Scope` naming that Mind or refuses. A
selector that is absent, a Mind the registry does not name and a Mind the peer holds no live grant
in all stop in that sequence with one `NOT_AUTHORIZED` and no `details`, exactly as the memory
path's admission answers. There is no host-configured route map to consume and no fallback: no
collection is guessed from an entry id, `owner`, `viewpoint` or Memory `SpaceId`, and the collection
is resolved again on every call by the store's own `admit`, on the transaction the op's statement
runs on. Memory space and workflow knowledge grants remain separate and go through the existing
`MemoryWorkflowBinding` admission. The `ke-ownership` protocol gate would check a negotiated flag
for a list with an explicit `viewpoint`; it neither chooses the collection nor grants the peer
entry access, and this face does not advertise the flag (above).

**The caller the scope carries.** `ScopedKnowledgePort` carries no principal, so the callback binds the caller it admitted into the `Scope` it returns, under this workspace's own extensions namespace: `mindcore_caller` / `principal` (`mindcore_sqlite::bind_caller`, read back by `bound_caller`). `validate_admission_scope` admits that one namespace and refuses every other extension selection, and the callback runs both before it returns, so no scope leaves the face unbound. Binding on every successful return is an obligation, not a precaution: Connect's `effective_scope` merges admitted extension namespaces over the request's on conflict **and passes a request's extensions through unchanged when the admitted scope carries none**, so an admission that answered an unbound scope would let a peer-supplied `mindcore_caller` decide the caller. A Mind the callback cannot resolve to a live admitted grant is therefore refused, never answered with an unbound scope (`.agents/adr/implemented/architecture/2026-11-26-knowledge-caller-in-scope-extensions.md`).

That binding stays off the wire: neither the `mindcore_caller` namespace nor the principal id bound into the scope appears in any answer, at any depth and as a key as well as a value. One explicit exception: a protocol receipt's own `requester_principal_id` member, at the receipt's own path, holding the expected scalar caller id. That member is the caller's attribution of its own request, required by the receipt polling contract, and is not the admission binding; any other position, object key, nesting or value is a leak.

**The per-operation grant rule.** `mindcore_spoke::MindKnowledgePort` is the one `ScopedKnowledgePort` the face installs, wrapping the storage port (`RoutedKnowledgeStore`). Each of its three methods runs one order — the scope's shape, then the caller's own live grant in the Mind's space, then the wrapped store:

| Operation | One live grant in the Mind's space must cover | Because |
|---|---|---|
| `port.knowledge.get`, `port.scope.list_knowledge_entries` | `knowledge_read` | it reads |
| `port.knowledge.put` | `knowledge_write` **and** `knowledge_read` | it writes, and Connect reads the entry through `get` first, for the anchor check and the compare-and-swap |

**The put's authorization runs after Connect's own pre-read.** The served put's grant check lives inside `MindKnowledgePort`, so it runs after Connect's `call_knowledge_op` has already read the entry through the same port's `get` — the read it performs for the anchor check and the compare-and-swap — and after Connect has evaluated its own revision and anchor verdicts against that read. The consequence, stated plainly because it is observable: **a caller whose grant covers `knowledge_read` but not `knowledge_write` can receive `STORED_REVISION_STALE` or an adoption-anchor refusal on a put it is not authorized to make**, rather than the port's `NOT_AUTHORIZED`. The order is Connect's, not this workspace's, and this layer does not change it. It discloses no entry content: the caller already reads the same entry through `get` under the same `knowledge_read`, so a revision number or an anchor verdict tells it nothing its authorized read did not already tell it. What the read grant does not buy is the write — the port still refuses it, and no entry is created or changed.

Requirements never union across grants: a caller holding `knowledge_write` on one row and `knowledge_read` on another is refused, exactly as `one_grant_covers` already decides for memory. A caller holding one grant covering both appends `modules.cognition.records` exactly as before: the append is guarded, not removed. The port answers by what is wrong, never with one answer for every fault: a malformed scope — `extensions` present but not exactly the binding, which the refusal table below admits as `InvalidInput` — is a caller input error and never reaches the binding read; a well-formed scope with no binding, a caller with no live grant and a grant that does not cover the operation answer one fixed `NOT_AUTHORIZED` with no `details` in Connect's admission-deny words; and the grant read takes the pool, never sharing a snapshot with the store call. The guard is the protocol adapter's, not the storage crate's (`2026-11-20-storage-ir-layering.md`), so `RoutedKnowledgeStore` keeps no grant rule; a host that installs it directly skips the guard, and `MindcoreServe` never does.

**The gate order the cohort pins.** After the static capability gate, the responder applies the scope shape check, the marker and ownership conjuncts and the cognition gate, then decodes — and the order differs with the kind of scope op:

- **Shape, then marker, then ownership, then cognition** — the order the pinned cohort applies to the optional `get`/`put` case: `validate_scope_declaration` refuses a present malformed `scope` first; the marker conjunct then answers a `get` or `put` whose payload carries `scope` on a session that did not negotiate `spoke-knowledge-scoped` with wire `op_unsupported`, never a scope it would silently drop; the ownership conjunct answers a non-empty `scope.viewpoint` without `ke-ownership` with wire `op_unsupported`; then the cognition gate. This workspace's evidence covers that order in two parts, and no more: the raw wire tests establish **shape-before-marker** and **marker-before-face** for `get` and `put`; the **ownership conjunct** is established through the typed dialer on the list op, whose required scope makes ownership its only capability conjunct — and because this face advertises no `ke-ownership`, that conjunct always refuses a viewpoint list. The **cognition conjunct**, and ownership on the optional-scope ops, are the cohort's own and are not asserted here.
- **A required-scope query defers one shape failure.** On the list op — and the other required-scope query ops — a scope-declaration failure caused only by the cognition subtree is held as a deferred reject, the cognition gate runs first, and the reject is answered after it. Every other baseline shape failure still answers immediately. The marker conjunct never applies to these ops, since their `scope` is required rather than optional.
- **Decode sits between two different probes.** A payload that declares a `scope` is decoded before the ports-presence check, so a malformed scoped payload answers `INVALID_INPUT` even on a host with no ports face at all. A payload without `scope` keeps the earlier order: the ports-presence check runs first, its decode failure answers next, and only a decodable call reaches `serve_knowledge_op`, which probes `as_scoped_knowledge()` before the plain knowledge face and answers `op_unsupported` when neither exists.

Serving follows that probe: the peer, then the serve budget, then admission, then the request's scope compared with the admitted one (`scope_matches_admission` on `scope_id` and `viewpoint`, then `effective_scope` for widening), and only then the store. A refusal at any step above the store leaves the store untouched.

`list` names the admitted `scope_id` and `viewpoint`, and Connect bounds the request by the admitted scope before calling the store. `get` and `put` carry the optional `scope` member, with the negotiable `spoke-knowledge-scoped` marker saying the responder honours it: a request that names a scope on a responder without the marker is refused as unsupported — wire `op_unsupported`, `CapabilityPortMissing` in the Rust dialer — and never served from a default collection, because a dropped scope would land the read or write in a collection the caller never named. The scoped adapter binds the admitted Mind's collection on every statement. Within it, entry-id, entry-type, source-id and viewpoint filters use SPOKE's knowledge predicate (including owner-private disclosure); an explicit empty set matches nothing. The face does not serve cognition, timeline or extension-based selection; refuse unsupported constraints rather than silently ignoring them. List results are in `entry_id` order. The host admission callback validates the scope it returns with `mindcore_sqlite::validate_admission_scope` before returning it: Connect's list merge keeps only the request's cognition selection, so an admitted cognition selection reaches the collection query unless it is refused at admission. A served list reads at most `SERVED_KNOWLEDGE_LIST_LIMIT` (512) **live** rows of the admitted Mind's collection — the live predicate rides in the statement — and refuses a larger live set with `InternalError` naming the bound; the bound applies before disclosure filtering and never truncates silently. A retired row therefore never consumes list budget.

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

**The two contract obligations.** The authorization rests on two rules of Connect's that this workspace does not own: an admitted extension namespace overwrites a requested one, and — the half the first rule depends on — a request's extensions pass through unchanged when the admitted scope carries none. Both are verified against the responder's merge and neither is enforced here. Two signed loopbacks in `crates/mindcore-runtime/tests/loopback.rs` are the contract's adversarial tests, and they trip the first rule: a peer-supplied namespace must not decide the caller, and no answer may carry the binding. A cohort that merged extensions the other way would fail them.

- `a_peer_supplied_caller_namespace_does_not_override_the_admitted_caller`. A peer whose grant covers `knowledge_read` only sends list, get and put carrying `"extensions": {"mindcore_caller": {"principal": <another principal>}}` in its own request scope. List and get succeed on its own grant; put answers the fixed `NOT_AUTHORIZED` and writes nothing, while the named principal's own peer then creates the same entry. The no-scope put it also sends proves the admission boundary, not the binding rule: a missing selector reaches the callback as the empty id, so `admitted_mind` refuses before any scope is constructed or bound. It is a control for "no request scope still refuses", and the merge behaviour is tripped only by the forged-scope calls above.
- `no_wire_response_carries_the_caller`. Every answer to both peers — list, get, put, the refusals above, an absent entry, a widening scope and an unadmitted or unknown Mind — is serialized whole and asserted to name neither principal id nor the `mindcore_caller` namespace.

### History

The `spoke-history` family publishes two operations over the same signed session: `history.revisions.get` returns a `RevisionSnapshot`, `history.revisions.prune` returns `HistoryPruneResult { deleted, floor_revision }` where `floor_revision` is the local report's floor.

The history face is composed when `ServeFaces.history` is true: `MindcoreServe::surface` installs one `RemoteHistoryService` — `HistoryPortService` over `mindcore_spoke::MindHistoryPort` and Connect's durable dedup store — and `MindPorts::as_history` answers it, so both operations are served per Mind. The same flag advertises `spoke-history`, and with it false both ops keep the dispatch-deny answer — raw wire `capability_missing`, which the Rust dialer surfaces as `CapabilityPortMissing` with `details.wire_code`. `ServeFaces.history` requires `ServeFaces.knowledge`, because both ops admit through that face's steps; `history` without `knowledge` is an assembly `InvalidInput` refusal before any port or registration exists.

One store fills two port roles: `RoutedKnowledgeStore` implements `spoke_operations::HistoryPort` beside `ScopedKnowledgePort`, so no second store module exists and the runtime owns no second history implementation — only the dedup record beside it. The face's admission is the knowledge callback's steps one to three, installed as a `ScopedHistoryAdmission` — `Arc<dyn Fn(&str, Option<&str>) -> SpokeResult<(Principal, Scope)>>`: the authenticated peer id **and** the request's scope selector, answering the `Principal` and the admitted `Scope` — rather than the peer-only `HistoryAdmission`, and its body is the host's peer mapping, then `admitted_mind` over the selector, and then it **stops**. The returned scope carries the Mind and nothing else — no caller binding, no extension namespace — because Connect compares a present request scope with the admitted scope for wire equality before it calls the port, and a binding the request cannot reproduce would refuse every scoped call. A peer the host mapping or the Mind admission rejects gets that callback's refusal, and a peer the admission lets through can therefore replay and prune exactly the scope it reads and writes through `port.knowledge.*`. Connect's own pre-port comparison still answers `NOT_FOUND` on a replayed get and `NOT_AUTHORIZED` on a prune whose declared scope is not the admitted one, the dedup claim answers a recorded outcome before the port, and the port re-applies `RoutedKnowledgeStore::admit` as its own guard.

**The per-operation grant rule.** `mindcore_spoke::MindHistoryPort` is the enforcement point: the one `HistoryPort` the face installs, wrapping the routed store's own `HistoryPort` impl. Each method authorizes on the admitted `Principal` — the trait carries it, so history needs no `Scope.extensions` binding — by reading that caller's own live grant in the Mind's space, then calls the wrapped port:

| Operation | One live grant in the Mind's space must cover | Because |
|---|---|---|
| `history.revisions.get` | `knowledge_read` | it replays a knowledge entry's revision, the equivalent of a knowledge get |
| `history.revisions.prune` | `knowledge_write` **and** `knowledge_read`, on one grant | it writes, and it is the destructive companion of a knowledge put whose anchor read passes through `get` |

Requirements never union across grants, as on the knowledge face, and the grant read takes the pool without sharing a snapshot with the port call — the same recorded window (`I-000154`). The prune's dedup claim is not on this path: Connect admits the Mind, claims through `lookup`, and only then calls this port, so a refused prune settles its claim with the refusal and never reaches the store. A caller the port refuses therefore deletes nothing and replays nothing. The two faces' **authorization** refusals are one value byte for byte: `MindHistoryPort` delegates to the knowledge face's `authorize_knowledge`, so code, message and absent `details` are the knowledge put's or get's own answer for the equivalent access.

**Admission refusals are not byte-identical across the two faces.** Connect's knowledge branch replaces every admission `Reject` with its own constant — `NOT_AUTHORIZED`, `knowledge scope not authorized`, no `details` — while its history branch returns the callback's `Reject` verbatim. The **Mind-admission** cases are where that differs in text: an unknown Mind, an unadmitted Mind and a frozen Mind the caller holds no grant in all produce the runtime's own `NOT_AUTHORIZED`, `the caller is not admitted to the named mind`, no `details`, and within the history face those three are byte-identical to each other. Across the two families they share the code and the absence of `details` and differ in the message, so a refusal's text is not a stable cross-family contract; the code, that absence and the unreached port are. Two other callback outcomes are not that Mind-admission value and are not claimed to be: the shared callback first returns the host's `principals` rejection **unchanged** — the host's own code and message, which this runtime neither rewrites nor compares — and an unexpected storage fault inside the admission leaves through a separate fault channel, Connect answering that request with its own `INTERNAL_ERROR`; neither reaches a port either.

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
| A fault the work could not carry out — a statement that could not run on the grant read, the route or the checkpoint reads, a commit that did not land, or a contradicted history gap | `OUTCOME_UNCERTAIN` with `retry_class: "uncertain"`; the fault is not settled as the key's outcome and the claim is released, so a retry once the fault clears executes — provided that release succeeded, since a failed delete leaves the claim and the key answers `RECEIPT_EXPIRED` |
| Same `idempotency_key` with a different payload digest, when a recorded outcome exists to compare | `IDEMPOTENCY_CONFLICT`, none |
| A claim with no outcome — another caller's live claim, one whose process never settled, or a fresh key its principal had no quota to claim (4096 rows already held by that principal in the collection's namespace) | `RECEIPT_EXPIRED`, none; the prune is not re-executed |
| The Mind admission refuses — unknown Mind, unadmitted Mind, frozen Mind without a grant | `NOT_AUTHORIZED`, none; the runtime's Mind-admission message, returned verbatim by Connect's history branch, and neither the port nor the dedup claim is reached |

A key that is both a retained claim and a same-key different-payload retry answers `RECEIPT_EXPIRED`: there is no recorded outcome to conflict with, and the claim wins.

`HISTORY_NOT_RETAINED` is never answered: every revision at or above the floor is rebuilt from the next checkpoint or the live row, and a contradicted gap is storage corruption, not a retention gap.

Prune is deduplicated by Connect's recorded-outcome store, keyed by `(op, effective scope, requester principal id, idempotency_key)` and holding the request digest plus the settled outcome. The face composes `crates/mindcore-runtime/src/history_dedup.rs`'s `SqliteHistoryDedupStore` through `HistoryPortService::with_dedup_store`, over the same pool and database file as the knowledge store — one store, one pool, one process. A host therefore gets the durable store, not the released in-memory default, and no `ServeFaces` field selects between them.

`history_dedup` keys each row by the admitted Mind's registered collection and the retry identity, in two columns under the composite primary key `(namespace, key)`. The store takes no namespace at construction: `namespace_of` parses the key as a JSON array, reads its second member's string `scope_id` — the effective scope, which is the Mind — and resolves that Mind's collection through `mindcore_sqlite::minds::registered_collection` on every `lookup` and `record`. A key that is not a JSON array, one whose second member is not an object carrying a string `scope_id`, and one whose Mind has no registry row all fail closed: no namespace resolves, so the lookup answers `Evicted`, no row is claimed and no prune runs. That parse is the boundary, not a canonicality check: it does not compare the key with the canonical serialization, so a differently spelled or otherwise non-canonical tuple whose second member still names a registered Mind resolves a namespace and is claimed like any other. Connect generates the key from its own canonical tuple, so a peer cannot construct one through the wire; strengthening the parse is a separate question tracked as `I-000163` and is not this contract. A framed string cannot carry the namespace: a collection name is arbitrary, so any separator would let two `(collection, key)` pairs collide. Two Minds on one database can share a wire scope, a principal and an idempotency key while reading different collections; the namespace keeps the second from replaying or refusing the first's prunes, and a same-collection retry replays exactly as before. Resolving per call rather than at assembly also means a Mind created after the surface was built is deduplicated from its first prune and needs no reassembly.

A third column, `principal`, carries the requester the key names — the third member of Connect's canonical `[op, effective_scope, principal_id, idempotency_key]` tuple. Each `(namespace, principal)` pair is bounded at 4096 rows, claims included. `lookup` claims the key by inserting the row before the port runs and `record` upserts the digest and the settled outcome into it, so the claim, the port call and the record each take and release the pool's one connection in turn. Within one process Connect's default keyed single-flight still serializes same-key callers, which then replay the recorded outcome. A **settled outcome** is an execution result the prune reached — success, or one of the refusals that are decisions (`NOT_AUTHORIZED`, `NOT_FOUND`, `HISTORY_TRUNCATED`, `HISTORY_ENTITY_UNSUPPORTED`, `INVALID_INPUT`). A **storage fault** is not settled: `INTERNAL_ERROR` is the served path's whole fault vocabulary, so `is_storage_fault` reads that code and refuses the row, the claim is deleted so the key is claimable again, and the caller is answered `OUTCOME_UNCERTAIN` rather than a recorded fault. The rule reads the code, never the message text, and a code outside the list is settled and replayed — which keeps one transient failure from becoming this retry identity's permanent answer without ever re-executing a decided refusal. The release is why a faulted key can recover: it leaves no claim for a later `lookup` to answer `RECEIPT_EXPIRED`, so the same `idempotency_key` executes once the fault clears. Recovery therefore follows a release that succeeded, not the fault alone — the delete is an ordinary statement and can fail on the same database, in which case the claim survives and the key keeps answering `RECEIPT_EXPIRED`, the exposure the dead-process case already carries.

**Capacity.** At the bound an unseen key is not inserted: `lookup` answers `Evicted`, so Connect answers `RECEIPT_EXPIRED` and no destructive prune runs, and `record` writes no row that would cross the bound — neither the claim path nor the settle path can grow a quota past it. A key already present is unaffected, so recorded outcomes still replay and claims still refuse at the bound.

The bound is per `(namespace, principal)`, not per database and not per collection alone: every Mind registered to one collection shares that collection's namespace, but each caller holds its own quota inside it, so one high-churn principal can exhaust only its own and never disable prune for another Mind reading the same collection. A different collection has its own namespaces and quotas. Nothing is ever deleted and no clock is read, so a recorded outcome replays for the database's lifetime and no forward host-clock jump can age a live record into a re-executed prune. A caller that exhausts its quota gets refusals for fresh keys rather than a prune the store cannot dedup; recorded outcomes for existing keys keep working, a fresh key becomes available only if the table loses rows, which nothing here does, and a host that needs more headroom raises the cap or recreates the database.

The quota's principal is read the same way: `principal_of` takes the key's third member when it is a string, and any key that does not yield one — an unparsable key, an array too short, a non-string member — is charged to one shared `unknown` bucket rather than to a principal of its own, so such keys cannot mint fresh quotas. That bucket is a quota fallback for the quota only; it does not decide whether a row is claimed, which the namespace parse above does. A key that resolves a namespace but no principal still claims and replays normally under that shared bucket until it is full.

A claim with no outcome answers `RECEIPT_EXPIRED` and never reaches the port. Three states reach it: a port call still in flight, one whose serve loop died before `record`, and — if a release itself fails — a fault that was answered but not cleared. A **fault** is not one of those three by design: the store releases the claim when a settlement fails, the port answers a storage fault, or the outcome read after the claim insert fails, so a key that hit a transient failure is claimable again and executes once the fault clears. That release is best-effort, not a guarantee: a delete that itself fails leaves the claim, and the key then answers `RECEIPT_EXPIRED` exactly as a dead serve loop's claim does — the exposure stated below. Two processes serving one database therefore turn the later same-key caller into a refusal instead of waiting; one serving process per database stays the host obligation.

**Dedup consequence:** a fault is retried when its release succeeds; a crash is not. A prune whose serve loop died between its committed port call and `record` leaves a claim with no outcome, so a same-key retry across a restart — or a reassembled surface on the same file — answers `RECEIPT_EXPIRED` and never reaches the port: no checkpoint the original call never saw is deleted. That refusal is the price of at-most-once for the case the process cannot observe, and the retry does not replay the lost `{deleted, floor_revision}` either; closing it needs the retry identity to reach the port, which the released history contract does not pass. A **fault** is the case the process does observe: the store releases the claim before answering `OUTCOME_UNCERTAIN`, so once that release has succeeded the key is claimable again and the retry executes. The release is a delete like any other statement, so it can fail on the same database that just faulted; when it does, the claim survives and the key answers `RECEIPT_EXPIRED` on every later retry — the dead-process exposure above, reached by a different cause. Nothing is lost by that fallback, since the prune may have committed either way, and no answer in this path asserts it did not. The two outcomes therefore differ by cause and are honest in both directions: `OUTCOME_UNCERTAIN` says the outcome is unknown, and `RECEIPT_EXPIRED` says no outcome was recorded — which a crash, and a failed release, each make true.

**Quota consequence:** the bound costs headroom, not history. A recorded outcome replays for the database's lifetime, because nothing deletes and no clock is read, so a same-key retry never becomes a new request by aging — the failure a time-based window introduces, where a forward host-clock jump reclaims unexpired records and lets the destructive prune run again. What the bound does cost is a ceiling per caller: a principal that spends its 4096 rows is refused fresh keys until the host raises the cap or recreates the database, which is a refusal rather than a prune the store cannot dedup. Because the quota is per principal, that ceiling is the noisy caller's own and never another caller's on the same collection.

Because the claim is taken before the port runs, `RECEIPT_EXPIRED` reports one thing only: no recorded outcome for that key. It says nothing about whether the original request executed. Two states reach it — a key already present with no outcome behind it, and a key its principal had no quota to claim — and the caller cannot tell them apart, so neither is a usable distinction.

- **What history remains now** is observable: the entry's persisted `history_floor_revision` and which checkpoints still read. That state tells the caller whether another prune is still needed — nothing more; it does not attribute the floor to any particular request.
- **Whether the original request executed** is not observable. The same claim row covers a port that was never called, a port call still in flight, and a port call whose process died before `record` — and the last two may have committed.

So the answer is not a case distinction a caller can act on, and a caller must not read `RECEIPT_EXPIRED` as proof the prune did not commit. The persisted floor and surviving checkpoints are the only evidence available, and they decide whether re-pruning under any `idempotency_key` is still needed.

Adoption keeps the released in-memory default and its own restart exposure. Adoption has no equivalent permanent-loss replay path, but it is not a guaranteed no-write repeat either. While the source still resolves to the terminal the first call copied and the destination already holds the copy, the create-only CAS and the anchor rules make the retry answer `already_present` or `REVISION_CONFLICT` and write nothing. After a reassembly dropped the record, a source since retired to a different terminal resolves the same request to that new terminal; with `destination_entry_id` omitted the copy is then addressed by the new terminal's id, so the retry writes a second copy. The original copy stays intact and nothing is lost — the repeat is simply not guaranteed to write nothing once the terminal changed and no destination id was supplied.

### Workflow tools

`MindcoreServe` derives served capability ids and tool descriptors from installed faces and handlers; the host supplies identity, transport, allowed peers and non-capability manifest metadata. The runtime's own `MindPorts` answers `as_memory`, `as_scoped_knowledge` when `ServeFaces.knowledge` composes the knowledge face, and `as_history` when `ServeFaces.history` composes the history face over the same routed store; it takes the trait's default `None` for the adoption family, so a host cannot configure a second adoption face beside it. The Memory service consumes a trusted peer-to-principal mapping and implements the advertised family contract; the runtime does not create fake baseline, reflection or transfer providers. For each accepted transport, register handlers before releasing inbound traffic; `connect_responder` starts its own SPOKE receive task and does not install handlers from the manifest.

The memory face advertises `spoke-memory` whenever a service is attached, and `spoke-memory-transfer` only when that service reports all five §7.29–§7.33 operations through `mindcore_spoke::ServedMemory::operations`. `RemoteMemoryService` exposes one `invoke` seam and names no operation, so the service itself carries the answer: `ServedMemory` extends it with an operation list whose default is empty, and a bare `RemoteMemoryService` therefore never claims the transfer family. The derivation is the only advertisement list: `face_capabilities` compares [`TRANSFER_OPERATIONS`] against what the service reports, so no host-supplied string can claim a family the bound service cannot show it dispatches.

`mindcore_spoke::TransferService` is the service this crate binds: `operations()` returns the six operation strings it dispatches — the five §7.29–§7.33 transfers and §7.28 receipt polling (`memory.receipts.get`) — and `invoke` routes each of them to the matching `MemoryAdapter` call over the host's pool. `RemoteMemoryService::invoke` hands the service no transaction, so the service is the caller: each op runs in one transaction — `BEGIN IMMEDIATE` for the four mutations, reads for `exports.get` and `receipts.get` — and commits only after the adapter answers `Ready`; a commit whose acknowledgement is lost answers `OUTCOME_UNCERTAIN` rather than claiming either outcome. Any other `memory.*` op is refused as `CAPABILITY_PORT_MISSING`.

**`memory.spaces.create`.** The adapter owns the operation: `MemoryAdapter::create_space` (`crates/mindcore-spoke/src/control.rs`) implements it and delegates the write to the library's `mindcore_sqlite::minds::create` on the caller's transaction rather than opening a second create path. Routing it is a host's job, not a family this crate binds: the operation is a non-transfer `memory.*` member, so a host's own `ServedMemory` implementation answers it on the same terms as every other non-transfer op — one transaction the service owns, `BEGIN IMMEDIATE`, commit only after `Ready`. This crate binds only `TransferService`, whose six operations do not include it, so on that service `memory.spaces.create` is `CAPABILITY_PORT_MISSING`. In this layer the one place the routing exists is this repository's signed loopback host (`crates/mindcore-runtime/tests/loopback.rs`, `CreateSpaceFace`), which wraps the adapter in exactly that transaction and proves the create over the wire; no shipped surface routes it yet.

The request type makes `space_id` and `idempotency_key` required, and `deny_unknown_fields`; `display_name` is the only member that reaches the space row, and `extensions` is transport metadata that is not stored. `create_space` keeps the port contract's order (`ports.rs:333-339`), which differs from `space_status`'s authorize-probe-gate because a replay must still answer its receipt after a later freeze:

1. Decode, and project the op so the payload digest binds the request as sent.
2. Idempotency probe first: a retained receipt replays; the same key with another digest is `IDEMPOTENCY_CONFLICT`; an evicted body is `RECEIPT_EXPIRED`.
3. No space row is a new id and needs no grant — an authenticated principal creates. An existing row requires the caller's space-wide live `admin`; without it the answer is `NOT_AUTHORIZED` with no `details`, never the library's `MIND_ALREADY_EXISTS`.
4. Freeze gate, existing space only: `SPACE_FROZEN`.
5. New id: the library's `create` writes the space, the carrier, the registry row and the creator's one default grant row.
6. Existing space under a fresh key: no write to space, grant or revision, and `{space_id, status, created: false, revision}`.
7. The committed receipt on that same transaction, then the ready body.

**A create is an occupancy signal.** Step 3's two arms make the outcome of one create a report on the id: `created: true` means the id was free and this caller now holds its space-wide `admin` grant, while `NOT_AUTHORIZED` means an id the caller holds no `admin` in exists. The refusal itself leaks nothing — its code, message and absent `details` are the admission refusal — but which arm answers does, and any authenticated peer can send the call. The same caller can therefore claim a free id first and take its space-wide `admin` grant, after which the intended creator's create of that id answers the undetailed refusal. Both halves follow from two settled owner decisions, not from code: the caller names the `space_id` (a request-type member, stage 2's A3), and a new id needs no grant ("an authenticated principal creates"). Stage 1's unknown-equals-unadmitted invariant covers operations on an existing Mind; it does not cover id creation, because there is no row to admit against before the create writes one. A host that cannot accept this needs its own create admission — a peer-to-id policy in front of the operation — or a server-assigned id, so that no wire value decides which id is claimed. Nothing is decided here: the decision is tracked in `I-000155`, and this paragraph records the signal and its cause.

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
| `knowledge.adopt` (capability `spoke-knowledge-adoption`, which requires `spoke-knowledge`; not composed) | `source_scope_id`, `source_entry_id`, positive integer `source_revision`, `destination_scope_id`, optional `destination_entry_id`, `idempotency_key`, optional `extensions` | `{outcome, entry}` with `outcome` = `adopted` or `already_present`; `entry` is the stored copy either way |

`deliberate` takes no `holder`: the Mind's registry row names the carrier, and a peer that still
sends the retired member is refused `InvalidInput` by the closed object. Every served argument
object names a Mind through `space_id` and never a collection, a carrier or a principal.

An `absorb` run that updates a record and dismisses its candidate stages two intents in one transaction, the record update and the candidate dismissal, then collects the settled revisions from each staged intent's own result: a revision is yielded only for a record id the result names. The dismissal leg's result names no record, so only the update leg's result yields a revision; walking both legs adds nothing from the dismissal.

**The deliberation judgment.** Every successful `deliberate` response carries a required top-level `judgment` beside `status`, `verdict` and that branch's own members. The projection — target, revision-pinned evidence, outcome, basis, `judgment_id` identity and `created_at` — is [`mind-model.md`](mind-model.md)'s; this page owns only what the seam does with it. The workflow builds and validates the judgment in `crates/mindcore-spi/src/judgment.rs` and the tool boundary forwards it unchanged. A judgment the released validator refuses never reaches the wire: the call answers `InternalError` with `details.kind = "judgment_projection"` and no `retry_class`, and on the `settled` branch that check runs before the cognition record is committed, so a projection defect writes nothing. `spoke-judgment` is advertised exactly when `tools.mindcore.deliberate` is in the final registration list: the family has no operation of its own and gates nothing, so a surface that serves no `deliberate` advertises none.

An `absorb` replay reports what an earlier run of the same capture recorded for each consumed candidate. A candidate consumed by the update leg's dismissal is the one consumption whose result names no record, so the binding reads the update leg's own receipt — keyed by the capture key, the candidate id and `update` — for the record id: a committed receipt reports `settled` with that id and an absent one reports `dropped`. Capture and update receipts are rows of one table under one aged policy, and the binding has no per-operation retention branch; the capture receipt is minted first, so an aged-out capture receipt refuses the replay before an aged-out update receipt could be read.

Two of the three workflow tools reach knowledge at all, and only they: `deliberate`'s carrier admission and `recall_context`'s knowledge slot. Deliberation settles onto exactly one carrier entry, so the Mind the request names decides it: `space_id` is resolved through the Mind's registry row on the caller's transaction (`MemoryWorkflowBinding::carrier_collection`), and a binding an in-process facade opened may pin its own Mind first. Both readers — the carrier write and the recall slot — resolve through that one lookup, so no workflow consumer can reach a collection other than the admitted Mind's. `absorb` consults no registry row. Adoption resolves its destination from the same registry row after admission: it is the admitted Mind's registered collection, so a copy it writes is one the same peer reads back through that Mind's `knowledge.get`. A `space_id` with no registry row is the admission refusal, before any carrier access: one `NOT_AUTHORIZED` carrying a fixed message and no `details`, so it never reveals whether a Mind exists. The row never substitutes for a live grant — every carrier write and knowledge read still re-derives the caller's own grants on its own transaction. Recall's slot is independent of the memory page and best effort for the row and the grant: a missing row, or a missing live `knowledge_read` grant in the request's space, returns the memory result with `knowledge: []`, never a refusal. That tolerance covers the row and the grant only — a grant-store read or collection-list failure is not converted into an empty slot; it propagates and fails the recall. With both the row and the grant the slot lists only the admitted Mind's collection under the same disclosure predicate as the served knowledge list, so an `owner`-private entry never reaches the pack. The registry row is the workflow collection allowlist: `create` writes it, it is not read from the wire, and no served argument names a collection, so a peer cannot select one or change a mapping without creating a Mind. `recall_context` keeps serving for a space that is no Mind, with no slot to fill, and `absorb` keeps serving because it never consults the row. Memory authorization, withholding, snapshot revalidation and token budgeting are unchanged by routing and stay separate rules: the page is still the adapter's own ACL and withholding read, so a memory grant revoked while providers run refuses the reread as before, while a knowledge grant revoked mid-flight empties the reread's slot, which makes the snapshot differ and surfaces as the retryable `RevisionConflict` rather than a released pack.

Settlement returns its original `SpokeReject` (code, message and details); malformed arguments and `SpiError::InvalidInput` return `InvalidInput`. A `PolicyRejected` policy veto is also `InvalidInput`, with `details.kind = "policy_rejected"` and the host's reason as the message — never `NotAuthorized`, which stays the lost-admission code. A deadline returns `InternalError` with `details.kind = "deadline_exceeded"` and `details.retry_class = "retryable"`: nothing committed, so the caller may repeat the call. That no-write guarantee now also covers an expiry during the store-connection wait inside `commit_cognition`, and an expiry on a connection already in hand before the begin; it stops at `BEGIN IMMEDIATE`, whose wait for SQLite's write lock is outside the request deadline, and a write that has begun keeps its actual or uncertain outcome. [`mind-model.md`](mind-model.md) owns that boundary. The other provider errors return `InternalError` with `details.kind` set to `embeddings_unavailable`, `embedding_failed`, `reasoner_unavailable`, `reasoner_failed`, or `invalid_reasoner_output` as appropriate and carry no retry class; do not reflect provider internals into the wire message. Errors use the signed error envelope, never a success DTO. The client must negotiate each tool id it intends to invoke; descriptor `capability_id` and `op` match, and the responder owns the `mindcore` namespace.

**The deliberation budget's stated limit.** `deliberate`'s host-side pre-check runs inside the
request's budget: this implementation relies on the timer plus dropping the pool wait, so an expiry
starts no workflow, and it does not use SQLite's own interrupt handle — a query already inside
SQLite's step runs to that step's end and holds its connection until then. The remaining budget is
sampled after the pre-check, and the holder and evidence are built between that sample and the
workflow's SPI deadline, so their construction is counted in neither segment.

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
- Every served knowledge operation is authorized on the caller the admitted scope carries, per operation: a read needs one live grant covering `knowledge_read`, a write one grant covering both `knowledge_write` and `knowledge_read`. The admission binds that caller into every scope it returns, so Connect's merge can only keep the admitted value; a scope with no binding, a caller with no live grant and a grant that does not cover the operation are one `NOT_AUTHORIZED` with no `details`, and the caller binding never appears in a wire answer — except a receipt's own `requester_principal_id` attribution, scoped exactly as the caller paragraph above states.
- `memory.spaces.create` refuses in this order: dedup probe, then the existing-space `admin` requirement, then the freeze gate; a replay answers its retained receipt even after a later freeze, and an existing Mind without space-wide live `admin` answers `NOT_AUTHORIZED` and never a taken-id refusal. That refusal leaks nothing, but the operation's outcome distinguishes a taken id from a free one and a free id needs no grant, so the unknown-equals-unadmitted invariant is scoped to operations on an existing Mind and does not reach id creation; the signal, its cause and the two remedies a host may need are stated above and tracked in `I-000155`.
- A create writes the space, the carrier, the registry row, the creator's one default grant row and its committed receipt on the one transaction the host service opened; a repeat under a fresh key writes only its receipt, and nothing else changes.
- History grants no access the knowledge face does not: both `history.*` ops admit through the knowledge face's two callbacks and the routed store's own `admit`, so a peer replays and prunes exactly the scope it reads and writes, and `MindHistoryPort` authorizes each op on the caller's own live grant by the rule above. The face is composed only with the knowledge face, never beside it; `face_capabilities` advertises `spoke-history` exactly when it is.
- Every served `deliberate` response carries the judgment its own attempt produced, and every judgment it carries passed the released validator before the call returned; the identity rule [`mind-model.md`](mind-model.md) states holds on the wire, and `spoke-judgment` is advertised exactly with the tool.

## Where it lives

- SQLite scoped store beside `crates/mindcore-sqlite/src/knowledge.rs` and its borrowed-transaction helpers; the served face binds it per Mind through `RoutedKnowledgeStore`, which resolves the registered collection on every call. The same file implements `HistoryPort` for `RoutedKnowledgeStore` and projects the replay onto `RevisionSnapshot`.
- `crates/mindcore-spoke/src/workflows.rs` owns workflow storage admission and the settlement transaction; `crates/mindcore-spi` owns the workflow contracts and their orchestration.
- `crates/mindcore-spoke/src/control.rs` owns `MemoryAdapter::create_space`, `list_spaces`, `freeze_space` and `unfreeze_space`; `crates/mindcore-spoke/src/served_knowledge.rs` owns `MindKnowledgePort` and the `authorize_knowledge` check; `crates/mindcore-sqlite/src/knowledge.rs` owns `bind_caller`, `bound_caller` and `validate_admission_scope`.
- `crates/mindcore-runtime/src/lib.rs` owns `ServeFaces` (manifest metadata, the memory service, the knowledge and history switches, the peer-to-principal admission, providers and host tools), the runtime's `MindPorts`, `MindcoreServe::scoped_knowledge` and `MindcoreServe::served_history` (the two faces and the shared `admit_peer` admission), the retired-id filter and `face_capabilities`; the composed faces are `ServeFaces.knowledge` and `ServeFaces.history`, while adoption stays uncomposed, so `crates/mindcore-runtime/src/adoption.rs` (`AdoptionService`) remains removed and a restored face must re-implement it (the Stage 2 handoff paragraph above). `spoke-connect`'s `AdoptionPortService` keeps its wire decode, effective-scope check and dedup seam, unused until that face is composed again; `crates/mindcore-runtime/src/tools.rs` owns the served workflow tool registrations and their advertised ABI. `crates/mindcore-runtime/src/history_dedup.rs` owns `SqliteHistoryDedupStore`. `crates/mindcore-runtime/tests/loopback.rs` asserts each advertisement against a signed session — the knowledge face's served ops and its two adversarial loopbacks, the history face's per-Mind replay and prune with its grant and admission cases, and the adoption family's `CapabilityPortMissing` denial; `crates/mindcore-runtime/tests/surface.rs` asserts the three knowledge capabilities exactly when `ServeFaces.knowledge` is true, `spoke-history` exactly when `ServeFaces.history` is, and the assembly refusal of `history` without `knowledge`.

## Change obligations

Changing an advertised face, tool id, argument/result shape, or scope admission rule updates this spec and the signed loopback assertions in the same change. This page is the served history contract's only home: a change to the refusal set, its `details` members, the `RevisionSnapshot` projection, the dedup store, its claiming rule, its namespace, its bound, its retention or its lifetime updates this page; a change to the local replay or retention semantics those answers carry updates `.agents/specs/data/knowledge-lifecycle.md` as well. This page is also the served adoption contract's only home: a change to the scope mapping, the terminal-anchor rule, the refusal vocabulary, the copy's inherited governance or the idempotency envelope updates this page, `serving-principal-routing.md` and the signed loopback assertions in the same change.
