# Architecture: Mindcore serving seam

## Scope

Signed Rust SPOKE sessions over a host-supplied transport: the Memory and Knowledge faces and the three Mind workflow tools. The library does not own a listener, accept loop, identity provisioning, publisher, or model provider.

## Current state

### Knowledge

The host installs a `KnowledgeAdmission` callback in `RemoteKnowledgeFace`. Connect supplies the authenticated peer id; the callback returns the peer's one admitted `Scope` or refuses. An exact host-configured route maps the admitted `scope_id` to `KnowledgeScope.collection` on the bound SQLite pool. No fallback guesses a collection from an entry id, `owner`, `viewpoint`, or Memory `SpaceId`. Memory space and workflow knowledge grants remain separate and go through the existing `MemoryWorkflowBinding` admission. The `ke-ownership` protocol gate checks the negotiated flag for a list with an explicit `viewpoint`; it neither chooses the collection nor grants the peer entry access.

`get` and `put` carry no wire scope. `list` names the admitted `scope_id` and `viewpoint`; Connect bounds the request by the admitted scope before calling the store. The scoped adapter binds the collection on every statement. Within it, entry-id, entry-type, source-id and viewpoint filters use SPOKE's knowledge predicate (including owner-private disclosure); an explicit empty set matches nothing. The face does not serve cognition, timeline or extension-based selection; refuse unsupported constraints rather than silently ignoring them. List results are in `entry_id` order. The host admission callback validates the scope it returns with `mindcore_sqlite::validate_admission_scope` before returning it: Connect's list merge keeps only the request's cognition selection, so an admitted cognition selection reaches the collection query unless it is refused at admission. A served list reads at most `SERVED_KNOWLEDGE_LIST_LIMIT` (512) stored rows of the routed collection and refuses a larger collection with `InternalError` naming the bound; the bound applies before disclosure filtering and never truncates silently.

The peer's route grants read/write knowledge access within its scope, not unconstrained access to the database. `put` checks the proposed payload and any existing row against the admitted constraints on the same `BEGIN IMMEDIATE` transaction, before exposing a revision or committing a change. A missing expected revision keeps create-only semantics; a supplied one performs compare-and-swap through the common store. A hidden existing row is never overwritten, and hidden entry data/revisions are never returned. No operation probes another collection.

| Refusal | Result |
|---|---|
| Required negotiated family, conditional ownership flag or serving face missing | Wire `op_unsupported`, exposed by the Rust dialer as `CapabilityPortMissing` with `details.wire_code`. |
| Peer unadmitted or list names/widens another scope; unknown trusted route | `NotAuthorized` (`NOT_AUTHORIZED`), no target details. |
| Host admission returns an unsupported selection | `NotAuthorized`, no target details: refused at admission before Connect merges the scope, so the store is never reached. |
| Malformed scope/payload, or a caller-declared (request) scope with an unsupported selection | `InvalidInput`; the unsupported-selection case is refused at the store. The responder's earlier payload-dependent gates still apply. |
| Get id absent or hidden within the routed collection | One generic `KnowledgeEntryNotFound`. An out-of-scope put is `NotAuthorized`. |
| Admitted create hits an existing visible id; admitted update carries a stale/missing stored revision | `KnowledgeEntryAlreadyExists`; `StoredRevisionStale`, respectively. Preserve backend codes; storage failures are `InternalError`. |

### Workflow tools

`MindcoreServe` derives served capability ids and tool descriptors from installed faces and handlers; the host supplies identity, transport, allowed peers and non-capability manifest metadata. Compose Memory and Knowledge with `RemoteServePortsComposite::knowledge_only(face).with_memory(service)`. The Memory service consumes a trusted peer-to-principal mapping and implements the advertised family contract; the runtime does not create fake baseline, reflection or transfer providers. For each accepted transport, register handlers before releasing inbound traffic; `connect_responder` starts its own SPOKE receive task and does not install handlers from the manifest.

The host's peer resolver supplies the `Principal` from the authenticated responder session, not from arguments. `ToolHandler` receives only JSON arguments, so a handler binds to its responder session (weak reference, no ownership cycle). Missing peer/principal is `NotAuthorized`. Do not share a handler closure bound to one session with a different session. Provider capabilities and `DomainLens` are host inputs. Workflows continue to enforce their own live memory/knowledge grants and settlement boundaries.

Connect owns the signed `{ "arguments": {…} }` request and `{ "result": … }` success wrappers. The following keys define closed argument objects; `request`, `holder` and `target` use the corresponding existing SPOKE DTOs. Results use the listed fields and snake-case branch tags:

| Capability id and op | Arguments | Result |
|---|---|---|
| `tools.mindcore.recall_context` | `request` (`MemoryRecallRequest`), positive integer `max_tokens` | `{records, knowledge, tokens_used}` |
| `tools.mindcore.absorb` | `request` (`MemoryCaptureRequest`) | Array of `{status, candidate_id}` with `status` = `pending`, `dropped`, or `settled` (adds `record_ids`) |
| `tools.mindcore.deliberate` | `space_id`, `subject_id`, `holder`, `target`, `evidence` (`[{id, text}]`), positive integer `timeout_ms` | `{status, verdict}` with `status` = `no_change`, `needs_review` (adds `draft`), or `settled` (adds `record`, `mind_state_version`) |

Deliberation settles onto a carrier entry, so it needs a knowledge route the other two tools do not: `ServeFaces.workflow_routes` maps each exact principal id — not a peer id, and never a value read from the wire — to the collection that principal's workflow calls reach, and `MemoryWorkflowBinding::with_knowledge_routes` validates and takes ownership of the whole map once at assembly. Every binding operation resolves its route from the admitted principal through the one lookup `MemoryWorkflowBinding::knowledge_route`, so the carrier read and its CAS append stay pinned to one collection and recall's slot can never resolve a different collection for the same principal. Two principals may route to the same collection and use distinct carrier holder entry ids; the route grants no per-holder isolation inside a shared collection, and it never substitutes for a live grant — every carrier write and knowledge read still re-derives the caller's own grants on its own transaction. At carrier admission a principal with no route is refused before any carrier access, as terminal `NotAuthorized` carrying the fixed message `principal has no workflow knowledge route` and no collection, entry, caller id, or other route detail, so it never reveals whether another principal has a route. Recall's slot is independent of the memory page and stays best effort: a missing route or a missing live `knowledge_read` grant in the request's space returns the memory result with `knowledge: []`, never a refusal; with both a route and the grant the slot lists only the routed collection under the same disclosure predicate as the served knowledge list, so an `owner`-private entry never reaches the pack. The host map is the workflow collection allowlist: a valid route need not name a collection that already holds rows, repeated destination collections are valid, and no route is checked against the independent Knowledge face or resolved to a default collection — to change a mapping, assemble a new surface. Assembly returns `InvalidInput` for an empty principal key or an empty destination collection, before any port or registration exists; an empty map is valid and serves no deliberation, so it installs no `deliberate` handler and advertises neither the capability nor a tool descriptor, and a host registration of that reserved id is dropped rather than advertised or installed. `recall_context` and `absorb` keep serving without a route map: their knowledge slot is optional. Only the store's own failures still fail a recall (a grant read or collection list that errors); a grant revoked while providers run empties the reread's slot, which surfaces as the retryable snapshot conflict rather than a released pack.

Settlement returns its original `SpokeReject` (code, message and details); malformed arguments and `SpiError::InvalidInput` return `InvalidInput`. A `PolicyRejected` policy veto is also `InvalidInput`, with `details.kind = "policy_rejected"` and the host's reason as the message — never `NotAuthorized`, which stays the lost-admission code. A deadline returns `InternalError` with `details.kind = "deadline_exceeded"` and `details.retry_class = "retryable"`: nothing committed, so the caller may repeat the call. The other provider errors return `InternalError` with `details.kind` set to `embeddings_unavailable`, `embedding_failed`, `reasoner_unavailable`, `reasoner_failed`, or `invalid_reasoner_output` as appropriate and carry no retry class; do not reflect provider internals into the wire message. Errors use the signed error envelope, never a success DTO. The client must negotiate each tool id it intends to invoke; descriptor `capability_id` and `op` match, and the responder owns the `mindcore` namespace.

### Store shape

The runtime serves a **protocol-shaped** store: the memory tables the workflows read are the Nexus protocol shape, not the pre-release legacy shape. `MindStore::open` applies the one embedded migration, so a freshly opened store already carries that shape and serves `recall_context` and `deliberate`; the loopback fixture opens that same production path and only seeds the rows the faces read, so it is no longer the only protocol-shaped store. Nexus host-only objects — the writer guards, outbox/immutability triggers, `nexus_writer_*` UDFs and the `core_*` tables — stay absent, so `open` needs no writer runtime. This plan's acceptance is therefore runtime reachability of the served tools against a store the production open path produced.

## Invariants

- No caller-provided principal, scope route, or manifest capability can bypass a host admission or storage guard.
- No baseline provider is required for either face and an absent face/tool is denied. Advertisements and handler registrations describe the same capabilities.
- Ablation: peer admission plus scoped storage are both required. Connect authenticates the peer and bounds the list request; the store applies the admitted collection and entry rules before reading, revealing or mutating data.
- An admitted scope is validated where the face is assembled and again in the store: neither Connect's list merge nor a caller-declared scope may drop or bypass an admitted constraint.

## Where it lives

- SQLite scoped store beside `crates/mindcore-sqlite/src/knowledge.rs` and its borrowed-transaction helpers; used both by the Knowledge loopback and by `crates/mindcore-runtime`.
- `crates/mindcore-spoke/src/workflows.rs` owns workflow storage admission and settlement; `crates/mindcore-spi` owns the workflow contracts.

## Change obligations

Changing an advertised face, tool id, argument/result shape, or scope admission rule updates this spec and the signed loopback assertions in the same change.
