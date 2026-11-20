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

Deliberation settles onto a carrier entry, so it needs a knowledge route the other two tools do not: the host names one caller principal with `ServeFaces.workflow_caller`, and the runtime binds that caller to the collection `ServeFaces.knowledge` names (`MemoryWorkflowBinding::with_knowledge_collection`). One served `deliberate` therefore serves exactly one caller principal over exactly one collection, and the workflow engine refuses a principal that is not that caller; per-principal routing is a later shape window, not implemented here. That caller match is the **carrier write's** authority only: recall's knowledge slot is admitted independently, by the caller's own live `knowledge_read` grant in the request's space against the configured collection, so a wired route never narrows another admitted peer's recall. That slot is **best effort** and never decides the recall's authorization outcome: a caller without the grant — or a surface with no collection bound — receives an empty slot beside its memory records instead of a refusal. Only the store's own failures still fail the call (a grant read or collection list that errors); a grant revoked while providers run empties the reread's slot, which surfaces as the retryable snapshot conflict rather than a released pack. A surface assembled with no caller serves no deliberation, so it installs no `deliberate` handler and advertises neither the capability nor a tool descriptor. `recall_context` and `absorb` keep serving without that route: their knowledge slot is optional. The no-route invariant holds over the final registration set: a host-supplied registration using `tools.mindcore.deliberate` is dropped rather than advertised or installed.

Settlement returns its original `SpokeReject` (code, message and details); malformed arguments and `SpiError::InvalidInput` return `InvalidInput`. `PolicyRejected` returns `NotAuthorized` with `details.kind = "policy_rejected"`. Deadline and provider errors return `InternalError` with `details.kind` set to `deadline_exceeded`, `embeddings_unavailable`, `embedding_failed`, `reasoner_unavailable`, `reasoner_failed`, or `invalid_reasoner_output` as appropriate; do not reflect provider internals into the wire message. Errors use the signed error envelope, never a success DTO. The client must negotiate each tool id it intends to invoke; descriptor `capability_id` and `op` match, and the responder owns the `mindcore` namespace.

### Store shape

The runtime serves a **protocol-shaped** store: the memory tables the workflows read are the Nexus protocol shape, not the pre-release legacy shape. A freshly opened `MindStore` is **not yet protocol-shaped** — `0001_init.sql` creates the legacy `memory_records` / `memory_candidates` / `review_receipts` tables and no protocol tables — so such a database cannot serve `recall_context` or `deliberate` until the store migration lands (issue `I-000068`, captured and out of this plan's scope). This plan's acceptance is therefore runtime reachability on a protocol-shaped host store: the loopback fixture installs the protocol DDL over a dropped legacy shape and is **not** the production open path.

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
