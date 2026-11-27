// The exported surface of `bindings/node/index.js`, hand-written because the addon is built
// without napi-rs's type-def feature. Every SPOKE wire object crosses as a plain object under its
// published field names; the shapes below mirror the serialized Rust structs of `spoke-schemas`
// (named beside each), so a caller can read a response without re-declaring it.

export interface MindcoreClientOptions {
  /** The one SQLite file this client serves. */
  storePath: string;
  /** The authenticated caller every facade call admits. */
  callerId: string;
  /** Journal root `review` publishes under. Absent: `review` refuses `INVALID_INPUT`. */
  journalDir?: string;
}

export interface CreateMindOptions {
  id?: string;
  displayName?: string;
}

export interface MindInfo {
  id: string;
  displayName: string | null;
  status: 'active' | 'frozen';
  knowledgeCollection: string;
  carrierEntryId: string;
  createdAt: string;
}

export interface CaptureInput {
  subject: string;
  context?: string;
  content: object;
  occurredAt?: string;
  kindHint?: string;
  idempotencyKey: string;
}

export interface RecallInput {
  subject: string;
  selection: object;
  maxItems: number;
  maxBytes?: number;
  query?: string;
  cursor?: string;
}

/**
 * SPOKE's `Decision` (`ops/memory_candidates_review_request.rs`): a union tagged on `action`.
 */
export type ReviewDecision =
  | { action: 'promote'; content_override?: object; kind?: string }
  | { action: 'fragment'; fragments: object[] }
  | { action: 'drop'; rationale?: string };

/** SPOKE's `CommittedReceipt`, the envelope every committed memory write answers. */
interface Receipt<TResult> {
  completed_at: string;
  created_at: string;
  extensions: Record<string, object>;
  idempotency_key: string;
  op: string;
  payload_digest: string;
  receipt_id: string;
  requester_principal_id: string;
  result: TResult;
  schema_version: number;
  space_id: string;
  status: string;
}

/** SPOKE's `MemoryCaptureResponse`; `CommittedReceiptResult` holds `candidate_ids`/`classified`. */
export type MemoryCaptureResponse = Receipt<{ candidate_ids: string[]; classified: number }>;

/** SPOKE's `MemoryCandidatesReviewResponse`; `Outcome` is `promoted|fragmented|dropped`. */
export type MemoryReviewResponse = Receipt<{
  outcome: 'promoted' | 'fragmented' | 'dropped';
  record_ids: string[];
}>;

/** SPOKE's `KnowledgeEntryBody`; an absent member is omitted, never `null`. */
export interface KnowledgeBody {
  attributes?: object[];
  computable?: Record<string, object>;
  state?: Record<string, object>;
  summary?: string;
  tags?: string[];
}

/** SPOKE's `KnowledgeEntry` (`data/knowledge_entry.rs`). Every optional member may be absent. */
export interface KnowledgeEntry {
  adoption_anchor?: object;
  body: KnowledgeBody;
  canonical_name: string;
  created_at?: string;
  disclosure?: string;
  entry_id: string;
  entry_type: string;
  extensions: Record<string, object>;
  /** Keyed by module name; `cognition` is the Mind's judgment module. */
  modules: Record<string, object | object[]>;
  owner?: string;
  revision?: number;
  schema_version: number;
  source_anchor?: object;
  status: string;
  updated_at?: string;
}

/** SPOKE's `Content` (`data/retained_record.rs`). */
export interface RecordContent {
  cognition?: object[];
  structured?: object;
  text: string;
}

/** SPOKE's `Provenance` (`data/retained_record.rs`). */
export interface Provenance {
  anchors: object[];
  captured_at: string;
  captured_by: string;
  source_context_ids: string[];
}

/**
 * SPOKE's `PlacementScope`: `shared`, or `context` with the context it is held in.
 */
export type PlacementScope =
  | { visibility: 'shared' }
  | { visibility: 'context'; context_id: string };

/** SPOKE's `RetainedRecord` (`data/retained_record.rs`), the element of a recall page. */
export interface RetainedRecord {
  content: RecordContent;
  created_at: string;
  extensions: Record<string, object>;
  import_ref?: object;
  kind: string;
  occurred_at?: string;
  provenance: Provenance;
  record_id: string;
  revision: number;
  schema_version: number;
  scope: PlacementScope;
  space_id: string;
  subject_id: string;
  tombstone?: object;
  updated_at: string;
}

/**
 * SPOKE's `MemoryRecallResponse`, whose success arm is `Variant0`. `next_cursor` and
 * `overflow_record_id` are omitted when absent, and an absent cursor ends the results.
 */
export interface MemoryRecallResponse {
  engine_policy_version: string;
  extensions?: Record<string, object>;
  next_cursor?: string;
  overflow_record_id?: string;
  records: RetainedRecord[];
  total_authorized: number;
  truncated: boolean;
}

/**
 * SPOKE's `MindState` (`data/mind_state.rs`). The facade emits five members and the schema admits
 * more optional ones, so this names what the binding always answers and admits the rest.
 */
export interface MindState {
  schema_version: number;
  mind_state_id: string;
  holder_entry_id: string;
  cognition: object;
  extensions: Record<string, object>;
  [member: string]: unknown;
}

export interface Cognition {
  /** The carrier revision the read served. */
  carrierRevision: number;
  records: object[];
  mindState: MindState;
}

/** A facade refusal: a SPOKE wire code, a message and the refusal's structured payload. */
export declare class MindcoreError extends Error {
  /**
   * The SPOKE wire code of the refusal (`NOT_FOUND`, `STORED_REVISION_STALE`, …), or
   * `BINDING_DEFECT` for an answer this addon could not render. Not a closed union: the facade
   * keeps adding codes, and a defect is not a refusal.
   */
  readonly code: string;
  /** The refusal's structured payload, absent when the refusal carries none. */
  readonly details: object | undefined;
}

export declare class MindcoreClient {
  /** Open the one store file this client serves. */
  static open(options: MindcoreClientOptions): Promise<MindcoreClient>;
  createMind(options?: CreateMindOptions): Promise<MindView>;
  listMinds(): Promise<MindInfo[]>;
  getMind(id: string): Promise<MindView>;
  /** Provision the subject row a memory write needs. */
  ensureSubject(mindId: string, subjectId: string): Promise<void>;
}

export declare class MindView {
  /** The Mind's registry id, read at open. */
  readonly id: string;
  /** The entry in the Mind's collection that holds its current judgment. */
  readonly carrierEntryId: string;
  knowledgeGet(entryId: string, revision?: number): Promise<KnowledgeEntry>;
  knowledgePut(
    entry: KnowledgeEntry,
    expectedBaseRevision?: number | null,
  ): Promise<KnowledgeEntry>;
  knowledgeList(): Promise<KnowledgeEntry[]>;
  capture(input: CaptureInput): Promise<MemoryCaptureResponse>;
  review(
    candidateId: string,
    decision: ReviewDecision,
    idempotencyKey: string,
  ): Promise<MemoryReviewResponse>;
  recall(input: RecallInput): Promise<MemoryRecallResponse>;
  /** The Mind's judgment, or `null` for a Mind that has judged nothing. */
  cognitionRead(): Promise<Cognition | null>;
}
