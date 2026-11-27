// The exported surface of `bindings/node/index.js`, hand-written because the addon is built
// without napi-rs's type-def feature. Every SPOKE wire object crosses as a plain object with its
// published field names unchanged, so each is one alias here and nothing is re-declared.

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

export interface Cognition {
  carrierRevision: number;
  records: object[];
  mindState: object;
}

/** SPOKE's `KnowledgeEntry`. */
export type KnowledgeEntry = object;
/** SPOKE's `MemoryCaptureResponse`. */
export type MemoryCaptureResponse = object;
/** SPOKE's `MemoryRecallResponse`. */
export type MemoryRecallResponse = object;
/** SPOKE's `MemoryCandidatesReviewResponse`. */
export type MemoryReviewResponse = object;
/** The `decision` member of the SPOKE review request. */
export type ReviewDecision = object;

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
