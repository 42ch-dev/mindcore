// The in-process Mindcore binding: the loader for the addon `cargo build -p mindcore-binding`
// produces, the one place a facade refusal becomes an error, and the two classes over the
// addon's opaque handles.

import Module from 'node:module';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ARTIFACT = `libmindcore_binding.${process.platform === 'darwin' ? 'dylib' : 'so'}`;

/** The addon: the one path `MINDCORE_BINDING_PATH` names, else the debug build, else release. */
function loadBinding() {
  const explicit = process.env.MINDCORE_BINDING_PATH;
  const candidates = explicit
    ? [explicit]
    : ['debug', 'release'].map((profile) =>
        join(fileURLToPath(new URL('../../', import.meta.url)), 'target', profile, ARTIFACT),
      );
  for (const path of candidates) {
    if (!existsSync(path)) continue;
    // `process.dlopen` on a fresh module loads the addon under the name cargo gave it, so
    // nothing is copied or renamed.
    const addon = new Module(path, null);
    process.dlopen(addon, path);
    return addon.exports;
  }
  throw new Error(
    `the mindcore binding is not built: no addon at ${candidates.join(', ')}; ` +
      'build it with cargo build -p mindcore-binding',
  );
}

const binding = loadBinding();

/** A facade refusal: `code` is the SPOKE wire code, `details` its structured payload. */
export class MindcoreError extends Error {
  constructor(code, message, details) {
    super(message);
    this.name = 'MindcoreError';
    this.code = code;
    this.details = details;
  }
}

/** One result object as the value it carries, or the refusal it answers. */
function settle(result) {
  if (result.ok) return result.value;
  throw new MindcoreError(result.code, result.message, result.details);
}

/** One store file, the caller every call admits, and the journal root `review` publishes under. */
export class MindcoreClient {
  #handle;

  constructor(handle) {
    this.#handle = handle;
  }

  static async open(options) {
    return new MindcoreClient(settle(await binding.openClient(options)));
  }

  async createMind(options) {
    return new MindView(settle(await binding.clientCreateMind(this.#handle, options)));
  }

  async listMinds() {
    return settle(await binding.clientListMinds(this.#handle));
  }

  async getMind(id) {
    return new MindView(settle(await binding.clientGetMind(this.#handle, id)));
  }

  async ensureSubject(mindId, subjectId) {
    settle(await binding.clientEnsureSubject(this.#handle, mindId, subjectId));
  }
}

/** One opened Mind; its registry id and carrier entry are read once, at open. */
export class MindView {
  #handle;
  #info;

  constructor(handle) {
    this.#handle = handle;
    this.#info = binding.mindViewInfo(handle);
  }

  get id() {
    return this.#info.id;
  }

  get carrierEntryId() {
    return this.#info.carrierEntryId;
  }

  async knowledgeGet(entryId, revision) {
    return settle(await binding.mindKnowledgeGet(this.#handle, entryId, revision));
  }

  async knowledgePut(entry, expectedBaseRevision) {
    return settle(await binding.mindKnowledgePut(this.#handle, entry, expectedBaseRevision));
  }

  async knowledgeList() {
    return settle(await binding.mindKnowledgeList(this.#handle));
  }

  async capture(input) {
    return settle(await binding.mindCapture(this.#handle, input));
  }

  async review(candidateId, decision, idempotencyKey) {
    return settle(await binding.mindReview(this.#handle, candidateId, decision, idempotencyKey));
  }

  async recall(input) {
    return settle(await binding.mindRecall(this.#handle, input));
  }

  async cognitionRead() {
    return settle(await binding.mindCognitionRead(this.#handle));
  }
}
