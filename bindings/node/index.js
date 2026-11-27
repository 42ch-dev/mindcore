// The in-process Mindcore binding: the loader for the addon `cargo build -p mindcore-binding`
// produces, the one place a facade refusal becomes an error, and the two classes over the
// addon's opaque handles.

import Module from 'node:module';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * The hosts this package is built and tested on — the same set `package.json`'s `os` field
 * declares. The artifact names below are keyed by exactly these entries, so a platform cannot be
 * in one list and not the other; a host missing here is refused by name, which is the truth,
 * rather than being told the addon is unbuilt.
 */
const PLATFORMS = ['darwin', 'linux'];

/** The addon's file name as cargo writes it, per declared platform. */
const ARTIFACTS = {
  darwin: 'libmindcore_binding.dylib',
  linux: 'libmindcore_binding.so',
};

// The table and the list cannot drift: every declared platform names an artifact.
for (const platform of PLATFORMS) {
  if (!ARTIFACTS[platform]) {
    throw new Error(`no addon name for the declared platform ${platform}`);
  }
}

/** The addon at one path, loaded under the name cargo gave it: nothing is copied or renamed. */
function openAddon(path) {
  const addon = new Module(path, null);
  process.dlopen(addon, path);
  return addon.exports;
}

/** The addon: the one path `MINDCORE_BINDING_PATH` names, else the debug build, else release. */
function loadBinding() {
  const explicit = process.env.MINDCORE_BINDING_PATH;
  if (explicit) {
    if (!existsSync(explicit)) {
      throw new Error(`MINDCORE_BINDING_PATH names ${explicit}, which does not exist`);
    }
    return openAddon(explicit);
  }
  const artifact = ARTIFACTS[process.platform];
  if (!artifact) {
    throw new Error(
      `the mindcore binding ships for ${PLATFORMS.join(', ')}; ${process.platform} is not one of ` +
        'them. A build for this host can still be named with MINDCORE_BINDING_PATH.',
    );
  }
  const candidates = ['debug', 'release'].map((profile) => ({
    profile,
    path: join(fileURLToPath(new URL('../../', import.meta.url)), 'target', profile, artifact),
  }));
  const found = candidates.filter((candidate) => existsSync(candidate.path));
  if (found.length === 0) {
    throw new Error(
      `the mindcore binding is not built: no addon at ${candidates.map((c) => c.path).join(', ')}; ` +
        `build it with cargo build -p mindcore-binding. It ships for ${PLATFORMS.join(', ')}.`,
    );
  }
  // Debug wins, as it always has. When a release build also exists, say so once: a measurement
  // that meant release must not silently run debug code, and the two paths are indistinguishable
  // from the outside.
  if (found.length > 1) {
    process.emitWarning(
      `the mindcore binding loaded the ${found[0].profile} addon at ${found[0].path}, ` +
        `but a ${found[1].profile} build also exists at ${found[1].path}; ` +
        'name MINDCORE_BINDING_PATH to choose one deliberately.',
      'MindcoreBindingShadow',
    );
  }
  return openAddon(found[0].path);
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

  async createMind(options = {}) {
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
