// The binding's one node:test file: the whole Mind flow of `crates/mindcore-runtime/examples/
// mind_facade.rs` through `index.js`, then the three refusals that are cheapest to get wrong.

import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, existsSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

import { MindView, MindcoreClient, MindcoreError } from './index.js';

const CALLER = 'caller_wren';
const SUBJECT = 'subj_wren';
const TEXT = 'the north gate is shut after dusk';

// Every store, its journal root and its keys live under one throwaway directory, and every other
// directory a case creates is tracked here too, so the run leaves nothing behind.
const temporaryDirectories = [];
function scratchDirectory(prefix) {
  const directory = mkdtempSync(join(tmpdir(), prefix));
  temporaryDirectories.push(directory);
  return directory;
}
const root = scratchDirectory('mindcore-binding-');
after(() => {
  for (const directory of temporaryDirectories) {
    rmSync(directory, { recursive: true, force: true });
  }
});

/** A client over its own store file and journal root under `root`; `journal` off keeps no publisher. */
function open(name, { journal = true } = {}) {
  return MindcoreClient.open({
    storePath: join(root, `${name}.db`),
    callerId: CALLER,
    journalDir: journal ? join(root, `${name}-journal`) : undefined,
  });
}

describe('the in-process binding', () => {
  it('runs one Mind from create through cognition', async () => {
    const client = await open('flow');

    const mind = await client.createMind({ displayName: 'Wren' });
    assert.ok(mind instanceof MindView);
    assert.match(mind.id, /^spc_/);
    assert.equal(mind.carrierEntryId, 'ke_carrier');

    await client.ensureSubject(mind.id, SUBJECT);

    const captured = await mind.capture({
      subject: SUBJECT,
      content: { text: TEXT },
      idempotencyKey: 'cap-north-gate',
    });
    const candidateId = captured.result.candidate_ids[0];
    assert.ok(candidateId, 'the capture stages one candidate');

    const reviewed = await mind.review(candidateId, { action: 'promote' }, 'rev-north-gate');
    assert.equal(reviewed.result.outcome, 'promoted');
    const recordId = reviewed.result.record_ids[0];

    const page = await mind.recall({
      subject: SUBJECT,
      selection: { mode: 'subject_all' },
      maxItems: 10,
    });
    const record = page.records.find((candidate) => candidate.record_id === recordId);
    assert.equal(record.content.text, TEXT, 'the recorded memory carries the captured text');

    // The carrier, then the same carrier with one current-view record: the write is one revision
    // step, the next read answers it, and the judgment derives from it.
    const carrier = await mind.knowledgeGet(mind.carrierEntryId);
    const baseRevision = carrier.revision;
    const written = withRecord(carrier, mind.carrierEntryId, mind.id, recordId);
    assert.equal((await mind.knowledgePut(written, baseRevision)).revision, baseRevision + 1);
    assert.equal((await mind.knowledgeGet(mind.carrierEntryId)).revision, baseRevision + 1);
    assert.equal((await mind.knowledgeList()).length, 1);

    const cognition = await mind.cognitionRead();
    assert.equal(cognition.carrierRevision, baseRevision + 1);
    assert.equal(cognition.records.length, 1);
    assert.equal(cognition.mindState.mind_state_id, mind.id);

    const minds = await client.listMinds();
    assert.equal(minds.length, 1);
    assert.deepEqual(
      { id: minds[0].id, status: minds[0].status, displayName: minds[0].displayName },
      { id: mind.id, status: 'active', displayName: 'Wren' },
    );

    // Replaying the write is the first refusal: the store answers the revision it holds.
    await assert.rejects(
      () => mind.knowledgePut(written, baseRevision),
      (error) => {
        assert.ok(error instanceof MindcoreError);
        assert.equal(error.code, 'STORED_REVISION_STALE');
        assert.deepEqual(error.details, {
          expected_base_revision: baseRevision,
          stored_revision: baseRevision + 1,
        });
        return true;
      },
    );
  });

  it('answers NOT_FOUND for a Mind that was never created', async () => {
    const client = await open('not-found');
    await assert.rejects(
      () => client.getMind('spc_never_created'),
      (error) => {
        assert.ok(error instanceof MindcoreError);
        assert.equal(error.code, 'NOT_FOUND');
        assert.equal(error.details, undefined);
        return true;
      },
    );
  });

  it('refuses a journalDir that is not a directory as invalid input', async () => {
    // A caller-supplied path is the caller's mistake to hear about, with the path named and the
    // io kind carried — not an internal fault.
    const path = join(root, 'not-a-directory');
    writeFileSync(path, 'this is a file');
    await assert.rejects(
      () =>
        MindcoreClient.open({
          storePath: join(root, 'refused.db'),
          callerId: CALLER,
          journalDir: join(path, 'journal'),
        }),
      (error) => {
        assert.ok(error instanceof MindcoreError);
        assert.equal(error.code, 'INVALID_INPUT');
        assert.equal(error.details.journalDir, join(path, 'journal'));
        assert.equal(typeof error.details.kind, 'string');
        return true;
      },
    );
  });

  it('refuses review on a client opened without a journal root', async () => {
    // No `journalDir`: the handle keeps no publisher, and `review` refuses before staging.
    const client = await open('no-journal', { journal: false });
    const mind = await client.createMind({ displayName: 'Journal-free' });
    await client.ensureSubject(mind.id, SUBJECT);
    const captured = await mind.capture({
      subject: SUBJECT,
      content: { text: TEXT },
      idempotencyKey: 'cap-no-journal',
    });
    await assert.rejects(
      () =>
        mind.review(captured.result.candidate_ids[0], { action: 'promote' }, 'rev-no-journal'),
      (error) => {
        assert.ok(error instanceof MindcoreError);
        assert.equal(error.code, 'INVALID_INPUT');
        return true;
      },
    );
  });

  it('provisions subjects on one store from two clients while the other writes', async () => {
    // `ensureSubject` takes the write lock up front (`BEGIN IMMEDIATE`): a deferred transaction that
    // upgrades after another connection's commit cannot be waited out, and SQLite answers
    // SQLITE_BUSY_SNAPSHOT instead of retrying. Both clients share one file, so their write locks
    // interleave — every call must settle as success.
    const storePath = join(root, 'race.db');
    const writer = await MindcoreClient.open({
      storePath,
      callerId: CALLER,
      journalDir: join(root, 'race-journal'),
    });
    const mind = await writer.createMind({ displayName: 'Raced' });
    const other = await MindcoreClient.open({
      storePath,
      callerId: CALLER,
      journalDir: join(root, 'race-journal'),
    });

    const provisions = Array.from({ length: 8 }, (_, index) =>
      other.ensureSubject(mind.id, `subj_race_${index}`),
    );
    const writes = [
      writer.ensureSubject(mind.id, 'subj_race_writer'),
      mind.capture({
        subject: 'subj_race_writer',
        content: { text: TEXT },
        idempotencyKey: 'cap-race',
      }),
    ];
    // Every one of these must resolve: a busy or stale-snapshot refusal fails the case.
    await Promise.all([...provisions, ...writes]);
  });

  it('creates a Mind when createMind is called with no argument', async () => {
    // The declaration says `options?`; the wrapper's default keeps that true against an addon
    // argument that is otherwise undefined.
    const client = await open('defaults');
    const mind = await client.createMind();
    assert.ok(mind instanceof MindView);
    assert.match(mind.id, /^spc_/);
    const minds = await client.listMinds();
    assert.deepEqual(
      minds.map((row) => row.id),
      [mind.id],
      'the no-argument create registered exactly that Mind',
    );
    assert.equal(minds[0].displayName, null, 'an unnamed Mind carries no display name');
  });

  it('refuses an empty journalDir as a name-shape error', async () => {
    // `''` names no directory at all: the caller's spelling, refused as such rather than as a
    // filesystem fault.
    await assert.rejects(
      () =>
        MindcoreClient.open({
          storePath: join(root, 'empty-journal.db'),
          callerId: CALLER,
          journalDir: '',
        }),
      (error) => {
        assert.ok(error instanceof MindcoreError);
        assert.equal(error.code, 'INVALID_INPUT');
        assert.equal(error.details.journalDir, '');
        return true;
      },
    );
  });

  it('opens a store through a symlink to a file, and refuses one to a directory', async () => {
    const target = join(root, 'real.db');
    const linkedFile = join(root, 'linked.db');
    symlinkSync(target, linkedFile);
    const client = await MindcoreClient.open({ storePath: linkedFile, callerId: CALLER });
    const mind = await client.createMind({ displayName: 'Through a link' });
    assert.match(mind.id, /^spc_/);

    const directory = join(root, 'a-directory');
    mkdirSync(directory);
    const linkedDirectory = join(root, 'linked-directory');
    symlinkSync(directory, linkedDirectory);
    await assert.rejects(
      () =>
        MindcoreClient.open({
          storePath: linkedDirectory,
          callerId: CALLER,
        }),
      (error) => {
        assert.ok(error instanceof MindcoreError);
        assert.equal(error.code, 'INVALID_INPUT');
        assert.equal(error.details.kind, 'is a directory');
        return true;
      },
    );
  });

  it('refuses ensureSubject for a caller without a grant on that Mind', async () => {
    // The binding performs no authorization of its own: the adapter's admission answers, so the
    // refusal arrives with its own code rather than one the binding invented.
    const owner = await open('authorized');
    const mind = await owner.createMind({ displayName: 'Wren' });
    const stranger = await MindcoreClient.open({
      storePath: join(root, 'authorized.db'),
      callerId: 'caller_stranger',
      journalDir: join(root, 'authorized-journal'),
    });
    await assert.rejects(
      () => stranger.ensureSubject(mind.id, SUBJECT),
      (error) => {
        assert.ok(error instanceof MindcoreError);
        assert.equal(error.code, 'NOT_AUTHORIZED');
        return true;
      },
    );
  });

  it('keeps a relative journal root pointing where it did at open', async () => {
    // A relative root names the directory that was current at open. The client retains the
    // resolved absolute path, so a later working-directory change cannot move the publication.
    const previous = process.cwd();
    const elsewhere = scratchDirectory('mindcore-elsewhere-');
    const journalName = 'relative-journal';
    let mind;
    let candidateId;
    try {
      process.chdir(root);
      const client = await MindcoreClient.open({
        storePath: join(root, 'relative.db'),
        callerId: CALLER,
        journalDir: journalName,
      });
      mind = await client.createMind({ displayName: 'Relative' });
      await client.ensureSubject(mind.id, SUBJECT);
      const captured = await mind.capture({
        subject: SUBJECT,
        content: { text: TEXT },
        idempotencyKey: 'cap-relative',
      });
      candidateId = captured.result.candidate_ids[0];
    } finally {
      process.chdir(previous);
    }

    // The publication happens with the process elsewhere: only a root resolved at open lands in
    // the directory the root named.
    process.chdir(elsewhere);
    try {
      await mind.review(candidateId, { action: 'promote' }, 'rev-relative');
    } finally {
      process.chdir(previous);
    }

    assert.ok(
      existsSync(join(root, journalName, 'records')),
      'the review published under the root the open named, not the directory current at review',
    );
    assert.ok(
      !existsSync(join(elsewhere, journalName)),
      'the directory current at publication gained nothing',
    );
  });
});

/** The carrier with one current-view cognition record appended, as the facade example builds it. */
function withRecord(carrier, carrierEntryId, mindId, recordId) {
  const holder = { kind: 'knowledge_entry', namespace: 'mindcore', id: carrierEntryId };
  return {
    ...carrier,
    modules: {
      ...(carrier.modules ?? {}),
      cognition: {
        records: [
          {
            kind: 'belief',
            order: 1,
            record_id: 'cog_north_gate',
            holder,
            target: { carrier: holder, target_id: 'prop_north_gate_shut', kind: 'proposition' },
            assessor: { kind: 'principal', namespace: 'mindcore', id: CALLER },
            assessor_role: 'holder_self_report',
            basis: {
              kind: 'evidence',
              evidence: [{ kind: 'memory_record', namespace: 'mindcore', id: recordId }],
            },
            assessment_context: { context_id: mindId },
            assessed_at: '2026-01-01T00:30:00Z',
            effective_at: '2026-01-01T00:30:00Z',
            view: 'current',
            q: {
              value: 0.8,
              assertion: 'belief_attribution',
              method: 'two_witness_reports',
              calibration: 'uncalibrated',
            },
            extensions: {},
          },
        ],
        world_facts: [],
        extensions: {},
      },
    },
  };
}
