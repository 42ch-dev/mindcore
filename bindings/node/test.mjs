// The binding's one node:test file: the whole Mind flow of `crates/mindcore-runtime/examples/
// mind_facade.rs` through `index.js`, then the three refusals that are cheapest to get wrong.

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

import { MindView, MindcoreClient, MindcoreError } from './index.js';

const CALLER = 'caller_wren';
const SUBJECT = 'subj_wren';
const TEXT = 'the north gate is shut after dusk';

// Every store, its journal root and its keys live in this throwaway directory, which is removed
// after the run; nothing here is committed and no path is reused between runs.
const root = mkdtempSync(join(tmpdir(), 'mindcore-binding-'));
after(() => rmSync(root, { recursive: true, force: true }));

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
