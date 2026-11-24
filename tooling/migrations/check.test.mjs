// The published set is compared against the lock at the baseline tag, so the guard is
// driven through the real script in a throwaway git repo.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const HERE = dirname(fileURLToPath(import.meta.url));

function git(root, ...args) {
  const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' });
  assert.equal(result.status, 0, `git ${args.join(' ')}: ${result.stderr}`);
  return result.stdout.trim();
}

const hash = (path) => createHash('sha256').update(readFileSync(path)).digest('hex');

function scratch() {
  const root = mkdtempSync(join(tmpdir(), 'migrations-check-'));
  cpSync(join(HERE, 'check.mjs'), join(root, 'tooling', 'migrations', 'check.mjs'), {
    recursive: true,
  });
  cpSync(join(HERE, '..', 'git', 'baseline.mjs'), join(root, 'tooling', 'git', 'baseline.mjs'));
  const migrations = join(root, 'crates', 'mindcore-sqlite', 'migrations');
  mkdirSync(migrations, { recursive: true });
  mkdirSync(join(root, 'tooling', 'migrations'), { recursive: true });

  const writeMigration = (file, body) => writeFileSync(join(migrations, file), body);
  const writeLock = (files) =>
    writeFileSync(
      join(root, 'tooling', 'migrations', 'migrations.lock.json'),
      `${JSON.stringify(files.map((file) => ({ file, sha256: hash(join(migrations, file)) })), null, 2)}\n`,
    );
  const commit = (message) => {
    git(root, 'add', '--all');
    git(root, 'commit', '--quiet', '-m', message);
  };
  const run = (args = []) =>
    spawnSync(process.execPath, ['tooling/migrations/check.mjs', ...args], { cwd: root, encoding: 'utf8' });
  const readLock = () => readFileSync(join(root, 'tooling', 'migrations', 'migrations.lock.json'), 'utf8');

  writeMigration('0001_init.sql', 'CREATE TABLE a (x);\n');
  writeLock(['0001_init.sql']);
  git(root, 'init', '--quiet', '--initial-branch', 'main');
  git(root, 'config', 'user.email', 'test@example.invalid');
  git(root, 'config', 'user.name', 'test');
  git(root, 'config', 'tag.gpgsign', 'false');
  commit('first published set');

  return { root, writeMigration, writeLock, commit, run, readLock, migrations };
}

test('an unlocked first published set passes', () => {
  const s = scratch();
  assert.equal(s.run().status, 0);
  rmSync(s.root, { recursive: true, force: true });
});
test('a present migration file the guard cannot parse fails loudly', () => {
  const s = scratch();
  s.writeMigration('1_short.sql', 'CREATE TABLE b (x);\n');

  const result = s.run();
  assert.equal(result.status, 1);
  assert.match(result.stderr, /1_short\.sql/);
  assert.match(result.stderr, /cannot parse/);
  rmSync(s.root, { recursive: true, force: true });
});


test('a published migration edited with its lock hash updated in the same change fails', () => {
  const s = scratch();
  git(s.root, 'tag', 'v0.1.0');
  s.writeMigration('0001_init.sql', 'CREATE TABLE a (x, y);\n');
  s.writeLock(['0001_init.sql']);
  s.commit('edit a published migration');

  const result = s.run();
  assert.notEqual(result.status, 0, 'an edited published migration must fail the check');
  assert.match(result.stderr, /changed its lock hash since v0\.1\.0/);
  rmSync(s.root, { recursive: true, force: true });
});

test('a published migration edited alone fails on the file bytes', () => {
  const s = scratch();
  git(s.root, 'tag', 'v0.1.0');
  s.writeMigration('0001_init.sql', 'CREATE TABLE a (x, y);\n');
  s.commit('edit a published migration without touching the lock');

  const result = s.run();
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /was edited since v0\.1\.0/);
  rmSync(s.root, { recursive: true, force: true });
});

test('an appended migration passes and one numbered before a published prefix fails', () => {
  const s = scratch();
  git(s.root, 'tag', 'v0.1.0');

  s.writeMigration('0002_next.sql', 'CREATE TABLE b (x);\n');
  s.writeLock(['0001_init.sql', '0002_next.sql']);
  s.commit('append 0002');
  assert.equal(s.run().status, 0);

  // Same release: a new file numbered below the published prefix is not an append.
  rmSync(join(s.migrations, '0002_next.sql'));
  s.writeMigration('0000_early.sql', 'CREATE TABLE c (x);\n');
  s.writeLock(['0000_early.sql', '0001_init.sql']);
  s.commit('insert before the published prefix');
  const result = s.run();
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /append-only/);
  rmSync(s.root, { recursive: true, force: true });
});

test('--write-lock folds an appended migration into the lock and is idempotent', () => {
  const s = scratch();
  s.writeMigration('0002_x.sql', 'CREATE TABLE b (x);\n');

  const first = s.run(['--write-lock']);
  assert.equal(first.status, 0, first.stderr);
  const lock = JSON.parse(s.readLock());
  assert.deepEqual(
    lock.map((entry) => entry.file),
    ['0001_init.sql', '0002_x.sql'],
  );
  assert.equal(lock[1].sha256, hash(join(s.migrations, '0002_x.sql')));

  const written = s.readLock();
  const second = s.run(['--write-lock']);
  assert.equal(second.status, 0, second.stderr);
  assert.equal(s.readLock(), written, 'a second --write-lock must not change the lock');
  rmSync(s.root, { recursive: true, force: true });
});

test('two migrations sharing a prefix fail both modes and leave the lock untouched', () => {
  const s = scratch();
  s.writeMigration('0002_x.sql', 'CREATE TABLE b (x);\n');
  assert.equal(s.run(['--write-lock']).status, 0);
  s.commit('lock 0002_x');
  const written = s.readLock();

  s.writeMigration('0002_y.sql', 'CREATE TABLE c (x);\n');

  for (const args of [[], ['--write-lock']]) {
    const result = s.run(args);
    assert.equal(result.status, 1, `check${args.length ? ' --write-lock' : ''} must fail`);
    assert.match(result.stderr, /0002_x\.sql/);
    assert.match(result.stderr, /0002_y\.sql/);
    assert.equal(s.readLock(), written, 'a failed check must not rewrite the lock');
  }
  rmSync(s.root, { recursive: true, force: true });
});

test('dropping a published migration from the lock fails', () => {
  const s = scratch();
  git(s.root, 'tag', 'v0.1.0');
  rmSync(join(s.migrations, '0001_init.sql'));
  s.writeLock([]);
  s.commit('drop a published migration');

  const result = s.run();
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /cannot be dropped from the published set/);
  rmSync(s.root, { recursive: true, force: true });
});
