import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { baselineTag } from '../git/baseline.mjs';
import {
  checkManifests,
  isSemver,
  main,
  publishesToCratesIo,
  PUBLISHABLE,
} from './check.mjs';

const workspace = (root, version = '0.2.0') =>
  writeFileSync(
    join(root, 'Cargo.toml'),
    `[workspace]\n[workspace.package]\nversion = "${version}"\n`,
  );

const crateToml = (root, name, { publish = 'true', deps = '', devDeps = '', features = '' } = {}) => {
  const dir = join(root, 'crates', name);
  mkdirSync(dir, { recursive: true });
  const body = [
    `[package]\nname = "${name}"\nversion.workspace = true\npublish = ${publish}\n`,
    features && `[features]\n${features}\n`,
    `[dependencies]\n${deps}\n`,
    devDeps && `[dev-dependencies]\n${devDeps}\n`,
  ]
    .filter(Boolean)
    .join('');
  writeFileSync(join(dir, 'Cargo.toml'), body);
};

const cleanFixture = (root) => {
  workspace(root);
  crateToml(root, 'mindcore-base');
  crateToml(root, 'mindcore-spi', {
    deps: 'mindcore-base = { path = "../mindcore-base", version = "0.2.0" }\n',
  });
  // A publishable crate may dev-depend on a private path crate: dev deps never
  // resolve on crates.io, so no version is needed there.
  crateToml(root, 'mindcore-sqlite', {
    deps: 'mindcore-base = { path = "../mindcore-base", version = "0.2.0" }\n',
    devDeps: 'mindcore-spoke = { path = "../mindcore-spoke" }\n',
  });
};

const fixture = (fn) => {
  const root = mkdtempSync(join(tmpdir(), 'semver-check-'));
  fn(root);
  return root;
};

test('isSemver accepts release, pre-release and build forms', () => {
  assert.ok(isSemver('0.1.0'));
  assert.ok(isSemver('1.2.3-rc.1'));
  assert.ok(isSemver('1.2.3+sha.abc'));
  assert.ok(!isSemver('0.1'));
  assert.ok(!isSemver('v1.2.3'));
  assert.ok(!isSemver('1.2.3-'));
});

test('accepts an unset publish and every value that still targets crates.io', () => {
  assert.ok(publishesToCratesIo(undefined), 'an unset publish means the default registry');
  for (const publish of ['true', '"crates-io"', '["crates-io"]', '[ "crates-io" ]']) {
    const root = fixture(cleanFixture);
    crateToml(root, 'mindcore-base', { publish });
    assert.deepEqual(checkManifests(root), [], `publish = ${publish} must be accepted`);
  }
});

test('rejects a publishable crate that lists another registry', () => {
  for (const publish of ['["other-registry"]', '["crates-io", "other-registry"]']) {
    const root = fixture(cleanFixture);
    crateToml(root, 'mindcore-base', { publish });
    assert.ok(
      checkManifests(root).some(
        (e) => e.includes('mindcore-base') && e.includes('other-registry'),
      ),
      `publish = ${publish} must be rejected`,
    );
  }
});

test('rejects a publishable crate that still has publish = false', () => {
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-base', { publish: 'false' });
  assert.ok(
    checkManifests(root).some(
      (e) => e.includes('mindcore-base') && e.includes('publish = false'),
    ),
  );
});

test('rejects an internal path dependency without version', () => {
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-sqlite', {
    deps: 'mindcore-base = { path = "../mindcore-base" }\n',
  });
  assert.ok(
    checkManifests(root).some(
      (e) => e.includes('mindcore-sqlite') && e.includes('mindcore-base') && e.includes('version'),
    ),
  );
});

test('rejects the test-scoring feature on mindcore-spi', () => {
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-spi', {
    deps: 'mindcore-base = { path = "../mindcore-base", version = "0.2.0" }\n',
    features: 'test-scoring = []\n',
  });
  assert.ok(
    checkManifests(root).some((e) => e.includes('mindcore-spi') && e.includes('test-scoring')),
  );
});

test('rejects a non-SemVer workspace version', () => {
  const root = fixture((r) => {
    workspace(r, '0.2');
    for (const name of PUBLISHABLE) crateToml(r, name);
  });
  assert.ok(checkManifests(root).some((e) => e.includes('SemVer')));
});

test('accepts a clean publishable workspace', () => {
  const root = fixture(cleanFixture);
  assert.deepEqual(checkManifests(root), []);
});

test('main skips the API diff when the fixture dir has no v* tag', () => {
  const root = fixture(cleanFixture);
  assert.equal(main(root), 0);
});

function gitRepo(fn) {
  const root = mkdtempSync(join(tmpdir(), 'semver-baseline-'));
  const git = (...args) => {
    const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' });
    assert.equal(result.status, 0, `git ${args.join(' ')}: ${result.stderr}`);
  };
  git('init', '--quiet', '--initial-branch', 'main');
  git('config', 'user.email', 'test@example.invalid');
  git('config', 'user.name', 'test');
  git('config', 'tag.gpgsign', 'false');
  const commit = (name) => {
    writeFileSync(join(root, name), name);
    git('add', name);
    git('commit', '--quiet', '-m', name);
  };
  fn({ root, git, commit });
  return root;
}

test('baselineTag is null without a v* tag or any git history', () => {
  // A directory that is not a repository at all answers "no baseline" too.
  assert.equal(baselineTag(fixture(cleanFixture)), null);
  gitRepo(({ root }) => assert.equal(baselineTag(root), null));
});

test('baselineTag is the previous tag on a tag commit and the newest tag elsewhere', () => {
  gitRepo(({ root, git, commit }) => {
    commit('one');
    assert.equal(baselineTag(root), null);
    git('tag', 'v0.1.0');
    // On the first release's own commit there is no predecessor to diff against.
    assert.equal(baselineTag(root), null);
    commit('two');
    assert.equal(baselineTag(root), 'v0.1.0');
    git('tag', 'v0.2.0');
    assert.equal(baselineTag(root), 'v0.1.0');
    commit('three');
    assert.equal(baselineTag(root), 'v0.2.0');
  });
});
