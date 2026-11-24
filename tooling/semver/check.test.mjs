import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test, { after } from 'node:test';
import { fileURLToPath } from 'node:url';

import { baselineTag } from '../git/baseline.mjs';
import {
  checkManifests,
  isSemver,
  main,
  parseToml,
  publishesToCratesIo,
  PUBLISHABLE,
} from './check.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));

const workspace = (root, version = '0.2.0', extra = '') =>
  writeFileSync(
    join(root, 'Cargo.toml'),
    `[workspace]\n[workspace.package]\nversion = "${version}"\n${extra}`,
  );

const crateToml = (
  root,
  name,
  { publish = 'true', deps = '', devDeps = '', features = '', extra = '' } = {},
) => {
  const dir = join(root, 'crates', name);
  mkdirSync(dir, { recursive: true });
  const body = [
    `[package]\nname = "${name}"\nversion.workspace = true\npublish = ${publish}\n`,
    features && `[features]\n${features}\n`,
    `[dependencies]\n${deps}\n`,
    devDeps && `[dev-dependencies]\n${devDeps}\n`,
    extra,
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
  scratch.push(root);
  fn(root);
  return root;
};

// Scratch dirs from fixture()/gitRepo() are removed even when a test fails.
const scratch = [];
after(() => {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
});

test('isSemver accepts release, pre-release and build forms', () => {
  assert.ok(isSemver('0.1.0'));
  assert.ok(isSemver('1.2.3-rc.1'));
  assert.ok(isSemver('1.2.3+sha.abc'));
  assert.ok(!isSemver('0.1'));
  assert.ok(!isSemver('v1.2.3'));
  assert.ok(!isSemver('1.2.3-'));
  assert.ok(!isSemver('01.2.3'));
  assert.ok(!isSemver('1.2.3-01'));
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

test('rejects a quoted dependency key without version', () => {
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-sqlite', {
    deps: '"mindcore-base" = { path = "../mindcore-base" }\n',
  });
  assert.ok(
    checkManifests(root).some(
      (e) => e.includes('mindcore-sqlite') && e.includes('mindcore-base') && e.includes('version'),
    ),
  );
});

test('rejects a path dependency in [build-dependencies] without version', () => {
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-base', {
    extra: '\n[build-dependencies]\ncodegen = { path = "../codegen" }\n',
  });
  assert.ok(
    checkManifests(root).some(
      (e) =>
        e.includes('mindcore-base') &&
        e.includes('build-dependencies') &&
        e.includes('codegen') &&
        e.includes('version'),
    ),
  );
});

test('checks target-specific dependency tables', () => {
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-base', {
    extra: `\n[target.'cfg(unix)'.dependencies]\nfoo = { path = "../foo" }\n`,
  });
  assert.ok(
    checkManifests(root).some(
      (e) =>
        e.includes('mindcore-base') &&
        e.includes(`[target.'cfg(unix)'.dependencies]`) &&
        e.includes('foo') &&
        e.includes('version'),
    ),
  );
});

test('checks target-specific build-dependency tables', () => {
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-base', {
    extra: `\n[target.'cfg(unix)'.build-dependencies]\nfoo = { path = "../foo" }\n`,
  });
  assert.ok(
    checkManifests(root).some(
      (e) =>
        e.includes('mindcore-base') &&
        e.includes(`[target.'cfg(unix)'.build-dependencies]`) &&
        e.includes('foo') &&
        e.includes('version'),
    ),
  );
});

test('resolves a workspace-inherited dependency against the root entry', () => {
  const root = fixture((r) => {
    workspace(r, '0.2.0', '\n[workspace.dependencies]\nfoo = { path = "crates/foo" }\n');
    for (const name of PUBLISHABLE) crateToml(r, name);
    crateToml(r, 'mindcore-base', { deps: 'foo = { workspace = true }\n' });
  });
  assert.ok(
    checkManifests(root).some(
      (e) => e.includes('mindcore-base') && e.includes('foo') && e.includes('version'),
    ),
  );
});

test('resolves a quoted key in [workspace.dependencies]', () => {
  const root = fixture((r) => {
    workspace(r, '0.2.0', '\n[workspace.dependencies]\n"foo" = { path = "crates/foo" }\n');
    for (const name of PUBLISHABLE) crateToml(r, name);
    crateToml(r, 'mindcore-base', { deps: 'foo = { workspace = true }\n' });
  });
  assert.ok(
    checkManifests(root).some(
      (e) => e.includes('mindcore-base') && e.includes('foo') && e.includes('version'),
    ),
  );
});

test('accepts a workspace-inherited dependency whose root entry pins a version', () => {
  const root = fixture((r) => {
    workspace(
      r,
      '0.2.0',
      '\n[workspace.dependencies]\nfoo = { path = "crates/foo", version = "0.2.0" }\n',
    );
    for (const name of PUBLISHABLE) crateToml(r, name);
    crateToml(r, 'mindcore-base', { deps: 'foo = { workspace = true }\n' });
  });
  assert.deepEqual(checkManifests(root), []);
});

test('leaves path-only dev-dependencies alone', () => {
  // cargo drops versionless dev-deps from the packaged manifest instead of
  // failing, so a version there would flag a legal tree.
  const root = fixture(cleanFixture);
  crateToml(root, 'mindcore-base', {
    extra: `\n[dev-dependencies]\nfoo = { path = "../foo" }\n\n[target.'cfg(unix)'.dev-dependencies]\nfoo = { path = "../foo" }\n`,
  });
  assert.deepEqual(checkManifests(root), []);
});

test('parseToml descends dotted headers into nested tables', () => {
  const toml = parseToml(`[target.'cfg(unix)'.dependencies]\nfoo = { path = "../foo" }\n`);
  // Quoted TOML keys are the same keys unquoted, so the parser drops the quotes.
  assert.deepEqual(toml.target['cfg(unix)'].dependencies.foo, '{ path = "../foo" }');
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

test('the API diff blocks off a tag commit only with --blocking', () => {
  gitRepo(({ root, git, commit }) => {
    cleanFixture(root);
    mkdirSync(join(root, 'tooling', 'semver'), { recursive: true });
    mkdirSync(join(root, 'tooling', 'git'), { recursive: true });
    cpSync(join(HERE, 'check.mjs'), join(root, 'tooling', 'semver', 'check.mjs'));
    cpSync(join(HERE, '..', 'git', 'baseline.mjs'), join(root, 'tooling', 'git', 'baseline.mjs'));

    // A fake cargo: the tool probe passes, every API diff reports a violation.
    const bin = join(root, 'bin');
    mkdirSync(bin);
    writeFileSync(
      join(bin, 'cargo'),
      '#!/bin/sh\n[ "$1" = semver-checks ] && [ "$2" = --version ] && exit 0\nexit 1\n',
      { mode: 0o755 },
    );

    git('add', '--all');
    git('commit', '--quiet', '-m', 'release 0.1.0');
    git('tag', 'v0.1.0');
    commit('breaking change');

    const run = (...args) =>
      spawnSync(process.execPath, ['tooling/semver/check.mjs', ...args], {
        cwd: root,
        encoding: 'utf8',
        env: { ...process.env, PATH: `${bin}:${process.env.PATH}` },
      });

    // Off a tag commit the break is only a warning: the release PR's version
    // bump makes it releasable.
    assert.equal(run().status, 0);
    // The release workflow checks before tagging, so there it must fail.
    assert.equal(run('--blocking').status, 1);
  });
});

test('the API diff blocks on a release tag commit without --blocking', () => {
  gitRepo(({ root, git }) => {
    cleanFixture(root);
    mkdirSync(join(root, 'tooling', 'semver'), { recursive: true });
    mkdirSync(join(root, 'tooling', 'git'), { recursive: true });
    cpSync(join(HERE, 'check.mjs'), join(root, 'tooling', 'semver', 'check.mjs'));
    cpSync(join(HERE, '..', 'git', 'baseline.mjs'), join(root, 'tooling', 'git', 'baseline.mjs'));

    // Same fake cargo: the tool probe passes, every API diff reports a violation.
    const bin = join(root, 'bin');
    mkdirSync(bin);
    writeFileSync(
      join(bin, 'cargo'),
      '#!/bin/sh\n[ "$1" = semver-checks ] && [ "$2" = --version ] && exit 0\nexit 1\n',
      { mode: 0o755 },
    );

    git('add', '--all');
    git('commit', '--quiet', '-m', 'release 0.1.0');
    git('tag', 'v0.1.0');
    git('commit', '--quiet', '--allow-empty', '-m', 'release 0.2.0');
    git('tag', 'v0.2.0');

    // HEAD is exactly v0.2.0: that tag is the release, so a break fails even
    // without --blocking.
    const run = spawnSync(process.execPath, ['tooling/semver/check.mjs'], {
      cwd: root,
      encoding: 'utf8',
      env: { ...process.env, PATH: `${bin}:${process.env.PATH}` },
    });
    assert.equal(run.status, 1, run.stderr);
  });
});

test('a build failure in the API diff fails even when non-blocking', () => {
  gitRepo(({ root, git, commit }) => {
    cleanFixture(root);
    mkdirSync(join(root, 'tooling', 'semver'), { recursive: true });
    mkdirSync(join(root, 'tooling', 'git'), { recursive: true });
    cpSync(join(HERE, 'check.mjs'), join(root, 'tooling', 'semver', 'check.mjs'));
    cpSync(join(HERE, '..', 'git', 'baseline.mjs'), join(root, 'tooling', 'git', 'baseline.mjs'));

    // The tool probe passes, then every API diff dies in the build — no diff
    // ever ran, so non-blocking must not wave it through as a warning.
    const bin = join(root, 'bin');
    mkdirSync(bin);
    writeFileSync(
      join(bin, 'cargo'),
      '#!/bin/sh\n[ "$1" = semver-checks ] && [ "$2" = --version ] && exit 0\necho "error: could not compile mindcore-sqlite" >&2\nexit 1\n',
      { mode: 0o755 },
    );

    git('add', '--all');
    git('commit', '--quiet', '-m', 'release 0.1.0');
    git('tag', 'v0.1.0');
    commit('breaking change');

    const run = (...args) =>
      spawnSync(process.execPath, ['tooling/semver/check.mjs', ...args], {
        cwd: root,
        encoding: 'utf8',
        env: { ...process.env, PATH: `${bin}:${process.env.PATH}` },
      });

    assert.equal(run().status, 1);
    assert.equal(run('--blocking').status, 1);
  });
});


function gitRepo(fn) {
  const root = mkdtempSync(join(tmpdir(), 'semver-check-'));
  scratch.push(root);
  const git = (...args) => {
    const result = spawnSync('git', ['-c', 'commit.gpgsign=false', ...args], { cwd: root, encoding: 'utf8' });
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

test('baselineTag is the previous stable tag on a tag commit and the newest stable tag elsewhere', () => {
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

test('baselineTag ignores a non-release tag between two releases', () => {
  gitRepo(({ root, git, commit }) => {
    commit('one');
    git('tag', 'v0.1.0');
    commit('two');
    git('tag', 'nightly-1');
    commit('three');
    git('tag', 'v0.2.0');
    // The non-release tag sits on the parent commit but is not a baseline.
    assert.equal(baselineTag(root), 'v0.1.0');
  });
});

test('baselineTag is null when only prerelease tags exist', () => {
  gitRepo(({ root, git, commit }) => {
    commit('one');
    git('tag', 'v0.1.0-rc.1');
    // A prerelease claims no registry version, so it is never a baseline.
    assert.equal(baselineTag(root), null);
    commit('two');
    git('tag', 'v0.1.0-rc.2');
    assert.equal(baselineTag(root), null);
  });
});

test('baselineTag prefers a stable tag over a newer prerelease', () => {
  gitRepo(({ root, git, commit }) => {
    commit('one');
    git('tag', 'v0.1.0');
    commit('two');
    git('tag', 'v0.2.0-rc.1');
    commit('three');
    git('tag', 'v0.2.0');
    // -v:refname puts v0.2.0-rc.1 first, but the rc is not a stable baseline.
    assert.equal(baselineTag(root), 'v0.1.0');
    commit('four');
    assert.equal(baselineTag(root), 'v0.2.0');
  });
});

test('baselineTag ignores a short alias tag that version sort ranks first', () => {
  gitRepo(({ root, git, commit }) => {
    commit('one');
    git('tag', 'v1.9.0');
    commit('two');
    // `v2` is a moving alias, not a release; only a full vX.Y.Z is a baseline.
    git('tag', 'v2');
    assert.equal(baselineTag(root), 'v1.9.0');
  });
});

test('baselineTag skips an alias tag on the parent of a release commit', () => {
  gitRepo(({ root, git, commit }) => {
    commit('one');
    git('tag', 'v1.9.0');
    commit('two');
    git('tag', 'v2');
    commit('three');
    git('tag', 'v2.0.0');
    // HEAD is the v2.0.0 release; the alias on its parent must not be the
    // predecessor the API diff compares against.
    assert.equal(baselineTag(root), 'v1.9.0');
  });
});
