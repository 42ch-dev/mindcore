#!/usr/bin/env node
// Release-surface lint for docs/semver.md.
//
// Static checks always run. The `cargo semver-checks` API diff runs only once a
// `v*` baseline tag exists — before the first tag this script skips it, so it
// is safe on a brand-new history. The diff is blocking on a release tag commit;
// `--blocking` makes it blocking everywhere, for the release workflow's window
// between the merged release PR and the tag it is about to create.

import { execFileSync, spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { baselineTag, git, isShallow, SHALLOW_HINT } from '../git/baseline.mjs';

export const PUBLISHABLE = ['mindcore-base', 'mindcore-spi', 'mindcore-sqlite'];

// Tables cargo rejects at package time when a path dep has no `version`.
// dev-dependencies are absent on purpose: see depTables().
const DEP_TABLES = ['dependencies', 'build-dependencies'];

const SEMVER =
  /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/;

// SemVer forbids leading zeros in the numeric core and in numeric prerelease
// identifiers; the plain regex above would accept `01.2.3` and `1.2.3-01`.
export const isSemver = (value) => {
  if (typeof value !== 'string') return false;
  const match = SEMVER.exec(value);
  if (!match) return false;
  if ([match[1], match[2], match[3]].some((part) => part.length > 1 && part.startsWith('0'))) {
    return false;
  }
  const prerelease = match[4];
  if (prerelease) {
    for (const identifier of prerelease.split('.')) {
      if (/^\d+$/.test(identifier) && identifier.length > 1 && identifier.startsWith('0')) {
        return false;
      }
    }
  }
  return true;
};

// The manifests this reads use single-line values only: scalars, arrays and
// inline tables. Dotted keys descend into nested objects; `#` comments end a
// line outside double-quoted strings.
export function parseToml(text) {
  const root = {};
  let table = root;
  // Quoted keys are legal TOML (`"foo" = ...`); compare them unquoted or the
  // dependency checks would look up `"foo"` and miss the entry.
  const key = (part) => unquote(part.trim());
  for (const raw of text.split('\n')) {
    const line = stripComment(raw).trim();
    if (!line) continue;
    const header = /^\[([^\]]+)\]$/.exec(line);
    if (header) {
      table = header[1].split('.').map(key).reduce((o, k) => (o[k] ??= {}), root);
      continue;
    }
    const kv = /^([^=]+?)\s*=\s*(.+)$/.exec(line);
    if (!kv) continue;
    const keys = kv[1].trim().split('.').map(key);
    const leaf = keys.pop();
    const target = keys.reduce((o, k) => (o[k] ??= {}), table);
    target[leaf] = unquote(kv[2].trim());
  }
  return root;
}

function stripComment(line) {
  let inString = false;
  for (let i = 0; i < line.length; i++) {
    if (line[i] === '"') inString = !inString;
    else if (line[i] === '#' && !inString) return line.slice(0, i);
  }
  return line;
}

function readManifest(path, errors) {
  try {
    return parseToml(readFileSync(path, 'utf8'));
  } catch {
    errors.push(`${path}: manifest missing or unreadable`);
    return null;
  }
}

// An internal path dependency is publishable only when it also pins `version`;
// cargo strips `path` on publish and resolves the declared version instead.
function depNeedsVersion(spec) {
  if (typeof spec === 'string')
    return /\bpath\s*=/.test(spec) && !/\bversion\s*=/.test(spec);
  return spec !== null && typeof spec === 'object' && 'path' in spec && !('version' in spec);
}

// `{ workspace = true }` defers `path`/`version` to the root entry, which is
// what cargo rewrites when packaging. Inline tables stay raw source text.
function inheritsWorkspace(spec) {
  if (typeof spec === 'string') return /\bworkspace\s*=\s*true\b/.test(spec);
  return spec !== null && typeof spec === 'object' && String(spec.workspace).trim() === 'true';
}

// Every table cargo validates at package time, prefixed `target.<cfg>.` for
// target-specific entries. dev-dependencies are excluded: cargo drops
// versionless (and path-only) dev-deps from the packaged manifest instead of
// failing, so a version cannot be required there.
function depTables(toml) {
  const tables = [];
  for (const table of DEP_TABLES) {
    if (toml[table]) tables.push([table, toml[table]]);
  }
  for (const [cfg, section] of Object.entries(toml.target ?? {})) {
    // parseToml unquotes keys, so re-quote a non-bare cfg key to keep the
    // reported header valid TOML, e.g. `target.'cfg(unix)'.dependencies`.
    const label = /^[A-Za-z0-9_-]+$/.test(cfg) ? cfg : `'${cfg}'`;
    for (const table of DEP_TABLES) {
      if (section?.[table]) tables.push([`target.${label}.${table}`, section[table]]);
    }
  }
  return tables;
}

// `publish` gates where a crate may be uploaded. Unset and `true` mean the
// default registry; `["crates-io"]` (or the bare string) names it explicitly.
// Anything else — `false` or another registry — keeps the crate off crates.io.
export function publishesToCratesIo(publish) {
  if (publish === undefined || publish === 'true') return true;
  const value = publish.trim();
  const list = /^\[(.*)\]$/.exec(value);
  const entries = (list ? list[1].split(',') : [value])
    .map((entry) => unquote(entry.trim()))
    .filter(Boolean);
  return entries.length === 1 && entries[0] === 'crates-io';
}

// Basic/literal string values lose their quotes; other values (booleans, arrays,
// inline tables) stay raw so checks match on their source text.
function unquote(value) {
  return value.length >= 2 &&
    ((value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'")))
    ? value.slice(1, -1)
    : value;
}
// Static lints only — no git, no cargo. Returns the list of violations.
export function checkManifests(rootDir) {
  const errors = [];

  const root = readManifest(join(rootDir, 'Cargo.toml'), errors);
  if (root) {
    const version = root.workspace?.package?.version;
    if (!isSemver(version)) {
      errors.push(
        `Cargo.toml: [workspace.package] version ${JSON.stringify(version)} is not valid SemVer`,
      );
    }
  }

  for (const crate of PUBLISHABLE) {
    const toml = readManifest(join(rootDir, 'crates', crate, 'Cargo.toml'), errors);
    if (!toml) continue;
    const pkg = toml.package ?? {};

    if (!publishesToCratesIo(pkg.publish)) {
      errors.push(
        `${crate}: publishable crate has \`publish = ${pkg.publish}\`; a published crate's ` +
          `value must be unset, \`true\`, or \`["crates-io"]\``,
      );
    }

    for (const [table, deps] of depTables(toml)) {
      for (const [dep, spec] of Object.entries(deps ?? {})) {
        // `{ workspace = true }` carries no path of its own; the root entry it
        // resolves to is what cargo rewrites on publish.
        const resolved = inheritsWorkspace(spec)
          ? root?.workspace?.dependencies?.[dep]
          : spec;
        if (resolved !== undefined && depNeedsVersion(resolved)) {
          errors.push(
            `${crate}: [${table}] \`${dep}\` uses \`path\` without \`version\`; it cannot resolve on crates.io`,
          );
        }
      }
    }

    if (crate === 'mindcore-spi' && toml.features && 'test-scoring' in toml.features) {
      errors.push(
        `mindcore-spi: \`test-scoring\` is a private test probe and must not ship in the published feature set`,
      );
    }
  }

  return errors;
}

function hasSemverChecks(cwd) {
  try {
    execFileSync('cargo', ['semver-checks', '--version'], { cwd, stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

export function main(rootDir = repoRoot(), { blocking: forceBlocking = false } = {}) {
  const errors = checkManifests(rootDir);
  if (errors.length) {
    for (const error of errors) console.error(`error: ${error}`);
    console.error('SemVer policy violations — see docs/semver.md');
    return 1;
  }

  const tag = baselineTag(rootDir);
  if (!tag) {
    if (isShallow(rootDir)) {
      console.error(`error: ${SHALLOW_HINT}`);
      return 1;
    }
    console.log('no baseline tag; API diff check skipped');
    return 0;
  }

  if (git(rootDir, ['cat-file', '-e', `${tag}^{commit}`]) === null) {
    console.error(
      `error: baseline tag ${tag} is not in this clone; a shallow checkout hides the previous ` +
        'release. Fetch the tags and their history (actions/checkout: fetch-depth: 0).',
    );
    return 1;
  }

  if (!hasSemverChecks(rootDir)) {
    console.error(`error: baseline tag ${tag} exists but cargo-semver-checks is not installed`);
    console.error('install it with: cargo install cargo-semver-checks --locked');
    return 1;
  }

  // Blocking on a release tag commit (HEAD is an exact v* tag): that tag is the
  // release, so an API break must fail. Off a tag commit a breaking change lands
  // with the release PR's version bump — report it as a warning and let the
  // tag-time check enforce it. `--blocking` forces it for the release workflow,
  // which checks the merged PR before creating the tag: the baseline is then
  // still the newest released tag.
  const blocking =
    forceBlocking ||
    git(rootDir, ['describe', '--tags', '--exact-match', '--match', 'v*', 'HEAD']) !== null;
  if (!blocking) {
    console.log(`API diff against ${tag} is non-blocking until a release tag lands`);
  }

  for (const crate of PUBLISHABLE) {
    // Exit status decides what happened: cargo-semver-checks exits 100 for a
    // completed check with violations; any other failure — a build error, an
    // invalid lint table, a signal or a spawn failure — means the API diff
    // never ran, so it fails in every mode instead of passing as a warning.
    const result = spawnSync(
      'cargo',
      ['semver-checks', 'check-release', '-p', crate, '--baseline-rev', tag],
      { cwd: rootDir, encoding: 'utf8' },
    );
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    if (result.status === 0) continue;
    if (result.status !== 100) {
      console.error(
        `error: the API diff for ${crate} vs ${tag} could not run (cargo semver-checks tool failure)`,
      );
      return 1;
    }
    if (blocking) {
      console.error(`error: cargo semver-checks reported violations in ${crate} vs ${tag}`);
      return 1;
    }
    console.error(`warning: cargo semver-checks reported violations in ${crate} vs ${tag} (non-blocking)`);
  }
  return 0;
}

function repoRoot() {
  return join(dirname(fileURLToPath(import.meta.url)), '..', '..');
}

// CLI: `--blocking` forces the API diff to block off a tag commit; any other
// argument is a usage error.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2);
  const unknown = args.filter((arg) => arg !== '--blocking');
  if (unknown.length) {
    console.error(
      `Usage: node tooling/semver/check.mjs [--blocking]\nunknown argument: ${unknown[0]}`,
    );
    process.exit(1);
  }
  process.exit(main(undefined, { blocking: args.includes('--blocking') }));
}
