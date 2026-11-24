#!/usr/bin/env node
// Release-surface lint for docs/semver.md.
//
// Static checks always run. The `cargo semver-checks` API diff runs only once a
// `v*` baseline tag exists — before the first tag this script skips it, so it
// is safe on a brand-new history.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const PUBLISHABLE = ['mindcore-base', 'mindcore-spi', 'mindcore-sqlite'];

// Published manifests only carry [dependencies] / [build-dependencies] into the
// crates.io graph; dev-dependencies never resolve there, so path deps in them
// stay exempt from the version requirement.
const VERSIONED_DEP_TABLES = ['dependencies', 'build-dependencies'];

const SEMVER =
  /^\d+\.\d+\.\d+(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

export const isSemver = (v) => typeof v === 'string' && SEMVER.test(v);

// The manifests this reads use single-line values only: scalars, arrays and
// inline tables. Dotted keys descend into nested objects; `#` comments end a
// line outside double-quoted strings.
export function parseToml(text) {
  const root = {};
  let table = root;
  for (const raw of text.split('\n')) {
    const line = stripComment(raw).trim();
    if (!line) continue;
    const header = /^\[([^\]]+)\]$/.exec(line);
    if (header) {
      table = header[1].split('.').reduce((o, k) => (o[k] ??= {}), root);
      continue;
    }
    const kv = /^([^=]+?)\s*=\s*(.+)$/.exec(line);
    if (!kv) continue;
    const keys = kv[1].trim().split('.');
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

    if (pkg.publish !== undefined && pkg.publish !== 'true') {
      errors.push(
        `${crate}: publishable crate still has \`publish = ${pkg.publish}\`; remove it (docs/semver.md § Publish scope)`,
      );
    }

    for (const table of VERSIONED_DEP_TABLES) {
      for (const [dep, spec] of Object.entries(toml[table] ?? {})) {
        if (depNeedsVersion(spec)) {
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

function newestBaselineTag(cwd) {
  try {
    const out = execFileSync('git', ['tag', '--list', 'v*', '--sort=-v:refname'], {
      cwd,
      encoding: 'utf8',
    });
    return out.split('\n')[0].trim() || null;
  } catch {
    return null;
  }
}

function hasSemverChecks(cwd) {
  try {
    execFileSync('cargo', ['semver-checks', '--version'], { cwd, stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

export function main(rootDir = repoRoot()) {
  const errors = checkManifests(rootDir);
  if (errors.length) {
    for (const error of errors) console.error(`error: ${error}`);
    console.error('SemVer policy violations — see docs/semver.md');
    return 1;
  }

  const tag = newestBaselineTag(rootDir);
  if (!tag) {
    console.log('no baseline tag; API diff check skipped');
    return 0;
  }

  if (!hasSemverChecks(rootDir)) {
    console.error(`error: baseline tag ${tag} exists but cargo-semver-checks is not installed`);
    console.error('install it with: cargo install cargo-semver-checks --locked');
    return 1;
  }

  for (const crate of PUBLISHABLE) {
    try {
      execFileSync(
        'cargo',
        ['semver-checks', 'check-release', '-p', crate, '--baseline-rev', tag],
        { cwd: rootDir, stdio: 'inherit' },
      );
    } catch {
      console.error(`error: cargo semver-checks reported violations in ${crate} vs ${tag}`);
      return 1;
    }
  }
  return 0;
}

function repoRoot() {
  return join(dirname(fileURLToPath(import.meta.url)), '..', '..');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exit(main());
}
