#!/usr/bin/env node
/**
 * Resolve the next workspace release version from `[workspace.package] version`.
 *
 * CLI:
 *   node tooling/release/next-version.mjs            # patch bump of the current version
 *   node tooling/release/next-version.mjs --current  # print the current version
 *   node tooling/release/next-version.mjs <X.Y.Z>    # validate an explicit version
 *
 * @module tooling/release/next-version
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Strict SemVer 2.0.0: no leading zeros in the core, non-empty identifiers,
// no leading zeros in numeric prerelease identifiers.
const SEMVER_PATTERN =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:[0-9A-Za-z-]+)(?:\.[0-9A-Za-z-]+)*))?$/;
const NUMERIC_IDENTIFIER = /^\d+$/;
const LEADING_ZERO_IDENTIFIER = /^0\d/;

/**
 * @typedef {{ major: bigint; minor: bigint; patch: bigint; prerelease: string[] | null }} ParsedVersion
 */

/**
 * @param {string} text
 * @returns {ParsedVersion | null}
 */
export function parseVersion(text) {
  const match = SEMVER_PATTERN.exec(String(text).trim());
  if (!match) {
    return null;
  }
  const prerelease = match[4] === undefined ? null : match[4].split(".");
  if (
    prerelease?.some(
      (id) => NUMERIC_IDENTIFIER.test(id) && LEADING_ZERO_IDENTIFIER.test(id),
    )
  ) {
    return null;
  }
  return {
    major: BigInt(match[1]),
    minor: BigInt(match[2]),
    patch: BigInt(match[3]),
    prerelease,
  };
}

/**
 * SemVer precedence: negative when a < b, 0 when equal, positive when a > b.
 *
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
export function compareVersions(a, b) {
  const left = parseVersion(a);
  const right = parseVersion(b);
  if (!left || !right) {
    throw new Error(`Cannot compare invalid SemVer: "${a}" vs "${b}"`);
  }
  for (const part of ["major", "minor", "patch"]) {
    if (left[part] !== right[part]) {
      return left[part] < right[part] ? -1 : 1;
    }
  }
  if (left.prerelease === null && right.prerelease === null) {
    return 0;
  }
  // A release outranks any prerelease of the same core.
  if (left.prerelease === null) {
    return 1;
  }
  if (right.prerelease === null) {
    return -1;
  }
  const length = Math.max(left.prerelease.length, right.prerelease.length);
  for (let index = 0; index < length; index += 1) {
    const l = left.prerelease[index];
    const r = right.prerelease[index];
    if (l === undefined) {
      return -1;
    }
    if (r === undefined) {
      return 1;
    }
    if (l === r) {
      continue;
    }
    const lNumeric = NUMERIC_IDENTIFIER.test(l);
    const rNumeric = NUMERIC_IDENTIFIER.test(r);
    if (lNumeric && rNumeric) {
      const leftNumber = BigInt(l);
      const rightNumber = BigInt(r);
      return leftNumber < rightNumber ? -1 : 1;
    }
    // Numeric identifiers rank below alphanumeric ones.
    if (lNumeric) {
      return -1;
    }
    if (rNumeric) {
      return 1;
    }
    return l < r ? -1 : 1;
  }
  return 0;
}

/**
 * The default next version: a prerelease promotes to its core version (the
 * release it announces), while a stable version takes the next patch.
 *
 * @param {string} current
 * @returns {string}
 */
export function patchBump(current) {
  const parsed = parseVersion(current);
  if (!parsed) {
    throw new Error(`Invalid current version: "${current}"`);
  }
  if (parsed.prerelease) {
    return `${parsed.major}.${parsed.minor}.${parsed.patch}`;
  }
  return `${parsed.major}.${parsed.minor}.${parsed.patch + 1n}`;
}

/**
 * The one version rule: an explicit input must be strictly greater than the
 * current version; otherwise the next patch is the answer.
 *
 * @param {string} current
 * @param {string | undefined} input
 * @returns {string}
 */
export function resolveNextVersion(current, input) {
  if (input === undefined || input === "") {
    return patchBump(current);
  }
  if (!parseVersion(input)) {
    throw new Error(`Invalid SemVer: "${input}"`);
  }
  if (compareVersions(input, current) <= 0) {
    throw new Error(
      `Version ${input} must be greater than the current workspace version ${current}.`,
    );
  }
  return input;
}

/**
 * @returns {string} Repository root; `MINDCORE_REPO_ROOT` overrides it for tests.
 */
export function repoRoot() {
  return (
    process.env.MINDCORE_REPO_ROOT ??
    join(dirname(fileURLToPath(import.meta.url)), "../..")
  );
}

/**
 * Body of the first `[<header>]` table in a Cargo manifest, excluding the header line.
 *
 * @param {string} contents Cargo.toml
 * @param {string} header e.g. "workspace.package"
 * @returns {string}
 */
export function tableBody(contents, header) {
  const opening = `[${header}]\n`;
  const start = contents.indexOf(opening);
  if (start === -1) {
    throw new Error(`Cargo.toml: missing [${header}] section`);
  }
  const rest = contents.slice(start + opening.length);
  const next = rest.search(/^\[/m);
  return next === -1 ? rest : rest.slice(0, next);
}

export function workspacePackageSection(contents) {
  return tableBody(contents, "workspace.package");
}

/**
 * @param {string} root
 * @returns {string}
 */
export function readWorkspaceVersion(root) {
  const contents = readFileSync(join(root, "Cargo.toml"), "utf8");
  const version = workspacePackageSection(contents).match(/^version\s*=\s*"([^"]+)"/m)?.[1];
  if (!version) {
    throw new Error('Cargo.toml: missing [workspace.package] version = "..."');
  }
  return version;
}

/**
 * @param {string[]} argv
 */
function main(argv) {
  const root = repoRoot();
  const current = readWorkspaceVersion(root);

  if (argv.length === 0) {
    process.stdout.write(`${resolveNextVersion(current)}\n`);
    return;
  }
  if (argv.length === 1 && argv[0] === "--current") {
    process.stdout.write(`${current}\n`);
    return;
  }
  if (argv.length === 1 && !argv[0].startsWith("-")) {
    process.stdout.write(`${resolveNextVersion(current, argv[0])}\n`);
    return;
  }
  console.error(
    "Usage: node tooling/release/next-version.mjs [--current | <X.Y.Z>]",
  );
  process.exit(1);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}
