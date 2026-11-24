#!/usr/bin/env node
/**
 * Regenerate `CHANGELOG.md` for a release version with git-cliff, preserving the
 * curated `## [Unreleased]` body and guaranteeing a version section exists.
 *
 * CLI: node tooling/release/render-changelog.mjs <X.Y.Z>
 *
 * Why: git-cliff renders the whole file from commits, so a curated Unreleased
 * body (migration notes, breaking-change prose) would be lost, and an all-skipped
 * range would render no section at all.
 *
 * @module tooling/release/render-changelog
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseVersion, repoRoot } from "./next-version.mjs";

const CHANGELOG = "CHANGELOG.md";
const PLACEHOLDER = "_No user-visible changes._";

/**
 * @param {string} value
 * @returns {string}
 */
function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * @param {string} version
 * @returns {RegExp}
 */
function versionHeading(version) {
  return new RegExp(
    `^## \\[${escapeRegex(version)}\\](?:[ \\t]+-[ \\t]+\\d{4}-\\d{2}-\\d{2})?[ \\t]*$`,
    "m",
  );
}

/**
 * Body of the `## [Unreleased]` section, trimmed, or null when absent or empty.
 *
 * @param {string} changelog
 * @returns {string | null}
 */
export function unreleasedBody(changelog) {
  const header = /^## \[Unreleased\][ \t]*$/m.exec(changelog);
  if (!header) {
    return null;
  }
  const rest = changelog.slice(header.index + header[0].length);
  const next = rest.search(/^## \[/m);
  const body = (next === -1 ? rest : rest.slice(0, next)).trim();
  return body.length > 0 ? body : null;
}

/**
 * Ensure the file has a `## [<version>]` section and place `body` directly under
 * its heading, before any generated group. Pure: no I/O.
 *
 * A missing section is inserted as the newest section (before the first `## [`
 * heading, or at the end); with no `body` it carries the placeholder line.
 *
 * @param {string} changelog Generated changelog text.
 * @param {string} version
 * @param {{ body?: string | null; date: string }} options `date` is YYYY-MM-DD.
 * @returns {string}
 */
export function mergeVersionSection(changelog, version, { body, date }) {
  const curated = body?.trim() ?? "";
  const existing = versionHeading(version).exec(changelog);

  if (existing) {
    if (curated.length === 0) {
      return changelog;
    }
    const afterHeading = existing.index + existing[0].length;
    const tail = changelog.slice(afterHeading).replace(/^\n+/, "");
    return `${changelog.slice(0, afterHeading)}\n\n${curated}\n\n${tail}`;
  }

  const section = `## [${version}] - ${date}\n\n${curated.length > 0 ? curated : PLACEHOLDER}\n`;
  const firstHeading = changelog.search(/^## \[/m);
  if (firstHeading === -1) {
    const head = changelog.replace(/\s*$/, "");
    return head.length > 0 ? `${head}\n\n${section}` : section;
  }
  return `${changelog.slice(0, firstHeading)}${section}\n${changelog.slice(firstHeading)}`;
}

/**
 * @returns {string} UTC date as YYYY-MM-DD.
 */
export function todayUtc() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * @param {string[]} argv
 */
function main(argv) {
  if (argv.length !== 1) {
    console.error("Usage: node tooling/release/render-changelog.mjs <X.Y.Z>");
    process.exit(1);
  }
  const version = argv[0];
  if (!parseVersion(version)) {
    console.error(`render-changelog: invalid SemVer "${version}"`);
    process.exit(1);
  }

  const root = repoRoot();
  const changelogPath = join(root, CHANGELOG);
  // A cold tree may have no changelog yet; git-cliff writes the first one.
  const curated = existsSync(changelogPath)
    ? unreleasedBody(readFileSync(changelogPath, "utf8"))
    : null;

  const cliff = spawnSync(
    "git-cliff",
    ["--tag", `v${version}`, "-o", CHANGELOG],
    { cwd: root, encoding: "utf8", stdio: ["ignore", "inherit", "inherit"] },
  );
  if (cliff.error) {
    console.error(`render-changelog: git-cliff failed: ${cliff.error.message}`);
    process.exit(1);
  }
  if (cliff.status !== 0) {
    console.error(`render-changelog: git-cliff exited ${cliff.status}`);
    process.exit(cliff.status ?? 1);
  }

  const generated = readFileSync(changelogPath, "utf8");
  const rendered = mergeVersionSection(generated, version, {
    body: curated,
    date: todayUtc(),
  });
  if (rendered !== generated) {
    writeFileSync(changelogPath, rendered, "utf8");
  }

  if (!versionHeading(version).test(rendered)) {
    console.error(
      `render-changelog: ${CHANGELOG} still has no "## [${version}]" section`,
    );
    process.exit(1);
  }
  console.log(
    `Rendered ${CHANGELOG} for v${version}${curated ? " with the curated Unreleased body" : ""}.`,
  );
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main(process.argv.slice(2));
}
