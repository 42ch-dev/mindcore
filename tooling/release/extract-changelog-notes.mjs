#!/usr/bin/env node
/**
 * Print the `## [X.Y.Z]` section of `CHANGELOG.md` (GitHub Release notes).
 *
 * CLI: node tooling/release/extract-changelog-notes.mjs <X.Y.Z>
 *
 * Exits 0 with the section on stdout. Every failure exits 1 with its own
 * stderr message: invalid version, missing heading, or an empty section.
 *
 * @module tooling/release/extract-changelog-notes
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseVersion, repoRoot } from "./next-version.mjs";

const HEADING_PATTERN = /^## \[([^\]]*)\]/gm;

/**
 * @param {string} value
 * @returns {string}
 */
function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * @param {string} changelog
 * @returns {string[]} Version labels of every `## [...]` heading, in file order.
 */
export function listHeadings(changelog) {
  return [...changelog.matchAll(HEADING_PATTERN)].map(([, label]) => label);
}

/**
 * @param {string} changelog
 * @param {string} version
 * @returns {{ notes: string } | { error: string }}
 */
export function readNotes(changelog, version) {
  const normalized = version.trim().replace(/^v/i, "");
  if (!parseVersion(normalized)) {
    return { error: `extract-changelog-notes: invalid SemVer "${version}"` };
  }

  const header = new RegExp(
    `^## \\[${escapeRegex(normalized)}\\](?:[ \\t]+-[ \\t]+\\d{4}-\\d{2}-\\d{2})?[ \\t]*$`,
    "m",
  ).exec(changelog);
  if (!header) {
    const found = listHeadings(changelog);
    return {
      error: `extract-changelog-notes: CHANGELOG.md has no "## [${normalized}]" section; headings found: ${found.length > 0 ? found.join(", ") : "none"}`,
    };
  }

  const rest = changelog.slice(header.index + header[0].length);
  const next = rest.search(/^## \[/m);
  const body = (next === -1 ? rest : rest.slice(0, next)).trim();
  if (body.length === 0) {
    return {
      error: `extract-changelog-notes: CHANGELOG.md "## [${normalized}]" section is empty`,
    };
  }
  return { notes: `${body}\n` };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const argv = process.argv.slice(2);
  if (argv.length !== 1) {
    console.error("Usage: node tooling/release/extract-changelog-notes.mjs <X.Y.Z>");
    process.exit(1);
  }
  let changelog;
  try {
    changelog = readFileSync(join(repoRoot(), "CHANGELOG.md"), "utf8");
  } catch (error) {
    console.error(
      `extract-changelog-notes: cannot read CHANGELOG.md: ${error instanceof Error ? error.message : String(error)}`,
    );
    process.exit(1);
  }
  const result = readNotes(changelog, argv[0]);
  if ("error" in result) {
    console.error(result.error);
    process.exit(1);
  }
  process.stdout.write(result.notes);
}
