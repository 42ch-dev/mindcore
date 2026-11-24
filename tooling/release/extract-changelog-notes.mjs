#!/usr/bin/env node
/**
 * Print the `## [X.Y.Z]` section of `CHANGELOG.md` (GitHub Release notes).
 *
 * CLI: node tooling/release/extract-changelog-notes.mjs <X.Y.Z>
 *
 * Exits 0 with the section on stdout, or 1 with empty stdout when it is missing.
 *
 * @module tooling/release/extract-changelog-notes
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { repoRoot } from "./next-version.mjs";

/**
 * @param {string} changelog
 * @param {string} version
 * @returns {string | null} Section body up to the next `## [` heading, minus the heading.
 */
export function extractSection(changelog, version) {
  const normalized = version.trim().replace(/^v/i, "");
  if (!/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(normalized)) {
    return null;
  }
  const escaped = normalized.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const header = new RegExp(
    `^## \\[${escaped}\\](?:\\s+-\\s+\\d{4}-\\d{2}-\\d{2})?\\s*$`,
    "m",
  ).exec(changelog);
  if (!header) {
    return null;
  }
  const rest = changelog.slice(header.index + header[0].length);
  const next = rest.search(/^## \[/m);
  const body = (next === -1 ? rest : rest.slice(0, next)).trim();
  return body.length > 0 ? `${body}\n` : null;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const argv = process.argv.slice(2);
  if (argv.length !== 1) {
    console.error("Usage: node tooling/release/extract-changelog-notes.mjs <X.Y.Z>");
    process.exit(1);
  }
  const section = extractSection(
    readFileSync(join(repoRoot(), "CHANGELOG.md"), "utf8"),
    argv[0],
  );
  if (!section) {
    process.exit(1);
  }
  process.stdout.write(section);
}
