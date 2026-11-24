#!/usr/bin/env node
/**
 * Add a release version section to `CHANGELOG.md` from git-cliff, without
 * rebuilding the file.
 *
 * CLI: node tooling/release/render-changelog.mjs <X.Y.Z>
 *
 * git-cliff renders only the new section (`--unreleased`); the committed file's
 * past version sections — curated prose included — are kept verbatim. The curated
 * `## [Unreleased]` body moves into the new section, and the emptied `[Unreleased]`
 * section is dropped.
 *
 * @module tooling/release/render-changelog
 */

import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseVersion, repoRoot } from "./next-version.mjs";

const CHANGELOG = "CHANGELOG.md";
const SECTION_HEADING = /^## \[/m;

/** Marks a version section with no user-visible change. */
export const PLACEHOLDER = "_No user-visible changes._";

/**
 * @param {string} value
 * @returns {string}
 */
function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * @param {string} markdown
 * @returns {{ preamble: string; sections: string[] }} `preamble` is everything
 *   before the first `## [` heading; each section starts at its heading.
 */
export function splitSections(markdown) {
  const first = markdown.search(SECTION_HEADING);
  if (first === -1) {
    return { preamble: markdown.replace(/\s*$/, ""), sections: [] };
  }
  return {
    preamble: markdown.slice(0, first).replace(/\s*$/, ""),
    sections: markdown
      .slice(first)
      .split(/^(?=## \[)/m)
      .map((section) => section.replace(/\s*$/, ""))
      .filter((section) => section.length > 0),
  };
}

/**
 * @param {string} section
 * @returns {string | null} The bracketed label of a `## [...]` heading.
 */
export function sectionLabel(section) {
  return /^## \[([^\]]*)\]/.exec(section)?.[1] ?? null;
}

/**
 * Body of the `## [Unreleased]` section, trimmed, or null when absent or empty.
 *
 * @param {string} changelog
 * @returns {string | null}
 */
export function unreleasedBody(changelog) {
  const { sections } = splitSections(changelog);
  const unreleased = sections.find((section) => sectionLabel(section) === "Unreleased");
  if (!unreleased) {
    return null;
  }
  const body = unreleased.replace(/^## \[Unreleased\][^\n]*\n?/, "").trim();
  return body.length > 0 ? body : null;
}

/**
 * Split a git-cliff render into its file header and its section block.
 *
 * @param {string} output
 * @returns {{ header: string; section: string | null }}
 */
export function cliffParts(output) {
  const first = output.search(SECTION_HEADING);
  if (first === -1) {
    return { header: output.trim(), section: null };
  }
  return {
    header: output.slice(0, first).replace(/\s*$/, ""),
    section: output.slice(first).replace(/\s*$/, ""),
  };
}

/**
 * Place the curated body directly under a section's heading, before the
 * generated groups. A missing section becomes one, carrying the placeholder when
 * there is no curated body. Pure.
 *
 * @param {string | null} section Section text starting at its `## [` heading.
 * @param {string} version
 * @param {{ body?: string | null; date: string }} options `date` is YYYY-MM-DD.
 * @returns {string}
 */
export function withCuratedBody(section, version, { body, date }) {
  const curated = body?.trim() ?? "";
  const heading = new RegExp(
    `^## \\[${escapeRegex(version)}\\][^\\n]*$`,
    "m",
  ).exec(section ?? "");
  if (!heading) {
    return `## [${version}] - ${date}\n\n${curated.length > 0 ? curated : PLACEHOLDER}\n`;
  }
  const rest = (section ?? "").slice(heading.index + heading[0].length).replace(/^\n+/, "");
  const below = curated.length > 0 ? `${curated}\n\n${rest}` : rest;
  return `${heading[0]}\n\n${below.replace(/\s*$/, "")}\n`;
}

/**
 * Prepend the new version section to the committed file and drop the
 * `[Unreleased]` section (and any stale section for the same version). Every
 * other section survives untouched. Pure.
 *
 * @param {string} existing Committed `CHANGELOG.md` ("" when there is none).
 * @param {string} version
 * @param {{ section?: string | null; body?: string | null; date: string; header?: string }} options
 *   `section` is the git-cliff render; `header` supplies the preamble when the
 *   committed file has none.
 * @returns {string}
 */
export function renderRelease(existing, version, { section, body, date, header }) {
  const { preamble, sections } = splitSections(existing);
  const kept = sections.filter((entry) => {
    const label = sectionLabel(entry);
    return label !== "Unreleased" && label !== version;
  });
  const next = withCuratedBody(section ?? null, version, { body, date });
  const head = preamble.length > 0 ? preamble : (header?.trim() ?? "");
  const parts = [...(head.length > 0 ? [head] : []), next, ...kept].map((part) =>
    part.replace(/\s*$/, ""),
  );
  return `${parts.join("\n\n")}\n`;
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
  const existing = existsSync(changelogPath) ? readFileSync(changelogPath, "utf8") : "";
  const curated = unreleasedBody(existing);

  const scratch = mkdtempSync(join(tmpdir(), "mindcore-cliff-"));
  const renderPath = join(scratch, "section.md");
  try {
    const cliff = spawnSync(
      "git-cliff",
      ["--tag", `v${version}`, "--unreleased", "-o", renderPath],
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

    const { header, section } = cliffParts(readFileSync(renderPath, "utf8"));
    writeFileSync(
      changelogPath,
      renderRelease(existing, version, {
        section,
        body: curated,
        date: todayUtc(),
        header,
      }),
      "utf8",
    );
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }

  console.log(
    `Rendered ${CHANGELOG} for v${version}${curated ? " with the curated Unreleased body" : ""}.`,
  );
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main(process.argv.slice(2));
}
