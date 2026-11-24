#!/usr/bin/env node
/**
 * Bump the workspace version: `[workspace.package] version` in `Cargo.toml` and
 * every workspace member `[[package]]` version in `Cargo.lock`.
 *
 * Path dependencies use `version.workspace = true`, so nothing else carries a number.
 *
 * CLI: node tooling/release/bump-version.mjs <X.Y.Z>
 *
 * @module tooling/release/bump-version
 */

import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  readWorkspaceVersion,
  repoRoot,
  resolveNextVersion,
  tableBody,
  workspacePackageSection,
} from "./next-version.mjs";

/**
 * @param {string} contents Cargo.toml
 * @returns {string[]} Member directories relative to the workspace root.
 */
export function parseWorkspaceMembers(contents) {
  const body = tableBody(contents, "workspace").match(/^members\s*=\s*\[([\s\S]*?)\]/m)?.[1];
  if (body === undefined) {
    throw new Error("Cargo.toml: missing [workspace].members array");
  }
  const paths = [...body.replace(/#.*$/gm, "").matchAll(/"([^"]+)"/g)].map(([, path]) => path);
  if (paths.some((path) => /[*?[\]]/.test(path))) {
    throw new Error("Cargo.toml: workspace members must be explicit paths, not globs");
  }
  return paths;
}

/**
 * @param {string} contents A member Cargo.toml.
 * @returns {string}
 */
export function parsePackageName(contents) {
  const name = tableBody(contents, "package").match(/^name\s*=\s*"([^"]+)"/m)?.[1];
  if (!name) {
    throw new Error("Cargo.toml: missing [package].name");
  }
  return name;
}

/**
 * @param {string} contents Cargo.toml
 * @param {string} version
 * @returns {string}
 */
export function replaceWorkspaceVersion(contents, version) {
  const section = workspacePackageSection(contents);
  const replaced = section.replace(/^version\s*=\s*"[^"]*"/m, `version = "${version}"`);
  if (replaced === section) {
    throw new Error('Cargo.toml: missing version = "..." in [workspace.package]');
  }
  return contents.replace(section, () => replaced);
}

/**
 * @param {string} contents Cargo.lock
 * @param {string} packageName
 * @param {string} version
 * @returns {string}
 */
export function replaceLockVersion(contents, packageName, version) {
  const escaped = packageName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(
    `(\\[\\[package\\]\\]\\nname = "${escaped}"\\nversion = ")[^"]+(")`,
  );
  if (!pattern.test(contents)) {
    throw new Error(`Cargo.lock: missing [[package]] entry for ${packageName}`);
  }
  return contents.replace(pattern, `$1${version}$2`);
}

/**
 * @param {string} root
 * @param {string} target
 * @returns {{ current: string; members: string[] }}
 */
export function bumpVersion(root, target) {
  const current = readWorkspaceVersion(root);
  // Same rule as next-version: an explicit version must be strictly greater.
  if (resolveNextVersion(current, target) !== target) {
    throw new Error(`Refusing to bump ${current} to ${target}.`);
  }

  const cargoPath = join(root, "Cargo.toml");
  const cargo = readFileSync(cargoPath, "utf8");
  const members = parseWorkspaceMembers(cargo).map((memberPath) =>
    parsePackageName(readFileSync(join(root, memberPath, "Cargo.toml"), "utf8")),
  );

  const lockPath = join(root, "Cargo.lock");
  let lock = readFileSync(lockPath, "utf8");
  for (const member of members) {
    lock = replaceLockVersion(lock, member, target);
  }

  writeFileSync(cargoPath, replaceWorkspaceVersion(cargo, target), "utf8");
  writeFileSync(lockPath, lock, "utf8");
  return { current, members };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const argv = process.argv.slice(2);
  if (argv.length !== 1) {
    console.error("Usage: node tooling/release/bump-version.mjs <X.Y.Z>");
    process.exit(1);
  }
  try {
    const { current, members } = bumpVersion(repoRoot(), argv[0]);
    console.log(
      `Bumped workspace version ${current} -> ${argv[0]} (${members.length} lock entries: ${members.join(", ")}).`,
    );
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}
