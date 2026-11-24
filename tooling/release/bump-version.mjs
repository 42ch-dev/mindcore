#!/usr/bin/env node
/**
 * Bump the workspace version: `[workspace.package] version` in `Cargo.toml`,
 * every workspace member `[[package]]` version in `Cargo.lock`, and every inline
 * `version = "..."` requirement on a path dependency between members.
 *
 * Member manifests take their own version from `version.workspace = true`, but an
 * inline dependency (`{ path = "../mindcore-base", version = "0.1.0" }`) still
 * carries a number whose `^` requirement would reject the bumped version.
 *
 * CLI: node tooling/release/bump-version.mjs <X.Y.Z>
 *
 * @module tooling/release/bump-version
 */

import { readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
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
  const pattern = /^version\s*=\s*"[^"]*"/m;
  // Match, not changed-string: a first release keeps the same version text.
  if (!pattern.test(section)) {
    throw new Error('Cargo.toml: missing version = "..." in [workspace.package]');
  }
  return contents.replace(section, () => section.replace(pattern, `version = "${version}"`));
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
 * Rewrite the `version` requirement of every inline dependency on a workspace
 * member. A dependency counts as internal when its `path` resolves to a member
 * directory, or when its name is a member package name. External dependencies
 * carry no `path` and are left alone.
 *
 * shortcut: only single-line inline tables are rewritten; a member dependency
 * spread over several lines is left as is.
 *
 * @param {string} contents A member Cargo.toml.
 * @param {{ dir: string; name: string }} self
 * @param {{ dir: string; name: string }[]} members
 * @param {string} version
 * @returns {string}
 */
export function replaceMemberDependencyVersions(contents, self, members, version) {
  const internalDirs = new Set(members.map((member) => resolve(member.dir)));
  const internalNames = new Set(members.map((member) => member.name));
  return contents.replace(
    /^(\s*)([A-Za-z0-9_-]+)\s*=\s*\{([^}]*)\}/gm,
    (line, indent, name, body) => {
      const path = body.match(/(?:^|,)\s*path\s*=\s*"([^"]+)"/)?.[1];
      const internal = path === undefined
        ? internalNames.has(name)
        : internalDirs.has(resolve(self.dir, path));
      if (!internal) {
        return line;
      }
      const requirement = /(^|,)(\s*version\s*=\s*)"[^"]*"/;
      if (!requirement.test(body)) {
        return line;
      }
      return `${indent}${name} = {${body.replace(requirement, `$1$2"${version}"`)}}`;
    },
  );
}

/**
 * @param {string} root
 * @param {string} target
 * @returns {{ current: string; members: string[] }}
 */
export function bumpVersion(root, target) {
  const current = readWorkspaceVersion(root);
  // Same rule as next-version: an explicit version must be strictly greater,
  // except equality before the first release (no v* tag yet).
  if (resolveNextVersion(current, target, root) !== target) {
    throw new Error(`Refusing to bump ${current} to ${target}.`);
  }

  const cargoPath = join(root, "Cargo.toml");
  const cargo = readFileSync(cargoPath, "utf8");
  const manifests = parseWorkspaceMembers(cargo).map((memberPath) => {
    const dir = join(root, memberPath);
    const contents = readFileSync(join(dir, "Cargo.toml"), "utf8");
    return { dir, contents, name: parsePackageName(contents) };
  });
  const members = manifests.map((manifest) => manifest.name);

  const lockPath = join(root, "Cargo.lock");
  let lock = readFileSync(lockPath, "utf8");
  for (const member of members) {
    lock = replaceLockVersion(lock, member, target);
  }

  for (const manifest of manifests) {
    const updated = replaceMemberDependencyVersions(manifest.contents, manifest, manifests, target);
    if (updated !== manifest.contents) {
      writeFileSync(join(manifest.dir, "Cargo.toml"), updated, "utf8");
    }
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
