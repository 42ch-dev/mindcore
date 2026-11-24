#!/usr/bin/env node
// Append-only guard for the published SQLite migration set
// (contract: docs/storage-compatibility.md). Published files are immutable; a schema
// change appends a new NNNN_*.sql whose prefix is greater than every locked one.
//
// The published set is the lock at the release this tree must stay compatible with
// (see tooling/git/baseline.mjs). Comparing against that lock — not only against the
// working-tree lock — is what stops an edit of a published file together with its lock
// hash from passing: the copy that shipped under the baseline tag has other bytes.
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { baselineTag, git, isShallow, SHALLOW_HINT } from "../git/baseline.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const migrationsDir = join(root, "crates", "mindcore-sqlite", "migrations");
const lockInRepo = "tooling/migrations/migrations.lock.json";
const lockPath = join(root, lockInRepo);
const NAME = /^(\d{4})_.*\.sql$/;
const prefixOf = (file) => Number(NAME.exec(file)[1]);
const sha256 = (file) =>
  createHash("sha256").update(readFileSync(join(migrationsDir, file))).digest("hex");

const locked = JSON.parse(readFileSync(lockPath, "utf8"));
if (!Array.isArray(locked)) {
  console.error(`error: ${lockInRepo} is not a JSON array`);
  process.exit(1);
}
const failures = [];

const lockedHashes = new Map();
let latestLocked = -1;
for (const { file, sha256: expected } of locked) {
  if (!NAME.test(file)) {
    failures.push(`lock entry ${JSON.stringify(file)} is not a NNNN_*.sql migration name`);
    continue;
  }
  const prefix = prefixOf(file);
  if (prefix <= latestLocked) {
    failures.push(`lock entry ${file} is out of order: locked prefixes must strictly increase`);
  }
  latestLocked = Math.max(latestLocked, prefix);
  lockedHashes.set(file, expected);

  let actual;
  try {
    actual = sha256(file);
  } catch {
    failures.push(`locked migration ${file} is missing from crates/mindcore-sqlite/migrations/`);
    continue;
  }
  if (actual !== expected) {
    failures.push(
      `locked migration ${file} was edited: sha256 is ${actual}, locked ${expected}. ` +
        `A published migration is immutable — restore it and append a new NNNN_*.sql instead.`,
    );
  }
}

// What already shipped. A tag whose tree has no lock file shipped no migrations, and
// with no baseline tag the working-tree lock is the first published set.
const baseline = baselineTag(root);
if (!baseline && isShallow(root)) failures.push(SHALLOW_HINT);
if (baseline && git(root, ["cat-file", "-e", `${baseline}^{commit}`]) === null) {
  failures.push(
    `baseline tag ${baseline} is not in this clone; a shallow checkout hides the published ` +
      `set. Fetch the tags and their history (actions/checkout: fetch-depth: 0).`,
  );
}
// A tag without the lock file shipped no migrations: that is the pre-lock history.
const shippedLock = baseline ? git(root, ["show", `${baseline}:${lockInRepo}`]) : null;
let shipped = [];
try {
  if (shippedLock !== null) shipped = JSON.parse(shippedLock);
} catch {
  shipped = null;
}
if (!Array.isArray(shipped)) {
  console.error(`error: ${baseline}:${lockInRepo} is not a JSON array; cannot read the published set`);
  process.exit(1);
}

let latestShipped = -1;
for (const { file, sha256: shippedHash } of shipped) {
  if (!NAME.test(file)) {
    failures.push(`baseline ${file} is not a NNNN_*.sql migration name`);
    continue;
  }
  latestShipped = Math.max(latestShipped, prefixOf(file));
  if (!lockedHashes.has(file)) {
    failures.push(
      `published migration ${file} (${baseline}) is missing from ${lockInRepo}; a released ` +
        `migration cannot be dropped from the published set.`,
    );
    continue;
  }
  if (lockedHashes.get(file) !== shippedHash) {
    failures.push(
      `published migration ${file} changed its lock hash since ${baseline}: ` +
        `${lockedHashes.get(file)}, shipped ${shippedHash}. Published files and their lock ` +
        `entries are immutable.`,
    );
    continue;
  }
  let actual;
  try {
    actual = sha256(file);
  } catch {
    failures.push(`published migration ${file} (${baseline}) is missing from the working tree`);
    continue;
  }
  if (actual !== shippedHash) {
    failures.push(
      `published migration ${file} was edited since ${baseline}: sha256 is ${actual}, ` +
        `shipped ${shippedHash}. A published migration is immutable — restore it and append ` +
        `a new NNNN_*.sql instead.`,
    );
  }
}

// New work must land after everything published, including a prefix the baseline lock
// carried and the working-tree lock dropped or reordered.
const latestPublished = Math.max(latestLocked, latestShipped);

for (const { file } of locked) {
  if (!NAME.test(file) || shipped.some((entry) => entry.file === file)) continue;
  if (prefixOf(file) <= latestShipped) {
    failures.push(
      `migration ${file} is locked at prefix ${prefixOf(file)}, which does not come after every ` +
        `prefix published at ${baseline} (${latestShipped}); the set is append-only.`,
    );
  }
}

const present = readdirSync(migrationsDir).sort();

// sqlx's migrate! accepts short versions, but this contract requires exactly four;
// a present *.sql the guard cannot parse must fail loudly, never drop silently.
for (const file of present) {
  if (file.endsWith(".sql") && !NAME.test(file)) {
    failures.push(
      `migration file ${file} cannot parse as a NNNN_<description>.sql name; present ` +
        `migration files must be numbered with exactly four digits — rename or remove it.`,
    );
  }
}
const named = present.filter((file) => NAME.test(file));

const appended = [];
for (const file of named) {
  if (lockedHashes.has(file)) continue;
  const prefix = prefixOf(file);
  if (prefix > latestPublished) {
    appended.push(file);
  } else {
    failures.push(
      `migration ${file} is not locked and its prefix ${prefix} does not come after every ` +
        `published prefix (${latestPublished}); the set is append-only, so a new migration must ` +
        `be numbered after the published ones.`,
    );
  }
}

console.log(`baseline: ${baseline ?? "(none — first published set)"}`);
console.log(`locked:   ${locked.map((entry) => entry.file).join(", ") || "(none)"}`);
console.log(`present:  ${named.join(", ") || "(none)"}`);
console.log(`appended but unlocked: ${appended.join(", ") || "(none)"}`);

if (failures.length > 0) {
  for (const failure of failures) console.error(`error: ${failure}`);
  process.exit(1);
}
