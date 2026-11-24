#!/usr/bin/env node
// Append-only guard for the published SQLite migration set
// (contract: docs/storage-compatibility.md). Published files are immutable; a schema change
// appends a new NNNN_*.sql whose prefix is greater than every locked one.
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const migrationsDir = join(root, "crates", "mindcore-sqlite", "migrations");
const lockPath = join(root, "tooling", "migrations", "migrations.lock.json");
const NAME = /^(\d{4})_.*\.sql$/;
const prefixOf = (file) => Number(NAME.exec(file)[1]);
const sha256 = (file) =>
  createHash("sha256").update(readFileSync(join(migrationsDir, file))).digest("hex");

const locked = JSON.parse(readFileSync(lockPath, "utf8"));
const failures = [];

const lockedNames = new Set();
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
  lockedNames.add(file);

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

const present = readdirSync(migrationsDir)
  .filter((file) => NAME.test(file))
  .sort();

const appended = [];
for (const file of present) {
  if (lockedNames.has(file)) continue;
  const prefix = prefixOf(file);
  if (prefix > latestLocked) {
    appended.push(file);
  } else {
    failures.push(
      `migration ${file} is not locked and its prefix ${prefix} does not come after every locked ` +
        `prefix (${latestLocked}); the set is append-only, so a new migration must be numbered ` +
        `after the published ones.`,
    );
  }
}

console.log(`locked:  ${locked.map((entry) => entry.file).join(", ") || "(none)"}`);
console.log(`present: ${present.join(", ") || "(none)"}`);
console.log(`appended but unlocked: ${appended.join(", ") || "(none)"}`);

if (failures.length > 0) {
  for (const failure of failures) console.error(`error: ${failure}`);
  process.exit(1);
}
