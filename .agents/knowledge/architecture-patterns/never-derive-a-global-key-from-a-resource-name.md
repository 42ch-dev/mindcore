---
module: mindcore-sqlite
date: 2026-11-26
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
applies_when:
  - A write creates a row whose primary key is derived from another row's identifier
  - Several spaces, tenants or principals share one table with a global primary key
tags:
  - identity
  - primary-key
  - namespace
  - create
---

# Never derive a global primary key from a name another namespace controls

## Context

`memory_grants.grant_id` is the table's primary key, so it is global across every space. A create
operation that wrote its default grant as `grt_<space_id>` therefore let a grant in an unrelated
space reserve the name of a Mind that did not exist yet: the fourth write failed, the transaction
rolled back, and the caller was told the Mind already existed. Mint the key; do not derive it.

## Guidance

1. Mint the key from the store's own identity source (`mindcore_base::ids::mint` here), never from
   a caller-chosen or resource-derived name.
2. Map an insert failure to the coordinate it actually names. A grant-insert conflict is not a
   taken resource id; reporting it as one turns an unrelated row into a permanent denial for a
   name the caller never used.
3. Keep the derived name only where it is not a key: a display name, a collection prefix the
   operation chooses itself, or a path segment inside one namespace.
4. If a key must be derived, put its namespace in the key. A derived key that omits its namespace
   is a collision waiting for the second tenant.
5. Test the cross-namespace case directly: seed the same-shaped key in another namespace and
   assert the new resource is still creatable.

## Why This Matters

A global primary key is a shared namespace. Deriving it from a name that belongs to one tenant
silently couples tenants: one row blocks another, the refusal names the wrong coordinate, and the
block lasts as long as the unrelated row exists. The symptom reads as a correctness bug in the
create path; the cause is a key that was never unique to the thing it identifies.

## When to Apply

- A create writes a row whose key is composed from another row's id or from a display name.
- Several tenants, spaces or principals share one table.
- A refusal says "already exists" for an object the caller has not created.

## Examples

### Before

```rust
// The key belongs to the space, not to the grant: another space's grant can hold this name.
let grant_id = format!("grt_{space_id}");
```

### After

```rust
// The key belongs to the row, so nothing outside this create can occupy it.
let grant_id = mindcore_base::ids::mint("grt");
```
