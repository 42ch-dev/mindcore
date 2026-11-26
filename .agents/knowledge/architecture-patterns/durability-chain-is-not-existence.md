# A durability chain must not infer durability from existence

## Context

The facade's journal publisher writes projection files, then fsyncs the file and the directory entries that name it, so a committed database row never points at bytes that a power loss can drop. The first version walked "the directories that do not exist yet" and fsynced each — and stopped at the first directory that already existed, on the assumption that existing means durable. It is not: a previous attempt can create a directory and fail before syncing it, so the next attempt sees it as existing and skips the entry that is still unpersisted. The same assumption broke a bare relative root (`journal_dir("journal")` or `./journal`), whose parent is the empty path, not the current directory.

## Rule

When persisting a path's ancestry, decide the sync set from the **path the caller names**, never from what the filesystem happens to contain. If existence is used at all, treat it as a hint that must be re-derived on every attempt, and make the set of synced entries a pure function of the target path.

The strongest simple form is to bound the contract instead: require the durable ancestor to be the host's responsibility (a pre-existing root), refuse a missing one with the requirement named, and sync every entry the publisher itself creates beneath it. Then no retry can be weaker than the first attempt, because both attempts compute the same set.

## Why

Durability is a property of an operation, not of a directory entry. A directory that "exists" may have been created by a failed attempt that never synced its parent; the entry is visible to the next process and invisible to the storage after a crash. Code that reads existence as durability silently narrows its own guarantee, and the narrowing is invisible in tests because an ordinary test filesystem never loses anything.

## How to apply

- Compute the sync set from the target path (and a documented anchor), not from `exists()`.
- Prefer making the durable ancestor a declared host contract when the library cannot prove it.
- Refuse a missing root with an error that names the requirement, rather than creating it and hoping.
- Pin the invariant with a test that fails when the computation is replaced by an existence check; prove the test has teeth by temporarily restoring the old behaviour.
- Empty-path components are a separate trap: `parent()` of a single relative component is `""`, which is neither `None` nor the current directory. Normalise it explicitly.

Seen in: `crates/mindcore-runtime/src/journal.rs`, the facade plan's B4/F-006/F-008 rounds.
