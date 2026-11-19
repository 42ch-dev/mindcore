# Contributing

## Toolchain

Stable Rust, edition 2021, resolver `2`. No toolchain file and no declared MSRV in this iteration;
the workspace builds on the current stable release.

## Before you push

Run the same three checks CI runs, from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

Use `--locked` on every Cargo command that resolves dependencies. A dependency change carries its
updated `Cargo.lock` in the same commit; never let a build rewrite the lockfile silently.

## Decisions

One decision, one ADR. When a change picks one option among real alternatives — a type boundary, a
persistence choice, a dependency — record it under `.agents/adr/` with the templates and lifecycle
described there. Routine work needs no ADR.

## Dependencies

Dependencies resolve from the published registry. No path dependencies on sibling repositories:
`spoke-schemas` and `spoke-operations` are version pins, never a local or `git` source, so a fresh
clone builds on its own.
