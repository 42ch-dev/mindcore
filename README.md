# mindcore

Mindcore is a settlement layer for memory, knowledge and cognition: one durable place to settle
what an agent heard, what it adopted as personal knowledge, and what it currently judges to be
true — with the provenance that keeps those three apart.

This iteration ships a Rust library against the SPOKE contracts. There is no server, no UI and no
network surface.

## Crates

| Crate | Responsibility |
| --- | --- |
| `mindcore-core` | Storage-agnostic domain core: SPOKE contracts and the backend traits they are served through. No I/O. |
| `mindcore-sqlite` | SQLite (SQLx) backend that implements the core storage traits over one local database file. |

## Checks

Requires a stable Rust toolchain.

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

CI runs these same three commands (`.github/workflows/ci.yml`).

## Not yet available

- Remote serving and capability advertisement.
- Memory transfer between minds.
- Migration of an existing store or provider (live data movement).

Each has an owner in the project roadmap; none is a stub here.
