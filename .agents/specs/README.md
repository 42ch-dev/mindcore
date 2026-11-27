# Specs

Current-state specifications of the Mindcore repository. Each page describes the state that is true now; history lives in Git and in the ADR tree.

| Spec | Covers |
| --- | --- |
| [`architecture/mind-model.md`](architecture/mind-model.md) | The Mind model, record transitions, and the embedded SQLite persistence contract |
| [`architecture/storage-ir-boundary.md`](architecture/storage-ir-boundary.md) | The storage-independent IR, its wire codec fidelity rule, and the repository transaction boundary |
| [`architecture/facade.md`](architecture/facade.md) | The in-process facade over a Mind: operations, transactions, and the review fold |
| [`architecture/serving-principal-routing.md`](architecture/serving-principal-routing.md) | Per-principal route map, carrier admission and the recall knowledge slot |
| [`architecture/daemon-kernel.md`](architecture/daemon-kernel.md) | The sidecar daemon: config, the store lock, framing, lifecycle, composition and shutdown |
| [`architecture/native-binding.md`](architecture/native-binding.md) | The napi-rs binding: exported surface, the result object, declarations and addon loading |
| [`architecture/serving-seam.md`](architecture/serving-seam.md) | Signed Connect serving: Knowledge face, workflow tools, and the store shape they require |
| [`data/cognition-record-storage.md`](data/cognition-record-storage.md) | Per-record cognition storage, the carrier head, and its CAS/projection rules |
| [`data/embedding-vector-store.md`](data/embedding-vector-store.md) | Identity-keyed record vectors, the query slot, and the exact-scoring read/fill order |
| [`data/store-schema.md`](data/store-schema.md) | The appended migrations `MindStore::open` applies, the tables they create, and the append-only policy for changing them |
| [`data/knowledge-lifecycle.md`](data/knowledge-lifecycle.md) | Revision history, replay by nearest checkpoint, adoption over two ports, and retirement by marker |
| [`data/transfer-bundle-chunking.md`](data/transfer-bundle-chunking.md) | What an export bundle carries, how it is chunked and reassembled, and what import staging and activation accept |
