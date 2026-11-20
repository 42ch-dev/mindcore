# Specs

Current-state specifications of the Mindcore repository. Each page describes the state that is true now; history lives in Git and in the ADR tree.

| Spec | Covers |
| --- | --- |
| [`architecture/mind-model.md`](architecture/mind-model.md) | The Mind model, record transitions, and the embedded SQLite persistence contract |
| [`architecture/storage-ir-boundary.md`](architecture/storage-ir-boundary.md) | The storage-independent IR, its wire codec fidelity rule, and the repository transaction boundary |
| [`architecture/serving-seam.md`](architecture/serving-seam.md) | Signed Connect serving: Knowledge face, workflow tools, and the store shape they require |
| [`data/cognition-record-storage.md`](data/cognition-record-storage.md) | Per-record cognition storage, the carrier head, and its CAS/projection rules |
| [`data/embedding-vector-store.md`](data/embedding-vector-store.md) | Identity-keyed record vectors, the query slot, and the exact-scoring read/fill order |
