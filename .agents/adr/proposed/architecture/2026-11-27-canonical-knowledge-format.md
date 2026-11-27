# ADR: One machine-first knowledge grammar with a deterministic compiler

Status: proposed — no grammar, compiler or gate exists; the direction is agreed and unimplemented
Date: 2026-11-27
Scope: The accepted document syntax, the determinism of outline compilation, the parser choice, and what the write gate refuses
Related: `2026-11-27-machine-owned-knowledge-writes.md`, `2026-11-27-stable-section-identity-and-drift.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-layered-knowledge-retrieval.md`

## Problem

Knowledge entries are JSON payloads whose body is a closed object of summary, tags and attributes (`schemas/data/knowledge-entry.schema.json`). That shape has no sections, so a document cannot be cited by section, no byte range can be verified, and a reader either takes the whole entry or nothing.

Writers are machines, so the syntax can be strict — but without a written grammar, section boundaries become whatever the parser of the day decides, frontmatter is unparsed text, and the gate can neither accept nor reject a document deterministically. If the outline is produced by a model, two runs over the same bytes differ, a cache cannot be keyed on anything stable, and a summary can drift from the text it summarises with no event to notice it. If the gate repairs non-conforming input, two writers submitting the same logical document produce different stored bytes, so a diff review shows changes nobody made.

The stakes are that every downstream decision assumes a document can be compiled to the same outline twice, and that an invalid document cannot enter the store.

## Proposal

The Knowledge domain will accept exactly one Markdown dialect, machine-first and machine-checked.

- **Structure.** One H1 that equals the document's canonical name; a leading metadata block in a restricted key/value grammar rather than general YAML; sections as headings that carry a stable id and one kind; declared constraint, invariant and interface-signature lines in a fixed form; a canonical path derived from the metadata. Section ids will be unique within a document.
- **No HTML escapes.** A document will use the dialect's own constructs only; an inline HTML block that changes rendering will be refused. The heading id and kind attribute will be the only allowed attribute form.
- **Whitespace will be significant, with one exact exception.** Trailing whitespace outside fenced blocks will be refused, because inside a fence it is content and outside it two trailing spaces are a hard break; a silent trim would change meaning. The grammar will therefore provide an explicit hard-break marker instead of a trailing-space one. The only transformation the compiler may treat as equivalent is a change in the number of blank lines outside fenced blocks, and that equivalence will be used only to compute the drift fingerprint, never to accept a different document (`2026-11-27-stable-section-identity-and-drift.md`).
- **Compilation will be a pure function of bytes and compiler version.** The L0 card and L1 outline will be derived from declared metadata, headings, kinds and declared blocks. There will be no model, no network, no clock and no randomness in compilation or in the gate. The same bytes and version will produce the same output on any host.
- **The cache key will be `(blob, compiler version)`.** A compiler change will invalidate derived rows rather than reinterpret them (see `2026-11-27-derived-knowledge-indexes.md`).
- **A model enhancement will be a separate derived artifact, never part of compilation.** The deterministic L0 card and L1 outline will remain the required baseline, produced only from document bytes, metadata, AST and compiler version. An optional model-generated abstract or summary may be derived from that projection, but it will never be required for availability, the write gate, compilation or baseline ranking. The artifact will carry model id/version, prompt id/version and input digest (visible projection, document version and compiler version); its cache identity will include all of them. A missing, stale or failed artifact will fall back to the deterministic view, and an enhancement will never silently replace that view: a response will identify which artifact was used.
- **The gate will reject and never repair.** Every violation will be reported with its rule and byte range, and nothing will be stored. A document will not be normalised, reformatted or re-serialised on the way in.
- **Parser.** `pulldown-cmark` will be the reference parser: it yields events with byte ranges over the source, exposes heading ids and attributes, and is a single pass. `comrak` is a genuine alternative — an arena tree with source positions and a front-matter extension — but it carries a heavier build and a newer minimum Rust version. The grammar will be defined by rule vectors, not by either parser, so a second parser can be admitted only against the same vectors.

## Alternatives considered

- **YAML frontmatter.** Familiar and expressive. Rejected: anchors, folds, implicit typing and duplicate-key rules make the parse ambiguous at the edges, and every ambiguity becomes a gate exception or a dependency choice. A small explicit key/value grammar needs no YAML parser.
- **TOML frontmatter.** Unambiguous and standardised. Rejected as more grammar surface than the metadata needs, with awkward multiline values for a summary.
- **A JSON document.** Least ambiguous. Rejected: prose becomes string escapes, line-level review and diff reading degrade, and the token cost of quoting rises.
- **HTML or XML.** Rejected: verbosity inflates context and review noise, and the format would fight the Markdown-shaped tooling people already read.
- **Free Markdown plus a model-based normaliser at write time.** Rejected: nondeterministic output, unbounded cost per write, no rule a gate can enforce, and a model becomes the gate.
- **Put a model summary in the compiler's required output.** Rejected: it would make an optional, nondeterministic artifact part of the gate, the cache key and baseline ranking. The optional enhancement is a separate derived artifact with provenance and a deterministic fallback, never a compiler dependency.
- **reStructuredText or AsciiDoc.** Viable, with stronger directed structure. Rejected because byte-exact span support and heading-attribute support are weaker in the available Rust parsers, and either would need a grammar of its own before the gate could exist.
- **Keep JSON entries and synthesise sections in code.** Rejected: the document's structure would live in the reader rather than in the document, ids would not survive a rewrite, and a second consumer would rebuild a different outline.

## Acceptance criteria

- A positive fixture bundle will compile with its byte ranges, both section digests and an outline asserted exactly, from bytes written in the test.
- Each grammar rule will have at least one negative fixture refused with the rule name and the offending byte range.
- Compiling the same bytes twice, in two processes and on two hosts, will produce identical output bytes.
- A compiler version change will yield a different cache key and will leave no derived row attributed to the old version.
- A static check will show no model, network or clock dependency in the compile and gate path.
- A write with no model artifact will compile and be gated identically to one with an artifact present; a model call will never be on the gate's critical path.
- An enhancement artifact will expose model, prompt and input digest, and a change to any of them will produce a new cache identity; a missing or failed artifact will return the deterministic view rather than a summary it cannot justify.
- An enhancement will never silently replace the deterministic L0/L1 view, and a response that uses it will say so.
- A stored document will be byte-identical to the submitted bytes; no path will normalise it.
- A document without a heading id, with a duplicate id, with a reused id, with an invalid lineage target, or with trailing whitespace outside a fence will be refused; a blank-line-only change outside a fence will be accepted and will compile to the same drift fingerprint; an indentation change that alters block structure or any change inside a fence will compile to a different fingerprint.

## Risks

- **Strictness blocks writers.** A writer that emits near-Markdown will be refused. Mitigation: rejections name the rule and the byte range, writer tooling targets the grammar, and a grammar change is a new ADR with a compiler version, not a gate exception.
- **Grammar and compiler versions must move together.** A grammar change without a version change would reinterpret stored documents silently. Mitigation: grammar and compiler versions are recorded on the commit and in the cache key; a change triggers rebuild, not reuse.
- **Parser differences.** A second parser may accept different edge cases. Mitigation: rule vectors are the contract and a parser is admitted only when it passes them; until then only the reference parser is supported.
- **Declared metadata duplicates Facts.** A document summary can look like a Fact. Mitigation: the boundary record says where a datum belongs; metadata exists to compile an outline, not to store claims.
- **Explicit constraint syntax costs bytes.** Mitigation: it is what makes L1 decision-dense; the cost gate measures the pack, not the document.
- **An optional enhancement is nondeterministic and costs calls.** Mitigation: it is never on the write, compile or baseline-ranking path; its provenance and cache identity are explicit, and the deterministic view is the fallback. The evaluation reports it separately from the mandatory baseline.
