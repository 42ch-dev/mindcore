# Architecture decision records

Every record here is one decision made **among real alternatives**, filed so a reader finds it without an index: the bucket gives the fate, the class the domain, the date sorts the rest.

## Directory shape

```
{bucket}/{class}/yyyy-mm-dd-topic.md
```

- **Bucket** — the fate: `proposed/`, `implemented/`, `rejected/`, `archived/`.
- **Class** — the domain: `feature/`, `bug-fix/`, `simplification/`, `architecture/`, `process/`, `testing/`. There is no `refactor`: the judge is mechanical — observable behavior changes → `bug-fix` or `feature`; it does not → `simplification`.
- **Class directories are created by the first record** of that class. They are deliberately absent from the empty tree: Git does not track empty directories, and 24 of them are noise.
- **Filename** — the identity: the record's **original** date plus a lowercase kebab topic. It never changes across a lifecycle move; only the bucket directory moves.

## Skeletons

- `proposed/TEMPLATE.md` — `Problem`, `Proposal`, `Alternatives considered`, `Acceptance criteria`, `Risks`.
- `implemented/TEMPLATE.md` — `Problem`, `Decision`, `Alternatives considered`, `Consequences`. `Decision` is present tense, never "we will".
- `rejected/` has no third template: it reuses the proposed skeleton's shape with a frozen body.

## Lifecycle

Three moves exist, each landed as one change — move, rewrite and header together: `proposed → implemented`, `proposed → rejected`, `implemented → archived`. The first line of a record is `# ADR: <title>`; the header also carries `Status: <state> — <why>`. For `proposed/`, `implemented/` and `rejected/` the state equals the containing bucket; an archived record keeps `Status: implemented — <why>` and carries an additional `Archived: yyyy-mm-dd — <reason>` line below it. An archive records a reversal of the decision, not a fourth status.
