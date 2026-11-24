# AGENTS.md — .agents/ (harness layer)

- Path symbols. The values below are the **bootstrap default** layout — the paths
  this scaffold creates when the repository declares nothing; a `.mstarc` declaration
  overrides them (resolve the live values with `mstar path resolve`):
  {HARNESS_DIR} = .mstar/; {PLAN_DIR} = .mstar/plans/; {SDD_DIR} = .mstar/sdd/<plan-id>/;
  {ITERATION_DIR} = .mstar/iterations/; {KNOWLEDGE_DIR} = .agents/knowledge/;
  {SPECS_DIR} = .agents/specs/; {ADR_DIR} = .agents/adr/; {WORKFLOW_DIR} = .mstar/workflows/;
  {PROJECT_DIR} = .mstar/projects/ (SSOT: skills/mstar-conventions; .mstarc declares the layout).
- Process vs results: process artifacts (plans/, iterations/, sdd/, status.json, workflows/,
  projects/) stay under {HARNESS_DIR} and are gitignored; results (this file, {KNOWLEDGE_DIR},
  {SPECS_DIR}, {ADR_DIR}) are tracked and shared across clones.
- Done: only @project-manager may set Done; implementers set InReview.
- User docs are bilingual: `README.md` (English) and `README.zh-CN.md` (Chinese) mirror each
  other, links and badges included, and change in the same commit. The Chinese README is the only
  translated user entry; `docs/`, specs, ADR and knowledge files stay English.
- `docs/` follows Diátaxis (tutorial / how-to / reference / explanation) and links to
  `{SPECS_DIR}` instead of restating it. `docs/assets/architecture.svg` is generated from
  `docs/assets/architecture.archify.json`; regenerate rather than hand-edit the SVG.
- Do not restate `CONTRIBUTING.md` or `CHANGELOG.md` in either README; link to them.
