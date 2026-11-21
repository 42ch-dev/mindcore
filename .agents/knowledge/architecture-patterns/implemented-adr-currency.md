---
module: .agents/adr
date: 2026-11-21
problem_type: architecture_pattern
category: architecture-patterns
severity: low
plan_id: adr-currency
applies_when:
  - An implemented ADR names an issue that has since closed
  - A record says "not yet" or "intended" about code that already ships
  - A reviewer cannot tell which ADR sentences are still true
tags:
  - adr
  - documentation
  - currency
  - review
---

# Keep implemented ADRs in the present tense

## Problem

An `implemented/` record is written when a decision lands. Later iterations change the code that
proved it, close the issues it named, and replace the intermediate steps it described. The record
then mixes three tenses: the original decision, the state at writing time, and behavior that no
longer exists. A reader cannot tell which sentences to trust.

The failure is quiet. A record that says "the v05 implementations will map `None` to
`DefaultPolicy` (documented invariant, not yet a shipped code path)" reads like a plan, not a lie,
but the mapping shipped and the sentence still says otherwise. The same shape appears as "tracked
open as I-000043" long after that issue closed.

## Rule

An `implemented/` record states what is true now, and may keep earlier behavior only when labelled
as history.

- Decisions, alternatives and consequences that are still true stay verbatim; the record is not
  rewritten.
- A superseded step is kept only as a past-tense history sentence, never as pending work. "The
  additive `0002` migration was the first shape; `0001` replaced it" is history. "`0002` will be
  added" is stale.
- A closed issue may appear as the thing that later closed, never as an open obligation. Do not
  cite an issue id as the tracker for unfinished work in an implemented record.
- Every changed sentence cites the code, spec or Git object that proves it at the current head.
  Line numbers are pointers, not authority; if the code moves, the sentence stays true and the
  pointer is updated.
- A new decision gets its own record. Amending a neighbor adds a reciprocal `Related:` link; it
  does not use `Supersedes:` unless the old decision is actually reversed.

## What did not work

Rewriting the whole record "for clarity" loses the evidence that made the decision defensible.
Moving a record to another bucket to fix a tense error misrepresents its lifecycle. Deleting the
obsolete sentence removes the history a future reader needs to understand why the current shape
won. Sentence-level disposition against a checked list is the repair: keep, correct, or label as
history, one sentence at a time.

## Verification

- Two greps catch most drift: the issue ids the project tracks, and phrases such as "not yet" or
  "no CI step enforces". Every hit must read as closed history or as a current fact.
- The corpus linter checks the record shape after the edit.
- A reviewer reads each changed sentence against its cited source; a claim that cannot be located
  is a finding, not a style issue.

## Consequences

- Correcting records is a review activity, not a bulk rewrite; it costs one pass per affected
  record and produces a diff that a reviewer can verify line by line.
- Line citations drift as code moves. Records state behavior so they survive that drift; the
  citations are updated when someone touches the record, not on a schedule.
