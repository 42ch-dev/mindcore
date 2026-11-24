#!/usr/bin/env node
// Shared release baseline for the release-surface checkers.
//
// The baseline is the release this tree must stay compatible with:
//   - on a tag commit, the previous tag (the one this release supersedes);
//   - anywhere else, the newest tag (what consumers already have).
// No tag at all means nothing has shipped yet, so callers fall back to their
// "first published set" behaviour. CI must check out with `fetch-depth: 0`; a
// shallow clone hides the tags, so the lookups below answer "no baseline" and
// the callers turn that into an error instead of a silent pass.

import { execFileSync } from 'node:child_process';

// Returns stdout, or null when git fails (not a repo, no history, no such
// object). Callers treat null as "the question has no answer", never as a reason
// to crash.
export function git(cwd, args) {
  try {
    return execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
  } catch {
    return null;
  }
}

export function baselineTag(cwd) {
  const current = git(cwd, ['describe', '--tags', '--exact-match', '--match', 'v*', 'HEAD']);
  if (current !== null) {
    // The first release has no predecessor: skip the diff rather than compare a
    // tag against itself.
    const previous = git(cwd, ['describe', '--tags', '--abbrev=0', `${current.trim()}^`]);
    return previous?.trim() || null;
  }

  const tags = git(cwd, ['tag', '--list', 'v*', '--sort=-v:refname']);
  return tags?.split('\n')[0].trim() || null;
}

// A shallow clone hides the tags, which turns a real baseline into "no baseline".
// Every caller states that instead of silently weakening its check.
export const SHALLOW_HINT =
  'shallow clone: the baseline tag may be hidden, so this check compared against nothing. ' +
  'Fetch the tags and their history (actions/checkout: fetch-depth: 0).';

export function isShallow(cwd) {
  return git(cwd, ['rev-parse', '--is-shallow-repository'])?.trim() === 'true';
}
