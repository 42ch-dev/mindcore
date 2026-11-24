#!/usr/bin/env node
// Shared release baseline for the release-surface checkers.
//
// The baseline is the release this tree must stay compatible with:
//   - on a tag commit, the previous stable release (the one this release
//     supersedes);
//   - anywhere else, the newest stable release (what consumers already have).
// Only stable `v*` tags qualify: a prerelease tag is Git-only and claims no
// registry version, so it is never a baseline. No stable tag at all means
// nothing has shipped yet, so callers fall back to their "first published set"
// behaviour. CI must check out with `fetch-depth: 0`; a shallow clone hides the
// tags, so the lookups below answer "no baseline" and the callers turn that
// into an error instead of a silent pass.

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

// Newest stable release tag from a `git tag --list` argument set: a full
// `vX.Y.Z` with no prerelease suffix. Aliases such as `v2` are ignored even when
// version sort ranks them first. `args` carries the list pattern and any
// --merged filter.
const RELEASE_TAG = /^v\d+\.\d+\.\d+$/;

function newestStable(cwd, args) {
  const tags = git(cwd, ['tag', '--list', ...args, '--sort=-v:refname']);
  if (tags === null) return null;
  return tags.split('\n').find((tag) => RELEASE_TAG.test(tag)) ?? null;
}

export function baselineTag(cwd) {
  const current = git(cwd, ['describe', '--tags', '--exact-match', '--match', 'v*', 'HEAD']);
  if (current !== null) {
    // The first release has no predecessor: skip the diff rather than compare a
    // tag against itself. `--merged <current>^` keeps the lookup to tags that
    // were already released when this tag shipped.
    return newestStable(cwd, ['v*', '--merged', `${current.trim()}^`]);
  }

  return newestStable(cwd, ['v*']);
}

// A shallow clone hides the tags, which turns a real baseline into "no baseline".
// Every caller states that instead of silently weakening its check.
export const SHALLOW_HINT =
  'shallow clone: the baseline tag may be hidden, so this check compared against nothing. ' +
  'Fetch the tags and their history (actions/checkout: fetch-depth: 0).';

export function isShallow(cwd) {
  return git(cwd, ['rev-parse', '--is-shallow-repository'])?.trim() === 'true';
}
