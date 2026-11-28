# ADR: `memory.grants.list` answers wider than it admits, and its marker follows the answer
Status: implemented — the operation serves admins the whole space and computes the marker over it.
Date: 2026-11-28

## Problem

`memory.grants.list` admits caller-only, but an administrator must see the whole space's grant set
to administer it. The same operation returns a cursor marker, and if the marker were computed over
the admission rows while the page were built from the wider set, a page would be validated against
rows the admin never saw. Another principal's grant change would then silently invalidate the
admin's cursor.

## Decision

Admission stays caller-only. An admin is served every grant in the space, and the marker is
computed over that served set. Every other caller is served exactly the rows admission read. The
principal and subject filters apply to the served set, and the cursor scope string keeps
operation, space, subject filter and applied marker separate.

## Alternatives considered

- **Serve every caller the whole space.** Rejected: it breaks the caller-only admission rule for
  callers with no administrative capability.
- **Keep the marker over admission's rows while serving the wider set.** Rejected: the marker
  would describe rows the answer does not contain, so a change by another principal would
  invalidate a page the admin did see.
- **Refuse the admin view.** Rejected: it removes the operation's administrative purpose.

## Consequences

An admin's page is invalidated by a grant change made by any principal in the space. That is the
price of a marker that describes the answer rather than the admission, and it is the same
snapshot rule the other list operations use.
