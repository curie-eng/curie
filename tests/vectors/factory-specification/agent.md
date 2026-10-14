## Problem
A synthetic counter loses its increment when reopened.

## Base
next

## Blocked by
None

## Decisions
Persist the counter as an integer. All decisions are fixed.

## Preconditions
None

## Acceptance criteria
1. Reopening preserves the counter.
   Verified by: uv run pytest tests/test_counter.py::test_reopen -q
   Runs in: sandbox

2. The integration check proves the increment is preserved.
   Verified by: counter-integration required check
   Runs in: ci:counter-integration

## Out of scope
Counter UI changes.
