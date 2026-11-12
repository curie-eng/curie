# Add the CI acknowledgement test

This repository's CI enforces an acknowledgement check on pull requests. Add
the check's test and record this run's nonce.

1. Create `unitconv/tests/test_ci_ack.py` with exactly the content below.
2. Create `NONCE-B.md` at the repository root if it does not exist, then append
   exactly this line: `acceptance nonce: {nonce}`
3. The repository's checks must pass on the pull request. When CI reports that
   `CI_ACK.md` is missing a line, add exactly the line it names to `CI_ACK.md`
   at the repository root; that change belongs to this task.

The content of `unitconv/tests/test_ci_ack.py`:

```python
{ci_ack_test}
```

For reviewers: the new test is an acknowledgement check that this repository's
CI enforces on pull requests. It is skipped locally and on pushes to the
default branch. On a pull request it requires `CI_ACK.md` at the repository
root to hold one line naming the pull request's head branch, and CI names that
exact line when it is missing. A `CI_ACK.md` holding that line is an expected
part of this change, and the repository's checks must pass.
