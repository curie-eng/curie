# How the factory follows your team's coding standards

Teams adopting the Software Factory usually already have coding instructions:
an `AGENTS.md`, a `CONTRIBUTING.md`, a style guide, a pull request template.
This guide says where a factory run reads them, so you know where to put your
guidelines and what wins when they conflict with the issue.

Guidance is per repository. There are no per-developer instructions today:
every run of a repository reads the same committed files.

## What a run reads, and when

The implement skill reads the repository's own guidance at two points in a
run, and the runner keeps workspace instructions in effect for the whole
session.

1. **Planning.** In the `plan` phase the skill reads `AGENTS.md`,
   `CONTRIBUTING.md`, `README.md`, and the project configuration (for example
   `pyproject.toml`, `package.json`, `Makefile`, or `Cargo.toml`) before it
   writes its plan. It looks there for the test and lint commands the project
   documents. Put your build, test, and style commands where that phase can
   find them. Source: section 3 of
   [`examples/dark-factory/skills/implement-issue/SKILL.md`](../../examples/dark-factory/skills/implement-issue/SKILL.md).
2. **Publishing.** In the `publish` phase the skill reads `AGENTS.md` and
   `CONTRIBUTING.md` again, plus the repository's pull request template (often
   under `.github/`) and any CI job that checks pull request bodies. When
   those conventions conflict with the skill's own body outline, the
   repository's conventions win: required trailers, selectors, and body
   format come from your files, not from the skill. Source: section 8 of
   [`examples/dark-factory/skills/implement-issue/SKILL.md`](../../examples/dark-factory/skills/implement-issue/SKILL.md).
3. **The whole session.** The runner loads the coding harness's settings
   from the `user`, `project`, and `local` sources (`_SETTING_SOURCES` in
   [`runner/src/curie_runner/adapter.py`](../../runner/src/curie_runner/adapter.py)),
   so a committed `CLAUDE.md` in the workspace stays in effect during the
   run.

## What the reviewers read

Two read-only reviewers check the work against the issue.

- The **plan reviewer** receives the issue, the numbered acceptance criteria,
  and the plan with its verification table in its prompt, and may read the
  repository checkout with read, grep, and glob tools. It checks that every
  criterion maps to a named edit and a verification row, that the named files
  exist, and that the repository's own instructions still apply. Source:
  [`examples/dark-factory/agents/plan-reviewer.md`](../../examples/dark-factory/agents/plan-reviewer.md).
- The **diff reviewer** reads the publication candidate patch file, the
  issue, the checks the implementer ran, and the plan review's verdict with
  each finding's resolution. It may run `git status`, `git diff`, `git log`,
  and the repository's own test command, it never edits, and any change
  under `.github/` is a blocking finding. Source:
  [`examples/dark-factory/agents/diff-reviewer.md`](../../examples/dark-factory/agents/diff-reviewer.md).

Both reviewers hold that the repository's own instructions still apply, so
the style rules in your `AGENTS.md` or `CLAUDE.md` bind the change they
approve.

## A worked example

Suppose your `AGENTS.md` contains:

```markdown
## Code style

- Functions stay under 40 lines.
- No new dependencies without a linked issue.
- Test file names mirror the module under test.
```

A run that adds a feature plans around those rules, and a reviewer should
expect a matching line in the checklist of the factory's pull request, for
example:

```text
- [x] `uv run pytest -q` passes; `check_order_total` split to stay under the
  40-line rule in AGENTS.md
```

The rules shaped the diff; the pull request body shows the evidence.

## What standards cannot do today

The factory reads committed repository files. A local conversation, an
installed local skill, or a private file does not reach it unless its content
is committed where a run reads it. Guidance is per repository, not per
developer.
