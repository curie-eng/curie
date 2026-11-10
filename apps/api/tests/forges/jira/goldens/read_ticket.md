# Fix the login redirect

## Background

Users hit **a loop** after *signing in* with `SSO`, see [the runbook](https://wiki.acme.example/runbook) and the red text.
Second line after a break.

- First step
  - Nested detail
- Second step

1. Reproduce
2. Patch

```python
print("hello")
```

> Quoted customer report

---

| Browser | Result |
| --- | --- |
| Firefox | Loops |

> Panel note for the team

Ask @Mia Krystof before merging.

[attachment: login-loop.png]

[attachment: 00000000-0000-4000-8000-0000000000e3]

Hidden details here.

## Comment by Mia Krystof at 2026-03-01T10:15:00.000-0500

Repro confirmed on staging.

## Comment by Sam Rivera at 2026-03-02T08:00:00.000-0500

@Curie Factory please take this.
