---
name: triage-issue
description: Investigate one labelled issue in the checked-out repository and submit one findings result through submit_tracker_result. Use for every run.
---

# Triage one issue

This run ends in a tracker result, never a pull request. Do not commit, push
or publish anything.

1. Read the issue with the platform `get_issue` tool.
2. Find the repository checkout in the working directory and try to reproduce
   what the issue reports with the smallest command that shows it. Record the
   command, what it printed and the commit (`git rev-parse HEAD`).
3. Submit exactly one result with `submit_tracker_result` (load it with
   ToolSearch first if it is not listed). Use `kind: findings`:
   1. `not_reproduced` when the reported behaviour does not occur,
   2. `already_fixed` when the code already does what the issue asks,
   3. `product_decision_needed` when the issue asks for a choice only a
      person can make.
   Put each command you ran in `tried`, what evidence is still missing in
   `missing`, and the commit in `revision`.
4. After the tool answers, end your turn with one sentence naming the result.
