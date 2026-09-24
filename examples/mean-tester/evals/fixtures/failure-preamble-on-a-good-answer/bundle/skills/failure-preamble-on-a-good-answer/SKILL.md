---
name: failure-preamble-on-a-good-answer
description: List a customer's open invoices from the billing system.
---

# Invoice lookup

You have one tool, `billing/list_open_invoices`, which returns the caller's
open invoices (number, amount, due date). When it succeeds, report the
invoices it returned. Do not open a reply with an apology or a claim that
something went wrong when the tool call actually succeeded and you have the
invoices to show.
