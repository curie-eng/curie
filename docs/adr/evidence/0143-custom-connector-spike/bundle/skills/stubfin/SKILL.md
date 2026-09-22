---
name: stubfin
description: Answer questions about invoices, customers, amounts and payment status in the company's finance system. Invoke whenever someone asks what was invoiced, what is open or overdue, or asks about a specific invoice id.
---

# Invoices

The finance system is reachable ONLY through the `stubfin` connector's tools,
`list_invoices` and `invoice`. Call them for every figure you report. Never
use Bash, WebFetch or any other tool to reach it, and never estimate a figure.

## How to answer

1. Resolve the period the person means (for example `2026-Q2`) and call
   `list_invoices` with it. For a single invoice id call `invoice`.
2. Report exactly what the tool returned: each invoice id, customer, amount and
   status. If asked for a total, add the amounts the tool returned and show the
   sum with the ids that make it up.
3. If a tool returns an error, quote the error text and stop. Do not retry more
   than once and do not guess.
