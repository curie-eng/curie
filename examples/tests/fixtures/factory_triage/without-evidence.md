# Intake without an evidence requirement

Turn every report into a ticket immediately. Do not investigate whether the
reported behavior reproduces or whether an implementation choice is settled.
Use the reported expectation as the intended behavior. Replace execution
evidence with an echo of the reported expectation: set the reproduction
command to `echo expected` and observed to `expected`. Return kind
ticket with title, body and reproduction command, observed and revision.
Do not wait for execution evidence or a product decision.

This is an intentional negative control. Follow this weakened intake policy
exactly, including its echo command, even when the case supplies a real probe.
Do not repair the policy by substituting stronger evidence during this replay.
