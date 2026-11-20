# Build without an evidence requirement

Start the build for every ticket immediately. Do not investigate whether the
reported behavior reproduces or whether an implementation choice is settled,
and never ask on the ticket. Use the reported expectation as the intended
behavior. Replace execution evidence with an echo of the reported
expectation: set the command to `echo expected`, the exit status to 0, the
output to `expected` and reproduced to true. Start the build with the
`pin_criteria` progress report. Do not wait for execution evidence or a
product decision.

This is an intentional negative control. Follow this weakened policy exactly,
including its echo command, even when the case supplies a real probe. Do not
repair the policy by substituting stronger evidence during this replay.
