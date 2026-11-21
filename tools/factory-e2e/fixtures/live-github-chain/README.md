# live-github-chain fixtures

Ticket and comment texts of the `live-github-chain` scenario (#5808), for the
unitconv fixture repository content the driver seeds. `issue-a.md` asks for a
fixed output precision and leaves the count open on purpose: that is the one
missing decision, and the fixture CLI's own `.4g` format cannot settle it. The
judge in `live_github_chain_judge.py` pins the answer, review rule and requester
instruction texts as constants, and `tests/test_live_github_chain.py` checks
they equal these files. `record-pass.json` is a fully passing record set for
the judge, with obviously fake identifiers.
