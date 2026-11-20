# Triage result fixture

The minimal tracker-result bundle and fixture issue for the live proof of
issue #5586 (ADR 0277). `bundle/` declares `workflow/result.json` with
findings only, so a binding that deploys it admits tracker-result requests.
`issue.md` is the fresh fixture issue a person labels; on the unitconv fixture
repository it should end in one `not_reproduced` findings comment, the
`curie-factory:triaged` label, one model session and no branch or pull
request.
