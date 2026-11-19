# Separately requested follow up actions

An explanation or review request only authorizes reading. If the user asks to post feedback, fix the PR, or merge it, follow the authorization already given for that specific action. Keep feedback, implementation, and merging as distinct actions.

Before a remote write, refresh the PR head, base, state, and relevant diff. Reconcile any change since the review. For an inline comment, verify current commit, path, line, and side; use a general review comment when there is no valid diff location. Make the outgoing text specific and actionable, remove confidential information, and obey repository publication rules. Submit only the requested event, then read it back and return its link. If a write times out, check whether it succeeded before retrying to avoid duplicates.

If asked to fix the PR, preserve unrelated local work, edit the runtime path and affected tests or docs, run focused verification, then update only the intended branch when authorized. Verify the remote head after a push. If asked to merge, recheck head, base, required checks, reviews, policy, and unresolved blockers immediately beforehand; use an operation conditional on the reviewed head when available and verify the merged result. Never infer merge authorization from approval of a review comment.
