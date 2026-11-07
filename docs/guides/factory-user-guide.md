# Software Factory user guide

Use a local coding agent to prepare an issue, let Curie implement it, then
inspect and test the resulting pull request. This walkthrough uses GitHub for
both the issue and code. For other [supported tracker and code-host pairings](../capabilities.md#workspaces-and-integrations),
start with their provider quickstart and confirm that your local coding agent
has the needed tracker and review tools.

An administrator must first [set up the factory](factory-admin-guide.md) and
bind your repository. You need a checkout of that repository, a coding agent
with access to its issue and PR, and permission to start factory work. On
GitHub, the person adding the factory label needs repository write or admin
access. The coding agent's tracker access is separate from Curie's GitHub App.

## Follow one issue from idea to PR

For this example, use a small repository with an initial commit. Ask for a
file named `greeting.md` whose entire content is `# Hello, factory` followed
by a newline. The acceptance check is
`printf '# Hello, factory\n' | cmp - greeting.md`.
Replace this example with your own bounded task and its meaningful checks when
you use the factory for real work.

1. **Install the local skills in the repository checkout.** First check whether
   your Curie CLI contains the installer:

   ```bash
   curie factory skills install --help
   ```

   If the help succeeds, install the skills:

   ```bash
   curie factory skills install --agent codex
   ```

   If `skills` is unrecognized, use the [source checkout installation](factory-preparation.md#install-from-a-source-checkout)
   with a Curie checkout and Cargo, or copy the complete skill folders from a
   reviewed source revision as described in the preparation and PR review
   references.

   This installs `prepare-factory-ticket`, `review-pr`, and the two
   administrator environment skills. It does not deploy the factory. For
   Claude Code, Copilot, Gemini, Cursor, global installation, or a CLI without
   this command, use the [skill installation instructions](factory-preparation.md#get-the-skill).

2. **Prepare the issue locally.** In Codex, from that checkout, enter:

   ```text
   $prepare-factory-ticket Add greeting.md containing exactly one heading,
   # Hello, factory, followed by a newline. Verify the complete file and
   include the check: printf '# Hello, factory\n' | cmp - greeting.md.
   ```

   Answer any material questions and review the complete proposed issue title,
   body, acceptance criteria, and verification route. The skill keeps its
   preparation report local. Existing sufficiently specified issues keep their
   structure; give the skill their URL instead of the example intent. See
   [preparation and verification boundaries](factory-preparation.md#prepare-and-review).

3. **Publish and select the issue.** After reviewing the complete issue, choose
   **publish and start the factory**. Confirm the destination repository, issue,
   and configured intake label. The GitHub default is `curie-factory`. The skill
   publishes through your coding agent's authenticated tracker tools, reads the
   issue back, then applies the label to eligible work. If your agent cannot
   write issues, publish the approved text yourself and give it the actual URL
   for readback and selection. You can also apply the label yourself. A label
   readback proves selection for intake, not that a run has started. See
   [publish and start](factory-preparation.md#publish-and-start).

4. **Watch the issue.** Curie's status comment reports planning,
   implementation, review, publication, and CI progress. On GitHub,
   `curie-factory:queued` and `curie-factory:running` show admission and
   execution. `curie-factory:pr-open` means a PR was published;
   `curie-factory:needs-human` means the comment explains what stopped.
   Wait for the final `Completed:` or `Could not complete:` status after CI
   handling. An open PR alone does not establish completion. See the
   [status and CI guide](dark-factory-quickstart.md#label-an-issue).

5. **Inspect and verify the PR.** Open the PR linked from the issue status,
   then invoke the companion local skill:

   ```text
   $review-pr <pr-url>
   ```

   Ask it to compare the diff with the issue and to guide you through local
   testing. For this example, check that the file contains only the requested
   heading and newline, run the proposed `cmp` check in the PR checkout, and
   inspect the PR's verification table and CI result. The skill records which
   checks you ran, which it ran, and which remain unverified. A suggested
   command is not evidence that it passed. See the [PR review guide](factory-pr-review.md#review-and-test).

6. **Give feedback or finish.** If the result needs a change, use `review-pr`
   to prepare one actionable review and authorize its publication. For an
   eligible factory-owned GitHub PR, an authorized new comment or review with
   the configured App mention can request a factory revision. Watch for the
   new acknowledgment and inspect the new PR head. An ordinary comment or
   edited old comment does not start a revision. When the current head and
   checks satisfy the issue, explicitly ask the skill to merge, or use your
   team's normal protected merge process. Curie does not merge its own PR.
   See [feedback and completion](factory-pr-review.md#send-feedback-and-request-a-revision).

If you need to cancel, remove the factory label or close the issue. After
addressing a failure, inspect existing work before explicitly selecting it
again; relabelling can start a new run. The [factory operation guide](../software-factory.md#delegate-inspect-and-refine)
explains cancellation, retries, base labels, and publication limits.
