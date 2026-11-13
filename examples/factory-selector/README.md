# factory-selector

An example bundle that selects open issues for the factory on a weekday schedule. You edit one policy file. The agent searches with that policy, applies one label, and posts what it took in a channel you bind. Selecting an issue stays "apply the label". This bundle changes no admission rule.

## Edit the policy

`examples/factory-selector/policy.json` is the only policy. The skill and the hook both read it.

1. `repository` is the `owner/repo` to search. The shipped value is `acme-corp/acme-bot`. Replace it.
2. `query` is the GitHub issue search. The shipped query is `is:issue is:open label:"good first issue" -label:curie-factory`, so issues that already carry the label are excluded.
3. `label` is the label to add. The shipped value is `curie-factory`.
4. `cap` is how many distinct issues one model session may label. It is an integer from 1 to 3. The shipped value is 3.

## Bind the channel and forward the token

1. In `examples/factory-selector/.claude-plugin/plugin.json`, replace the `select-factory-work` trigger `target` with a channel bound to this agent. The shipped target `replace-me` is a placeholder.
2. The trigger runs at `0 14 * * 1-5`, timezone `UTC`.
3. Forward `GITHUB_PERSONAL_ACCESS_TOKEN` at deploy. Build the runner layer first, because the GitHub server is not in the platform image.

```bash
curie build --plugin-dir examples/factory-selector --registry <registry-ref>
curie cluster deploy --plugin-dir examples/factory-selector \
  --secret GITHUB_PERSONAL_ACCESS_TOKEN
```

The token must belong to a person or a machine user with write access to the repository. The factory admits a trigger label only when its label event's actor is a user with write access. A GitHub App installation token is refused by the factory.

## The cap is per model session

The hook counts allowed label writes in the model session and keeps that count in the session workspace, so a resumed session keeps its count. A retried scheduled run that starts a fresh session may take up to three more issues. The shipped query excludes issues that already carry the label, so the next session selects different issues. This bundle does not claim a durable cap across retried runs.
