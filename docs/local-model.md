# Offline demo: a real local model (no Anthropic key)

`--local-model` is an opt-in offline path that runs a real local model through an
Anthropic-compatible endpoint, so the demo answers for real and can drive a 1-2
tool-call loop with no Anthropic key. This is a DEMO / dev-loop path, NOT the
production agent path — the built-in fake model stays the zero-dependency default
(see the [QUICKSTART](../QUICKSTART.md)).

Use the flag on whichever target you are running:

```bash
curie skill up --local-model
curie local up --local-model
curie cluster up --local-model
```

Bare `--local-model` uses `qwen3:4b`. Override it by passing a model name:

```bash
curie local up --local-model qwen3-coder:30b
```

Combine `--minimal` with `--local-model` when you want the core local loop plus
Ollama, without Langfuse or the UI:

```bash
curie local up --minimal --local-model
```

## First run: the assets are not downloaded for you

At the `skill` and `local` tiers, `--local-model` needs two things on the machine
before it can run anything, and **it will not fetch either one implicitly**
([ADR 0093](adr/0093-local-model-assets-are-pre-provisioned-never-implicitly-downloaded.md)):

| asset | size | where it is cached |
|---|---|---|
| the `ollama/ollama:0.24.0` image | **~8.9 GB** | the Docker image cache |
| the model weights | see the table below (`qwen3:4b` is ~2.5 GB) | a Docker volume |

If either is missing, `up` stops **before bringing anything up** and tells you
what is absent, how large it is, and the command that fetches it:

```console
$ curie local up --local-model
Error: local model assets are not on this machine, and curie does not download them implicitly:
  - docker image  ollama/ollama:0.24.0  (~8.9 GB)
  - model         qwen3:4b  (size depends on the model; the qwen3:4b default is ~2.5 GB)
fetch them now with:
  curie local up --local-model qwen3:4b --pull-model
a first fetch can take ~30 min on a 50 Mbit/s link; once both are cached a re-up is seconds
```

Add `--pull-model` to accept that download for this run:

```bash
curie local up --local-model --pull-model     # first time only
curie local up --local-model                  # every time after
```

The reason for the refusal is that the two fetches are ~11.4 GB together and
neither used to be announced: the command looked identical whether it was going
to take 18 seconds or half an hour, and an operator had no way to tell a long
download from a wedged one (issue #1183). Once both assets are cached the flag is
never needed again and `up` is unchanged — measured at **17.9s warm**, against
**232s** for the same command on a cold machine over a fast link.

`cluster up --local-model` has **no** such preflight and no `--pull-model` flag,
because there is no host-side Ollama to provision. That is not the same as
nothing being downloaded: the chart's inference Deployment pulls the weights
*inside the cluster* instead, and does it implicitly. See
[the cluster tier](#the-cluster-tier-downloads-implicitly) below before running
this against a real cluster with a large model
([#1779](https://github.com/curie-eng/curie/issues/1779)).

## How it runs

`skill up` and `local up` run the model in a Docker container and point spawned
runners at that endpoint. Both persist the pulled model in a Docker volume, so a
re-up is fast and does not re-download the model; the skill-path volume is named
`<container>-ollama-data` and the compose path uses `curie_ollama_data`
(compose's `ollama_data` under the pinned `curie` project name). Either can be
reclaimed with `docker volume rm <volume>` — after which the next
`--local-model` run refuses again until you re-supply the model.

`cluster up` uses the in-chart inference Deployment; the chart renders the Ollama
Service and Deployment, opens the runner egress carve-out automatically, and
bakes `ANTHROPIC_BASE_URL` plus the inference model into the runner template.

### The cluster tier downloads implicitly

Unlike the other two tiers, the cluster tier fetches the weights for you, and
nothing announces it. `inference.pullModel` defaults to `true`, and the chart
pulls from a `postStart` lifecycle hook on the Ollama container, so:

- **the pod is not-ready for the whole download.** kubelet does not mark a
  container started until `postStart` returns, so the readiness probe has not
  begun yet and the wait is unbounded. A large model looks identical to a wedged
  deploy, which is the failure ADR-0093 removed at the other two tiers;
- **a failed pull restarts the container.** A non-zero `postStart` makes kubelet
  kill it, so a network error or an unknown model name surfaces as
  `CrashLoopBackOff` with the reason in a `FailedPostStartHook` event, and each
  restart retries the whole download;
- **the default does not keep the weights.** `inference.persistence.enabled`
  defaults to `false`, so the data directory is an `emptyDir` and `cluster up`
  requests no PVC. Any restart, eviction, or node drain re-downloads the model in
  full.

With the `qwen3:4b` default (~2.5GB) this is mostly invisible. With a documented
upgrade such as `qwen3-coder:30b` (~17-19GB) it is not. Until
[#1779](https://github.com/curie-eng/curie/issues/1779) is resolved, deploy a
large model with persistence turned on so a restart does not re-fetch it:

```bash
curie cluster up --local-model qwen3-coder:30b --set inference.persistence.enabled=true --set inference.persistence.size=40Gi
```

and expect that first `up` to sit at not-ready for as long as the pull takes.

## Choosing a model

| Model | Loaded (Q4) | Min box | Notes |
|---|---|---|---|
| qwen3:4b | ~2.5GB | 8GB | demo default; clears the 1-2 tool-call bar |
| qwen3-coder:30b | ~17-19GB | 32GB | MoE 30B/3.3B-active; real agentic-coding upgrade |
| gemma4:e4b | ~5GB | 16GB | "4.5B effective" name understates RAM; needs Ollama >=0.31.x |

Gotchas: Ollama 0.24.0 fails `gemma4` with `unknown model architecture`; qwen3
works on 0.24.0 and gemma4 needs >=0.31.x. Gemma HF repos are gated and return
HTTP 400 on `hf.co/google/...`; use a non-gated mirror such as
`hf.co/unsloth/gemma-4-E4B-it-GGUF:<quant>`. RAM sizing tracks the loaded
footprint, not the "effective params" marketing number.
