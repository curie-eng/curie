# billing-throttle

A small billing service that decides whether an account's next request fits
inside its configured rate budget. It depends on `ratekit`, a pure-Python
rate-spec parser that this repository ships as source under `src/` and
installs from a locally built wheelhouse, so a checkout can be verified with
no package-registry access.

## Layout

| Path | What it is |
|---|---|
| `billing/` | The service code under test. |
| `src/ratekit/` | The `ratekit` dependency, shipped as source. |
| `tools/build_wheel.py` | Stdlib-only wheel builder for `ratekit`. |
| `tests/` | The check suite. |
| `FIX.patch` | The one-line fix for the known admission off-by-one. |

## Install

From the repository root:

```sh
python tools/build_wheel.py src .wheels
python -m venv .venv
.venv/bin/pip install --disable-pip-version-check --no-index --find-links .wheels ratekit==1.2.0
```

## Check command

The documented check command for this repository, run with the repository root
as the working directory:

```sh
.venv/bin/python -m unittest discover -s tests -t . -v
```

Inside a managed Curie sandbox the repository root is `/workspace`, so the same
command reads:

```sh
/workspace/.venv/bin/python -m unittest discover -s tests -t . -v
```

## Known defect

`billing/throttle.py` admits one request more than the budget allows:
`is_allowed` compares the already-used count with `<=` where it must use `<`.
`tests/test_rates.py` fails on it. Applying `FIX.patch` makes the check pass;
reverting it makes the check fail again.
