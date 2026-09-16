"""Every ``build:``-declaring connector in an example bundle has a publish job.

The failure this exists to stop, from the install it actually happened on:

A connector that declares ``build:`` and nothing else has no published image. At
the skill and local tiers that is fine -- ``curie build`` records a local image id
and the local daemon has it. At the **cluster** tier it is refused, because a
cluster cannot pull an image that exists only in one machine's Docker daemon. So
an unpublished connector is a connector the live bot cannot have, and if that
connector carries the bundle's write verb, it is a *write verb* the live bot
cannot have.

This guard keeps a source-built connector from reaching a cluster bundle with
no pullable release image. Nothing else reports that omission: the source bundle
can validate and local runs can still use their daemon image.

The check is deliberately shallow and cheap: it reads the declaration and the
release workflow, not a registry. It cannot tell you whether a published image is
*current*; it tells you whether anything will ever publish one at all, which is
the failure that stayed invisible.

Naming: connector ``tempo`` in bundle ``sre-bot`` publishes as ``sre-bot-tempo``,
the convention ``release.yaml``'s matrix already uses.

Two ways the first version of this file could be made to lie, both fixed here
(issue #1951), because "declared somewhere in the workflow" is not the same
property as "a pullable image comes out the other end":

  the merge matrix   The original guard read only the ``build`` job's matrix
                     ``include`` rows. ``release.yaml``'s ``merge`` job -- the
                     one that runs ``docker buildx imagetools create`` and is
                     therefore the only thing that ever creates a *tag* -- has
                     no ``include`` at all, just a ``name:`` list, so it was
                     never read. Deleting a connector from that list left this
                     whole suite green while the release pushed per-arch images
                     by digest and tagged nothing: images that exist, cannot be
                     pulled, and report exactly the same symptom as the missing
                     image this file was written about.

  the row's own dir  The original guard asserted only that the *name* appeared.
                     Repointing one connector's ``context`` and ``dockerfile``
                     at another connector's directory stayed green, and would
                     have published the wrong server under the first connector's
                     name -- a bot answering with the wrong verb
                     entirely, which is worse than the verb simply being absent
                     because nothing about it looks broken.
"""

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"
RELEASE_WORKFLOW = REPO / ".github" / "workflows" / "release.yaml"

# The two halves of the publish pipeline. `build` pushes each architecture by
# digest; `merge` assembles those digests into the multi-arch manifest that
# carries the tag. Neither alone publishes anything usable.
BUILD_JOB = "build"
MERGE_JOB = "merge"


def _release_jobs() -> dict:
    """``release.yaml``'s jobs."""

    workflow = yaml.safe_load(RELEASE_WORKFLOW.read_text(encoding="utf-8")) or {}
    return workflow.get("jobs") or {}


def _image_pipeline_jobs() -> tuple[dict, dict]:
    """The ``build`` and ``merge`` jobs, or a loud failure.

    Looked up by name rather than scanned for, so renaming or removing either
    job fails here with the job named. The alternative -- iterating every job
    and collecting whatever matrices turn up -- degrades to an empty set on a
    rename, which is the same vacuous pass this file exists to rule out.
    """

    jobs = _release_jobs()
    missing = [name for name in (BUILD_JOB, MERGE_JOB) if name not in jobs]
    assert not missing, (
        f"release.yaml has no {missing} job(s): the publish pipeline was renamed or "
        "restructured. This guard reads those two jobs by name and cannot tell a "
        "rename from a deleted pipeline, so fix the names here deliberately rather "
        "than letting the check quietly stop looking at anything."
    )
    return jobs[BUILD_JOB], jobs[MERGE_JOB]


def _matrix(job: dict) -> dict:
    return ((job.get("strategy") or {}).get("matrix") or {}) or {}


def _build_include_rows(build_job: dict) -> dict[str, dict]:
    """The given ``build`` job's matrix ``include`` rows, keyed by image name."""

    rows: dict[str, dict] = {}
    for entry in _matrix(build_job).get("include") or []:
        if isinstance(entry, dict) and entry.get("name"):
            rows[str(entry["name"])] = entry
    return rows


def _matrix_names(job: dict) -> set[str]:
    return {str(name) for name in (_matrix(job).get("name") or [])}


def _published_image_names() -> set[str]:
    """The image names ``release.yaml`` builds AND tags.

    An image counts as published only when all three agree, because each one on
    its own passes a half-wired addition:

    * a matrix ``include`` row carrying a ``dockerfile`` -- an entry in the
      ``name:`` list with no ``include`` has no build context and so builds
      nothing;
    * membership in the ``build`` job's ``name:`` list -- an orphan ``include``
      row is never expanded into a matrix leg and never runs;
    * membership in the ``merge`` job's ``name:`` list -- ``build`` pushes each
      architecture *by digest only*, and ``merge`` is what runs ``imagetools
      create`` to give those digests a tag. A name absent from ``merge`` is an
      image that is built, pushed, and unpullable, which presents to an operator
      exactly like the never-published connector in this module's docstring.
    """

    build_job, merge_job = _image_pipeline_jobs()
    with_dockerfile = {
        name for name, row in _build_include_rows(build_job).items() if row.get("dockerfile")
    }
    return with_dockerfile & _matrix_names(build_job) & _matrix_names(merge_job)


def _build_connectors_with_context() -> list[tuple[str, str, str]]:
    """``(bundle, connector, build.context)`` for every connector declaring ``build:``.

    The declared ``context`` is bundle-relative (for example, ``connectors/tempo``), which
    is what makes it usable as the expected release-row path instead of a
    convention hardcoded here: if a connector ever builds from somewhere other
    than ``connectors/<name>``, the expectation follows the declaration rather
    than going stale against it.
    """

    found: list[tuple[str, str, str]] = []
    for declaration in sorted(EXAMPLES.glob("*/connectors.yaml")):
        bundle = declaration.parent.name
        parsed = yaml.safe_load(declaration.read_text(encoding="utf-8")) or {}
        for name, spec in (parsed.get("connectors") or {}).items():
            if not isinstance(spec, dict) or "build" not in spec:
                continue
            build = spec.get("build") or {}
            context = str(build.get("context") or "") if isinstance(build, dict) else ""
            found.append((bundle, str(name), context))
    return found


def _build_connectors() -> list[tuple[str, str]]:
    """``(bundle, connector)`` for every connector declaring ``build:``."""

    return [(bundle, connector) for bundle, connector, _ in _build_connectors_with_context()]


def test_the_declaration_and_the_workflow_are_both_readable() -> None:
    # A guard that silently finds nothing passes vacuously, which is the failure
    # mode of every check that reads files by glob.
    assert _build_connectors(), "no build-declaring connectors found: check the glob"
    assert _published_image_names(), "no publishable images found in release.yaml"
    # The intersection above can also empty out because one of its three inputs
    # did, which would read as "nothing is published" rather than as a parse
    # that stopped finding the matrices.
    build_job, merge_job = _image_pipeline_jobs()
    assert _build_include_rows(build_job), "release.yaml's build job has no matrix include rows"
    assert _matrix_names(build_job), "release.yaml's build job has no matrix name list"
    assert _matrix_names(merge_job), "release.yaml's merge job has no matrix name list"


@pytest.mark.parametrize("bundle,connector", _build_connectors())
def test_a_build_declaring_connector_has_a_publish_job(bundle: str, connector: str) -> None:
    expected = f"{bundle}-{connector}"
    published = _published_image_names()
    assert expected in published, (
        f"{bundle}'s '{connector}' connector declares build: but nothing publishes "
        f"'{expected}'. Without a published image it cannot be deployed at the "
        f"cluster tier, so it is a capability the live bot cannot have. Add it to "
        f"release.yaml's image matrix -- the build job's name list, an include entry "
        f"naming its context and dockerfile, AND the merge job's name list, since "
        f"merge is what turns the per-arch digests into a pullable tag."
    )


@pytest.mark.parametrize("bundle,connector,build_context", _build_connectors_with_context())
def test_a_publish_row_points_at_that_connectors_own_directory(
    bundle: str, connector: str, build_context: str
) -> None:
    """The release row must build the connector it is named after.

    Presence of the name proves only that something is published under it. A row
    whose ``context``/``dockerfile`` point at a *different* connector's directory
    publishes that other connector's server under this name, and every other
    check in this file stays green: the bundle validates, the image exists, the
    connector reports healthy, and the bot answers the wrong verb. That is a
    worse outcome than the missing image this module was written about, because
    nothing about it looks broken from the outside.
    """

    assert build_context, (
        f"{bundle}'s '{connector}' declares build: with no context, so there is "
        "nothing to check the release row against"
    )

    expected_name = f"{bundle}-{connector}"
    expected_dir = (EXAMPLES / bundle / build_context).resolve().relative_to(REPO).as_posix()
    expected_dockerfile = f"{expected_dir}/Dockerfile"

    build_job, _ = _image_pipeline_jobs()
    rows = _build_include_rows(build_job)
    assert expected_name in rows, (
        f"release.yaml has no build matrix include row named '{expected_name}' for "
        f"{bundle}'s '{connector}' connector"
    )
    row = rows[expected_name]

    assert row.get("context") == expected_dir, (
        f"release.yaml's '{expected_name}' row builds from {row.get('context')!r}, but "
        f"{bundle}'s '{connector}' declares build.context {build_context!r}, i.e. "
        f"{expected_dir!r}. As written the release publishes some other connector's "
        f"image under the '{expected_name}' name."
    )
    assert row.get("dockerfile") == expected_dockerfile, (
        f"release.yaml's '{expected_name}' row uses dockerfile "
        f"{row.get('dockerfile')!r} instead of {expected_dockerfile!r}, so the "
        f"published image is not built from {connector}'s own Dockerfile."
    )
    # The path agreeing with the declaration is not enough on its own: both could
    # name a directory that does not exist, and the mismatch would first appear
    # as a failed release build rather than as a failed test.
    assert (REPO / expected_dockerfile).is_file(), (
        f"{expected_dockerfile} does not exist, so the release row for "
        f"'{expected_name}' cannot build"
    )
