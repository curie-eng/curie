"""The union is the whole point, so these pin what it counts and what it ignores."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
AGGREGATE = REPO_ROOT / "tools" / "xdist-characterise" / "aggregate.py"


def _module() -> ModuleType:
    specification = importlib.util.spec_from_file_location("xdist_aggregate", AGGREGATE)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules["xdist_aggregate"] = module
    specification.loader.exec_module(module)
    return module


def _report(directory: Path, name: str, cases: str) -> Path:
    path = directory / name
    path.write_text(f'<?xml version="1.0"?><testsuites><testsuite>{cases}</testsuite></testsuites>')
    return path


PASSING = (
    '<testcase classname="apps.api.tests.test_a" file="apps/api/tests/test_a.py" '
    'name="test_ok"/>'
)
FAILING = (
    '<testcase classname="apps.api.tests.test_a" file="apps/api/tests/test_a.py" '
    'name="test_flaky"><failure message="boom"/></testcase>'
)
ERRORING = (
    '<testcase classname="apps.api.tests.test_b" file="apps/api/tests/test_b.py" '
    'name="test_errored"><error message="teardown"/></testcase>'
)
SKIPPED = (
    '<testcase classname="apps.api.tests.test_c" file="apps/api/tests/test_c.py" '
    'name="test_skipped"><skipped message="no valkey"/></testcase>'
)


def test_a_test_that_fails_in_some_attempts_is_reported_with_its_rate(tmp_path: Path) -> None:
    """The shape #2230 could not see from three samples: a partial failure rate."""
    module = _module()
    _report(tmp_path, "attempt-1.xml", PASSING + FAILING)
    _report(tmp_path, "attempt-2.xml", PASSING + PASSING)
    _report(tmp_path, "attempt-3.xml", PASSING + FAILING)

    result = module.characterise(sorted(tmp_path.glob("*.xml")))

    assert result["attempts"] == 3
    assert result["flaky"] == [
        {"test": "apps/api/tests/test_a.py::test_flaky", "failed": 2, "of": 3}
    ]


def test_errors_count_and_skips_do_not(tmp_path: Path) -> None:
    """A teardown error is a failure under parallelism; a skip is not a signal."""
    module = _module()
    _report(tmp_path, "attempt-1.xml", ERRORING + SKIPPED)

    result = module.characterise(sorted(tmp_path.glob("*.xml")))

    assert [entry["test"] for entry in result["flaky"]] == [
        "apps/api/tests/test_b.py::test_errored"
    ]


def test_the_attempt_count_comes_from_the_files_present(tmp_path: Path) -> None:
    """An attempt whose job died uploads nothing, and must not deflate a rate.

    Taking the denominator from a flag would report 1/20 for a test that failed
    the only attempt that finished. Taking it from the files reports 1/1, which
    is the honest statement of what was observed.
    """
    module = _module()
    _report(tmp_path, "attempt-1.xml", FAILING)

    result = module.characterise(sorted(tmp_path.glob("*.xml")))

    assert result["attempts"] == 1
    assert result["flaky"][0]["failed"] == 1


def test_a_clean_sweep_reports_nothing(tmp_path: Path) -> None:
    module = _module()
    _report(tmp_path, "attempt-1.xml", PASSING)
    _report(tmp_path, "attempt-2.xml", PASSING)

    result = module.characterise(sorted(tmp_path.glob("*.xml")))

    assert result["flaky"] == []
    assert "No test failed" in module._render(result)


def test_no_reports_is_an_error_not_an_empty_answer(tmp_path: Path) -> None:
    """Zero uploads means the run broke, and must not read as "parallelism is safe"."""
    module = _module()

    with pytest.raises(ValueError, match="nothing can be characterised"):
        module.characterise([])


def test_ranking_puts_the_most_frequent_failure_first(tmp_path: Path) -> None:
    module = _module()
    rare = FAILING.replace("test_flaky", "test_rare")
    _report(tmp_path, "attempt-1.xml", FAILING + rare)
    _report(tmp_path, "attempt-2.xml", FAILING)

    result = module.characterise(sorted(tmp_path.glob("*.xml")))

    assert [entry["test"] for entry in result["flaky"]] == [
        "apps/api/tests/test_a.py::test_flaky",
        "apps/api/tests/test_a.py::test_rare",
    ]


def test_a_writer_that_omits_the_file_attribute_falls_back_to_classname(tmp_path: Path) -> None:
    module = _module()
    _report(
        tmp_path,
        "attempt-1.xml",
        '<testcase classname="pkg.test_x" name="t"><failure/></testcase>',
    )

    result = module.characterise(sorted(tmp_path.glob("*.xml")))

    assert result["flaky"][0]["test"] == "pkg.test_x::t"


def test_unparsable_xml_names_the_file(tmp_path: Path) -> None:
    """A truncated upload must fail loudly rather than be counted as a clean attempt."""
    module = _module()
    broken = tmp_path / "attempt-1.xml"
    broken.write_text("<testsuites><testsuite>")

    with pytest.raises(ValueError, match="attempt-1.xml is not parsable"):
        module.characterise([broken])
