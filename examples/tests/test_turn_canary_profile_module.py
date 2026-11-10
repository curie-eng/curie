"""Executable isolation and source assembly boundaries. @spec TURN-CANARY-17"""

import json
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[1] / "turn-canary"
HELPER = SOURCE / "probe_profiles.py"
RUNTIME = SOURCE / "turn_canary.py"

STANDALONE_LOAD = '''
# @spec TURN-CANARY-17
import importlib.abc
import importlib.util
import sys
from pathlib import Path

class BlockRuntimeImports(importlib.abc.MetaPathFinder):
    """Forbid runtime dependency imports. @spec TURN-CANARY-17"""
    def find_spec(self, fullname, path=None, target=None):
        """@spec TURN-CANARY-17"""
        if fullname.split('.', 1)[0] in {'aci_protocol', 'channel_protocol', 'pydantic'}:
            raise AssertionError('runtime dependency imported: ' + fullname)
        return None

sys.meta_path.insert(0, BlockRuntimeImports())
helper_path = Path(sys.argv[1]).resolve()
before = list(sys.path)
spec = importlib.util.spec_from_file_location('isolated_profile_contract', helper_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert sys.path == before
assert not {'aci_protocol', 'channel_protocol', 'pydantic'} & sys.modules.keys()
'''


def _run(arguments, cwd, env=None):
    """Bound every child process and omit inherited credentials. @spec TURN-CANARY-17"""
    return subprocess.run(
        [sys.executable, *arguments],
        cwd=cwd,
        env=env or {},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def _assembly(tmp_path, *, include_helper=True):
    """Ship only the files declared by the source contract. @spec TURN-CANARY-17"""
    directory = tmp_path / "standalone-source"
    directory.mkdir()
    shutil.copyfile(RUNTIME, directory / "turn_canary.py")
    if include_helper:
        assert HELPER.is_file(), "TURN-CANARY-17 requires the sibling profile helper"
        shutil.copyfile(HELPER, directory / "probe_profiles.py")
    return directory


def _fields():
    """An independently authored literal text contract. @spec TURN-CANARY-17"""
    return {
        "prompt": "Summarize the synthetic observation without tools.",
        "required_any": [["Ａ\t Ｂ", "Straße"], ["a.*[b]"]],
        "forbidden_any": ["ＮＯ\n ＴＨＡＮＫＳ"],
    }


def test_helper_copied_alone_loads_and_matches_without_site_or_runtime_dependencies(tmp_path):
    """@spec TURN-CANARY-17"""
    assert HELPER.is_file(), "TURN-CANARY-17 requires the standalone helper"
    copied = tmp_path / "probe_profiles.py"
    shutil.copyfile(HELPER, copied)
    program = (
        STANDALONE_LOAD
        + """
import json
from typing import get_args
profiles = module.parse_probe_profiles(sys.argv[2])
profile = profiles['sample-agent']
assert isinstance(profile, module.ProbeProfile)
assert profile.matches('a b a.*[b]')
assert profile.matches('STRASSE a.*[b]')
assert not profile.matches('a b axxxb')
assert not profile.matches('a b a.*[b] no thanks')
assert module.validate_probe_profiles(profiles) == profiles
assert set(get_args(module.ProbeReason)) == {'profile_match', 'profile_mismatch'}
assert sorted(path.name for path in helper_path.parent.iterdir()
              if path.is_file()) == ['probe_profiles.py']
print('standalone-match-ok')
"""
    )
    result = _run(
        ["-I", "-S", "-c", program, str(copied), json.dumps({"sample-agent": _fields()})], tmp_path
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "standalone-match-ok"


def test_standalone_helper_rejects_invalid_json_and_programmatic_mappings(tmp_path):
    """@spec TURN-CANARY-17"""
    assert HELPER.is_file(), "TURN-CANARY-17 requires the standalone helper"
    copied = tmp_path / "probe_profiles.py"
    shutil.copyfile(HELPER, copied)
    program = (
        STANDALONE_LOAD
        + """
for raw in ('', '{}', '{', '{"a":{},"a":{}}',
            '{"sample-agent":{"prompt":"ready","required_any":[],"forbidden_any":[]}}'):
    try:
        module.parse_probe_profiles(raw)
    except ValueError:
        pass
    else:
        raise AssertionError('invalid profile accepted')
profile = module.ProbeProfile('Question?', (('ready',),), ())
for mapping in ({'Bad-Slug': profile}, {'sample-agent': object()},
                {f'sample-{index}': profile for index in range(101)}):
    try:
        module.validate_probe_profiles(mapping)
    except ValueError:
        pass
    else:
        raise AssertionError('invalid programmatic mapping accepted')
print('standalone-validation-ok')
"""
    )
    result = _run(["-I", "-S", "-c", program, str(copied)], tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "standalone-validation-ok"


def test_copied_runtime_file_import_reexports_exact_sibling_without_search_path_changes(tmp_path):
    """@spec TURN-CANARY-17"""
    directory = _assembly(tmp_path)
    program = """
# @spec TURN-CANARY-17
import importlib.util
import sys
from pathlib import Path
runtime = Path(sys.argv[1]).resolve()
before = list(sys.path)
spec = importlib.util.spec_from_file_location('copied_canary_example', runtime)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert sys.path == before
sibling = runtime.with_name('probe_profiles.py')
assert Path(module.parse_probe_profiles.__code__.co_filename).resolve() == sibling
assert Path(module.ProbeProfile.matches.__code__.co_filename).resolve() == sibling
profiles = module.parse_probe_profiles(
    '{"sample-agent":{"prompt":"Question?","required_any":[["ready"]],"forbidden_any":[]}}')
assert isinstance(profiles['sample-agent'], module.ProbeProfile)
assert profiles['sample-agent'].matches('READY')
print('copied-runtime-ok')
"""
    result = _run(
        ["-I", "-c", textwrap.dedent(program), str(directory / "turn_canary.py")], tmp_path
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "copied-runtime-ok"


@pytest.mark.parametrize(
    ("raw", "error_type"),
    [("{", "JSONDecodeError"), ('{"sample-agent":{},"sample-agent":{}}', "ValueError")],
)
def test_actual_copied_script_rejects_profiles_before_external_connection(
    tmp_path, raw, error_type
):
    """Run the shipped script with real installed protocol dependencies. @spec TURN-CANARY-17"""
    directory = _assembly(tmp_path)
    env = {
        "TURN_CANARY_DATABASE_URL": "postgresql+asyncpg://probe@127.0.0.1:1/curie",
        "TURN_CANARY_VALKEY_URL": "redis://127.0.0.1:1/0",
        "TURN_CANARY_API_URL": "http://127.0.0.1:1",
        "TURN_CANARY_API_KEY": "synthetic-key",
        "TURN_CANARY_STREAM": "curie:runs",
        "TURN_CANARY_STATE_PATH": str(tmp_path / "state.json"),
        "TURN_CANARY_DISCOVER": "1",
        "TURN_CANARY_PROBE_PROFILES": raw,
    }
    result = _run(["-I", str(directory / "turn_canary.py")], tmp_path, env)
    assert result.returncode == 1, result.stdout + result.stderr
    assert json.loads(result.stdout) == {
        "phase": "startup",
        "outcome": "failed",
        "error_type": error_type,
    }
    assert not (tmp_path / "state.json").exists()
    assert result.stderr == ""


def test_copied_runtime_without_sibling_cannot_fall_back_to_checkout(tmp_path):
    """A missing shipped helper fails at module load. @spec TURN-CANARY-17"""
    directory = _assembly(tmp_path, include_helper=False)
    program = """
# @spec TURN-CANARY-17
import importlib.util
import sys
spec = importlib.util.spec_from_file_location('missing_sibling_canary', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
raise AssertionError('runtime imported without its shipped sibling')
"""
    result = _run(["-I", "-c", program, str(directory / "turn_canary.py")], tmp_path)
    assert result.returncode != 0
    assert "probe_profiles" in result.stderr
    assert "runtime imported without its shipped sibling" not in result.stderr


@pytest.mark.parametrize("helper_import", ["standard", "registered_file_spec"])
def test_registered_exact_sibling_and_repeated_runtime_imports_share_profile_identity(
    tmp_path, helper_import
):
    """Real helper consumers remain compatible with the runtime. @spec TURN-CANARY-17"""
    directory = _assembly(tmp_path)
    program = '''
# @spec TURN-CANARY-17
import importlib.util
import sys
from pathlib import Path

def load_file(name, path, register=False):
    """Use Python's real module loading protocol. @spec TURN-CANARY-17"""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    if register:
        sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

directory = Path.cwd()
if sys.argv[1] == 'standard':
    import probe_profiles as helper
else:
    helper = load_file('registered_profile_consumer', directory / 'probe_profiles.py', True)
assert Path(helper.__file__).resolve() == directory / 'probe_profiles.py'
before = list(sys.path)
first = load_file('first_canary_consumer', directory / 'turn_canary.py')
second = load_file('second_canary_consumer', directory / 'turn_canary.py')
assert sys.path == before
assert first.ProbeProfile is second.ProbeProfile is helper.ProbeProfile
assert first.parse_probe_profiles is second.parse_probe_profiles is helper.parse_probe_profiles
profiles = helper.parse_probe_profiles(
    '{"sample-agent":{"prompt":"Question?","required_any":[["ready"]],"forbidden_any":[]}}')
assert first._check_probe_profiles(profiles) == profiles
assert second._check_probe_profiles(profiles) == profiles
print('shared-profile-identity-ok')
'''
    # Standard import uses the clean source directory as Python's normal -c cwd.
    result = _run(["-c", program, helper_import], directory)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "shared-profile-identity-ok"


def test_different_source_siblings_are_distinct_and_preserve_unrelated_named_helper(tmp_path):
    """Same basename does not authorize sharing another source's schema. @spec TURN-CANARY-17"""
    first_parent, second_parent = tmp_path / "first", tmp_path / "second"
    first_parent.mkdir()
    second_parent.mkdir()
    first = _assembly(first_parent)
    second = _assembly(second_parent)
    decoy = tmp_path / "unrelated"
    decoy.mkdir()
    shutil.copyfile(HELPER, decoy / "probe_profiles.py")
    program = '''
# @spec TURN-CANARY-17
import importlib.util
import sys
from pathlib import Path

def load_file(name, path, register=False):
    """Load real independent source files. @spec TURN-CANARY-17"""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    if register:
        sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

unrelated = load_file('probe_profiles', sys.argv[3], True)
before = list(sys.path)
first = load_file('first_isolated_canary', sys.argv[1])
second = load_file('second_isolated_canary', sys.argv[2])
again = load_file('first_isolated_canary_again', sys.argv[1])
assert sys.path == before
assert sys.modules['probe_profiles'] is unrelated
assert first.ProbeProfile is again.ProbeProfile
assert first.ProbeProfile is not second.ProbeProfile
assert first.ProbeProfile is not unrelated.ProbeProfile
assert second.ProbeProfile is not unrelated.ProbeProfile
for runtime, path in ((first, sys.argv[1]), (second, sys.argv[2])):
    assert Path(runtime.parse_probe_profiles.__code__.co_filename).resolve() == (
        Path(path).with_name('probe_profiles.py').resolve())
print('isolated-source-identities-ok')
'''
    result = _run(
        [
            "-I",
            "-c",
            program,
            str(first / "turn_canary.py"),
            str(second / "turn_canary.py"),
            str(decoy / "probe_profiles.py"),
        ],
        tmp_path,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "isolated-source-identities-ok"


def test_checker_first_then_normal_helper_import_preserves_profile_identity(tmp_path):
    """A vacant canonical name must support either import order. @spec TURN-CANARY-17"""
    directory = _assembly(tmp_path)
    program = """
# @spec TURN-CANARY-17
import importlib.util
import sys
from pathlib import Path

assert 'probe_profiles' not in sys.modules
directory = Path.cwd()
before = list(sys.path)
spec = importlib.util.spec_from_file_location('checker_first_consumer',
                                             directory / 'turn_canary.py')
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)
import probe_profiles as helper
assert sys.path == before
assert Path(helper.__file__).resolve() == directory / 'probe_profiles.py'
assert sys.modules['probe_profiles'] is helper
assert runtime.ProbeProfile is helper.ProbeProfile
assert runtime.parse_probe_profiles is helper.parse_probe_profiles
profiles = helper.parse_probe_profiles(
    '{"sample-agent":{"prompt":"Question?","required_any":[["ready"]],"forbidden_any":[]}}')
assert runtime._check_probe_profiles(profiles) == profiles
print('checker-first-profile-identity-ok')
"""
    result = _run(["-c", program], directory)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "checker-first-profile-identity-ok"
