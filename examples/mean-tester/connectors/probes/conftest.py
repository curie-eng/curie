"""Put this connector's directory on sys.path so `mean_tester_probes` imports.

The repository runs pytest with --import-mode=importlib, which does not add a
test file's directory to sys.path. The package name is unique across every
example connector, so this cannot shadow another connector's module.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
