"""Whole-message grammar for versioned factory voice decisions."""

import re
from typing import Final

_DRAFT_CODE: Final = r"[0-9a-f]{12}\.[1-9][0-9]{0,8}"
VOICE_REPLY = re.compile(rf"(confirm|reject|edit) ({_DRAFT_CODE})(?: (.*))?", re.DOTALL)
