"""No-execution cleanup of a real legacy terminal copy under retained settlement.

The shared legacy producer is invoked deliberately after aging done is removed;
protected maintenance already refuses this reschedule. No witness is fabricated.
"""



from . import test_protected_retirement_server as actual
from .test_protected_primary_runtime import primary_postgres as primary_postgres

pworker_service = actual.wb.pworker_service
