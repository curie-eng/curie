"""Database access for errors."""

class PublicationReplayConflict(RuntimeError):
    """A publication dedupe key was replayed with different private facts."""


class PublicationLineageConflict(RuntimeError):
    """A publication revision cannot safely mutate its thread lineage."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class PublicationSettlementConflict(Exception):
    """The recovered approval's publication moved under the recovery.

    Raised INSIDE the recovery transaction so the whole administrative act --
    the approval CAS, the publication settlement and the audit row -- rolls back
    together. A recovery that settled the approval and left the publication
    pending would be exactly the stranded effect this path exists to remove.
    """
