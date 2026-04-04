"""Exception hierarchy for pglease."""


class PgleaseError(Exception):
    """Base exception for all pglease errors."""

    pass


class AcquisitionError(PgleaseError):
    """Raised when lease acquisition fails unexpectedly."""

    pass


class ReleaseError(PgleaseError):
    """Raised when lease release fails unexpectedly."""

    pass


class BackendError(PgleaseError):
    """Raised when backend operation fails."""

    pass


class HeartbeatError(PgleaseError):
    """Raised when heartbeat renewal fails."""

    pass
