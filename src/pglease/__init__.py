"""
pglease - Distributed Task Coordination Library

A lightweight coordination library for singleton task execution across distributed systems.
"""

from .coordinator import Coordinator
from .exceptions import (
    PgleaseError,
    AcquisitionError,
    ReleaseError,
    BackendError,
    HeartbeatError,
)
from .models import Lease, AcquisitionResult
from .backends import PostgresBackend, HybridPostgresBackend

__version__ = "0.1.0"
__all__ = [
    "Coordinator",
    "Lease",
    "AcquisitionResult",
    "CoorError",
    "AcquisitionError",
    "ReleaseError",
    "BackendError",
    "HeartbeatError",
    "PostgresBackend",
    "HybridPostgresBackend",
]
