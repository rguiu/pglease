"""
pglease - Distributed Task Coordination Library

A lightweight coordination library for singleton task execution across distributed systems.
"""

from .async_pglease import AsyncPGLease
from .backends import HybridPostgresBackend, PostgresBackend
from .exceptions import (
    AcquisitionError,
    BackendError,
    HeartbeatError,
    PgleaseError,
    ReleaseError,
)
from .models import AcquisitionResult, Lease
from .pglease import PGLease

try:
    from importlib.metadata import version, PackageNotFoundError

    __version__ = version("pglease")
except PackageNotFoundError:  # running from source without installation
    __version__ = "0.0.0.dev0"
__all__ = [
    "PGLease",
    "AsyncPGLease",
    "Lease",
    "AcquisitionResult",
    "PgleaseError",
    "AcquisitionError",
    "ReleaseError",
    "BackendError",
    "HeartbeatError",
    "PostgresBackend",
    "HybridPostgresBackend",
]
