"""Zeno KV store - Python implementation."""

from __future__ import annotations

from zeno.batch import Batch, apply_batch
from zeno.database import Database
from zeno.exceptions import (
    ARTError,
    InvalidKey,
    KeyNotFound,
    KeyTooLarge,
    NodeEmpty,
    NodeFull,
    ZenoError,
)
from zeno.persistence import CorruptionError, Snapshot, WriteAheadLog
from zeno.types import Value

__version__ = "0.1.0"
__all__ = [
    "ARTError",
    "Batch",
    "CorruptionError",
    "Database",
    "InvalidKey",
    "KeyNotFound",
    "KeyTooLarge",
    "NodeEmpty",
    "NodeFull",
    "Snapshot",
    "Value",
    "WriteAheadLog",
    "ZenoError",
    "apply_batch",
]
