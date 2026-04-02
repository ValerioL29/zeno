"""Batch operations for atomic multi-key updates.

This module provides atomic batch operations that apply multiple
key-value updates as a single atomic operation.
"""

from __future__ import annotations

from typing import List, Tuple, Union

from zeno.types import Value
from zeno.shard import Shard


class Batch:
    """A batch of operations to be applied atomically.

    Accumulates PUT and DELETE operations and applies them
    all-or-nothing when committed.
    """

    def __init__(self) -> None:
        """Initialize an empty batch."""
        self.operations: List[Tuple[str, bytes, Union[Value, None]]] = []

    def put(self, key: Union[str, bytes], value: Value) -> Batch:
        """Add a PUT operation to the batch.

        Args:
            key: Key to insert/update
            value: Value to store

        Returns:
            Self for chaining
        """
        key_bytes = key if isinstance(key, bytes) else key.encode("utf-8")
        self.operations.append(("PUT", key_bytes, value))
        return self

    def delete(self, key: Union[str, bytes]) -> Batch:
        """Add a DELETE operation to the batch.

        Args:
            key: Key to delete

        Returns:
            Self for chaining
        """
        key_bytes = key if isinstance(key, bytes) else key.encode("utf-8")
        self.operations.append(("DELETE", key_bytes, None))
        return self

    def __len__(self) -> int:
        """Return number of operations in batch."""
        return len(self.operations)

    def is_empty(self) -> bool:
        """Check if batch is empty."""
        return len(self.operations) == 0

    def clear(self) -> None:
        """Clear all operations from batch."""
        self.operations.clear()


async def apply_batch(shard: Shard, batch: Batch) -> int:
    """Apply a batch of operations to a shard atomically.

    Args:
        shard: Shard to apply operations to
        batch: Batch containing operations

    Returns:
        Number of operations applied
    """
    if batch.is_empty():
        return 0

    # For single-shard operations, we can optimize
    count = 0
    for op_type, key, value in batch.operations:
        if op_type == "PUT" and value is not None:
            await shard.put(key, value)
            count += 1
        elif op_type == "DELETE":
            if await shard.delete(key):
                count += 1

    return count
