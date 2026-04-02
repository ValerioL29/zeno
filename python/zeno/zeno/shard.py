"""Shard implementation for zeno KV store.

Each shard contains:
- An ART index for ordered key storage
- A lock for thread safety
- TTL tracking for expiration

The shard design allows for 256-way parallelism by sharding
keys based on hash.
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

import anyio

from zeno.art.tree import Tree
from zeno.types import Value


class Shard:
    """A single shard of the database.

    Manages a subset of keys using an ART index.
    Uses anyio.Lock for async safety.
    """

    __slots__ = ("shard_idx", "_tree", "_lock", "_ttl_index")

    def __init__(self, shard_idx: int) -> None:
        """Initialize a shard.

        Args:
            shard_idx: Index of this shard (0-255)

        Raises:
            ValueError: If shard_idx is out of range.
        """
        if not 0 <= shard_idx <= 255:
            raise ValueError(f"shard_idx must be in range [0, 255], got {shard_idx}")
        self.shard_idx: int = shard_idx
        self._tree: Tree = Tree()
        self._lock: anyio.Lock = anyio.Lock()
        self._ttl_index: Dict[bytes, float] = {}

    async def get(self, key: bytes) -> Optional[Value]:
        """Get value by key.

        Time Complexity: O(k) where k is key length

        Args:
            key: Key to lookup

        Returns:
            Value if found and not expired, None otherwise
        """
        async with self._lock:
            return await self._get_unlocked(key)

    async def _get_unlocked(self, key: bytes) -> Optional[Value]:
        """Get value without acquiring lock (internal use only)."""
        # Check TTL
        if key in self._ttl_index:
            if time.time() > self._ttl_index[key]:
                # Expired
                self._tree.delete(key)
                del self._ttl_index[key]
                return None

        value = self._tree.lookup(key)
        return value.clone() if value is not None else None

    async def put(self, key: bytes, value: Value) -> None:
        """Insert or update key-value pair.

        Time Complexity: O(k) where k is key length

        Args:
            key: Key to insert
            value: Value to store
        """
        async with self._lock:
            self._tree.insert(key, value.clone())
            # Clear any existing TTL
            self._ttl_index.pop(key, None)

    async def delete(self, key: bytes) -> bool:
        """Delete key from shard.

        Time Complexity: O(k) where k is key length

        Args:
            key: Key to delete

        Returns:
            True if key was found and deleted, False otherwise
        """
        async with self._lock:
            # Clear TTL if exists
            self._ttl_index.pop(key, None)
            return self._tree.delete(key)

    async def exists(self, key: bytes) -> bool:
        """Check if key exists and is not expired.

        Time Complexity: O(k) where k is key length

        Args:
            key: Key to check

        Returns:
            True if key exists and is not expired
        """
        async with self._lock:
            # Check TTL
            if key in self._ttl_index:
                if time.time() > self._ttl_index[key]:
                    # Expired
                    self._tree.delete(key)
                    del self._ttl_index[key]
                    return False

            return self._tree.lookup(key) is not None

    async def scan_prefix(self, prefix: bytes) -> List[Tuple[bytes, Value]]:
        """Scan all keys with given prefix.

        Time Complexity: O(p + n) where p is prefix length, n is matches

        Args:
            prefix: Prefix to match

        Returns:
            List of (key, value) tuples sorted by key
        """
        async with self._lock:
            results = self._tree.scan_prefix(prefix)
            return self._filter_expired(results)

    async def scan_range(self, start: bytes, end: bytes) -> List[Tuple[bytes, Value]]:
        """Scan keys in range [start, end).

        Time Complexity: O(k + n) where k is key length, n is matches

        Args:
            start: Start key (inclusive)
            end: End key (exclusive)

        Returns:
            List of (key, value) tuples sorted by key
        """
        async with self._lock:
            results = self._tree.scan_range(start, end)
            return self._filter_expired(results)

    def _filter_expired(
        self, results: List[Tuple[bytes, Value]]
    ) -> List[Tuple[bytes, Value]]:
        """Filter out expired keys from scan results."""
        current_time = time.time()
        filtered_results = []

        for key, value in results:
            if key in self._ttl_index:
                if current_time > self._ttl_index[key]:
                    # Expired, skip
                    continue

            filtered_results.append((key, value.clone()))

        return filtered_results

    async def expire_at(self, key: bytes, timestamp: float) -> bool:
        """Set expiration time for a key.

        Args:
            key: Key to expire
            timestamp: Unix timestamp when key should expire

        Returns:
            True if key exists and expiration was set
        """
        async with self._lock:
            if self._tree.lookup(key) is None:
                return False

            self._ttl_index[key] = timestamp
            return True

    async def ttl(self, key: bytes) -> Optional[float]:
        """Get remaining TTL for a key.

        Args:
            key: Key to check

        Returns:
            Seconds remaining until expiration, or None if no TTL
        """
        async with self._lock:
            if key not in self._ttl_index:
                return None

            remaining = self._ttl_index[key] - time.time()
            if remaining <= 0:
                # Expired, clean up
                self._tree.delete(key)
                del self._ttl_index[key]
                return None

            return remaining

    async def cleanup_expired(self) -> int:
        """Remove all expired keys from the shard.

        Returns:
            Number of keys removed.
        """
        async with self._lock:
            current_time = time.time()
            expired_keys = [
                key
                for key, timestamp in self._ttl_index.items()
                if current_time > timestamp
            ]

            for key in expired_keys:
                self._tree.delete(key)
                del self._ttl_index[key]

            return len(expired_keys)

    def size(self) -> int:
        """Get number of keys in shard."""
        return self._tree.size()

    def is_empty(self) -> bool:
        """Check if shard is empty."""
        return self._tree.is_empty()
