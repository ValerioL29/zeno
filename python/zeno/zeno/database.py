"""Database implementation for zeno KV store.

Manages 256 shards for parallel access:
- Keys are hashed and routed to appropriate shard
- Each shard has independent locking
- Supports hash tags for key locality: {user:1}:profile

Operations:
- get, put, delete: Point operations
- exists: Check existence
- scan_prefix, scan_range: Range queries
"""

from __future__ import annotations

import hashlib
from typing import List, Optional, Tuple, Union

from zeno.shard import Shard
from zeno.types import Value
from zeno.constants import NUM_SHARDS


KeyType = Union[str, bytes]


def _encode_key(key: KeyType) -> bytes:
    """Convert key to bytes."""
    if isinstance(key, str):
        return key.encode("utf-8")
    return key


def _get_shard_index(key: bytes) -> int:
    """Get shard index for a key.

    Uses hash of the key modulo NUM_SHARDS.
    Supports hash tags: {tag}:rest uses hash of tag only.

    Args:
        key: Key bytes

    Returns:
        Shard index (0-255)
    """
    # Check for hash tag
    if b"{" in key and b"}" in key:
        start = key.index(b"{")
        end = key.index(b"}", start)
        if end > start + 1:
            # Use only the tag part for hashing
            key = key[start + 1 : end]

    # Hash the key
    hash_val = hashlib.md5(key).digest()
    # Use first byte as shard index
    return hash_val[0] % NUM_SHARDS


class Database:
    """Main database class managing 256 shards.

    Provides async key-value operations with:
    - Automatic sharding by key hash
    - Hash tag support for key locality
    - Concurrent access via per-shard locks
    """

    __slots__ = ("_shards",)

    def __init__(self) -> None:
        """Initialize database with 256 shards."""
        self._shards: List[Shard] = [Shard(i) for i in range(NUM_SHARDS)]

    def _get_shard(self, key: bytes) -> Shard:
        """Get the shard for a key."""
        shard_idx = _get_shard_index(key)
        return self._shards[shard_idx]

    async def get(self, key: KeyType) -> Optional[Value]:
        """Get value by key.

        Time Complexity: O(1) shard routing + O(k) lookup

        Args:
            key: Key to lookup (str or bytes)

        Returns:
            Value if found, None otherwise
        """
        key_bytes = _encode_key(key)
        shard = self._get_shard(key_bytes)
        return await shard.get(key_bytes)

    async def put(self, key: KeyType, value: Value) -> None:
        """Insert or update key-value pair.

        Time Complexity: O(1) shard routing + O(k) insert

        Args:
            key: Key to insert (str or bytes)
            value: Value to store
        """
        key_bytes = _encode_key(key)
        shard = self._get_shard(key_bytes)
        await shard.put(key_bytes, value)

    async def delete(self, key: KeyType) -> bool:
        """Delete key from database.

        Time Complexity: O(1) shard routing + O(k) delete

        Args:
            key: Key to delete (str or bytes)

        Returns:
            True if key was found and deleted, False otherwise
        """
        key_bytes = _encode_key(key)
        shard = self._get_shard(key_bytes)
        return await shard.delete(key_bytes)

    async def exists(self, key: KeyType) -> bool:
        """Check if key exists.

        Time Complexity: O(1) shard routing + O(k) lookup

        Args:
            key: Key to check (str or bytes)

        Returns:
            True if key exists
        """
        key_bytes = _encode_key(key)
        shard = self._get_shard(key_bytes)
        return await shard.exists(key_bytes)

    async def scan_prefix(self, prefix: KeyType) -> List[Tuple[bytes, Value]]:
        """Scan all keys with given prefix.

        Note: This scans ALL shards and aggregates results.
        Use with caution on large databases.

        Time Complexity: O(s * (p + n)) where s is shard count

        Args:
            prefix: Prefix to match (str or bytes)

        Returns:
            List of (key, value) tuples sorted by key
        """
        prefix_bytes = _encode_key(prefix)

        # Scan all shards
        all_results: List[Tuple[bytes, Value]] = []

        for shard in self._shards:
            results = await shard.scan_prefix(prefix_bytes)
            all_results.extend(results)

        # Sort by key
        all_results.sort(key=lambda x: x[0])

        return all_results

    async def scan_range(
        self, start: KeyType, end: KeyType
    ) -> List[Tuple[bytes, Value]]:
        """Scan keys in range [start, end).

        Note: This scans ALL shards and filters results.
        Use with caution on large databases.

        Time Complexity: O(s * (k + n)) where s is shard count

        Args:
            start: Start key (inclusive, str or bytes)
            end: End key (exclusive, str or bytes)

        Returns:
            List of (key, value) tuples sorted by key
        """
        start_bytes = _encode_key(start)
        end_bytes = _encode_key(end)

        # Scan all shards
        all_results: List[Tuple[bytes, Value]] = []

        for shard in self._shards:
            results = await shard.scan_range(start_bytes, end_bytes)
            all_results.extend(results)

        # Sort by key
        all_results.sort(key=lambda x: x[0])

        return all_results

    async def expire_at(self, key: KeyType, timestamp: float) -> bool:
        """Set expiration time for a key.

        Args:
            key: Key to expire (str or bytes)
            timestamp: Unix timestamp when key should expire

        Returns:
            True if key exists and expiration was set
        """
        key_bytes = _encode_key(key)
        shard = self._get_shard(key_bytes)
        return await shard.expire_at(key_bytes, timestamp)

    async def ttl(self, key: KeyType) -> Optional[float]:
        """Get remaining TTL for a key.

        Args:
            key: Key to check (str or bytes)

        Returns:
            Seconds remaining until expiration, or None if no TTL
        """
        key_bytes = _encode_key(key)
        shard = self._get_shard(key_bytes)
        return await shard.ttl(key_bytes)

    async def expire(self, key: KeyType, seconds: float) -> bool:
        """Set expiration for a key in seconds from now.

        Args:
            key: Key to expire (str or bytes)
            seconds: Number of seconds until expiration

        Returns:
            True if key exists and expiration was set
        """
        import time

        return await self.expire_at(key, time.time() + seconds)

    async def cleanup_expired(self) -> int:
        """Remove all expired keys from all shards.

        Returns:
            Total number of keys removed.
        """
        total = 0
        for shard in self._shards:
            total += await shard.cleanup_expired()
        return total

    def size(self) -> int:
        """Get total number of keys in database."""
        return sum(shard.size() for shard in self._shards)

    def is_empty(self) -> bool:
        """Check if database is empty."""
        return all(shard.is_empty() for shard in self._shards)
