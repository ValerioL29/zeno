"""Tests for TTL (Time-To-Live) functionality."""

from __future__ import annotations

import asyncio
import time

import anyio
import pytest

from zeno.database import Database
from zeno.shard import Shard
from zeno.types import Value


class TestShardTTL:
    """Test TTL operations at shard level."""

    @pytest.mark.anyio
    async def test_expire_at(self):
        """Set expiration time for a key."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        # Set expiration 1 second from now
        future_time = time.time() + 1.0
        result = await shard.expire_at(b"key", future_time)
        assert result is True

        # Key should still exist
        assert await shard.get(b"key") is not None

    @pytest.mark.anyio
    async def test_expire_at_nonexistent(self):
        """Cannot set expiration for non-existent key."""
        shard = Shard(shard_idx=0)

        result = await shard.expire_at(b"nonexistent", time.time() + 1.0)
        assert result is False

    @pytest.mark.anyio
    async def test_ttl_get(self):
        """Get remaining TTL."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        future_time = time.time() + 10.0
        await shard.expire_at(b"key", future_time)

        ttl = await shard.ttl(b"key")
        assert ttl is not None
        assert 9.0 <= ttl <= 10.0

    @pytest.mark.anyio
    async def test_ttl_no_expiration(self):
        """TTL returns None for key without expiration."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        ttl = await shard.ttl(b"key")
        assert ttl is None

    @pytest.mark.anyio
    async def test_ttl_expired(self):
        """TTL returns None for expired key and cleans up."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        # Set expiration in the past
        past_time = time.time() - 1.0
        await shard.expire_at(b"key", past_time)

        ttl = await shard.ttl(b"key")
        assert ttl is None

        # Key should be deleted
        assert await shard.get(b"key") is None

    @pytest.mark.anyio
    async def test_get_expired_key(self):
        """Getting an expired key returns None and cleans up."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        past_time = time.time() - 1.0
        await shard.expire_at(b"key", past_time)

        result = await shard.get(b"key")
        assert result is None

    @pytest.mark.anyio
    async def test_exists_expired_key(self):
        """Exists returns False for expired key and cleans up."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        past_time = time.time() - 1.0
        await shard.expire_at(b"key", past_time)

        result = await shard.exists(b"key")
        assert result is False

    @pytest.mark.anyio
    async def test_put_clears_ttl(self):
        """Putting a key clears its TTL."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        future_time = time.time() + 10.0
        await shard.expire_at(b"key", future_time)

        # Overwrite with put
        await shard.put(b"key", Value.string("new_value"))

        # TTL should be cleared
        ttl = await shard.ttl(b"key")
        assert ttl is None

    @pytest.mark.anyio
    async def test_scan_prefix_filters_expired(self):
        """Scan prefix filters out expired keys."""
        shard = Shard(shard_idx=0)

        await shard.put(b"user:1", Value.string("alice"))
        await shard.put(b"user:2", Value.string("bob"))

        # Expire user:1
        past_time = time.time() - 1.0
        await shard.expire_at(b"user:1", past_time)

        results = await shard.scan_prefix(b"user:")
        keys = [r[0] for r in results]

        assert b"user:1" not in keys
        assert b"user:2" in keys

    @pytest.mark.anyio
    async def test_scan_range_filters_expired(self):
        """Scan range filters out expired keys."""
        shard = Shard(shard_idx=0)

        for i in range(5):
            await shard.put(bytes([97 + i]), Value.integer(i))

        # Expire 'b'
        past_time = time.time() - 1.0
        await shard.expire_at(b"b", past_time)

        results = await shard.scan_range(b"a", b"e")
        keys = [r[0] for r in results]

        assert b"a" in keys
        assert b"b" not in keys
        assert b"c" in keys
        assert b"d" in keys

    @pytest.mark.anyio
    async def test_cleanup_expired(self):
        """Cleanup expired keys returns count."""
        shard = Shard(shard_idx=0)

        # Add some keys
        await shard.put(b"key1", Value.string("value1"))
        await shard.put(b"key2", Value.string("value2"))
        await shard.put(b"key3", Value.string("value3"))

        # Expire key1 and key2
        past_time = time.time() - 1.0
        await shard.expire_at(b"key1", past_time)
        await shard.expire_at(b"key2", past_time)

        # Cleanup should remove 2 keys
        count = await shard.cleanup_expired()
        assert count == 2

        # Verify cleanup
        assert await shard.get(b"key1") is None
        assert await shard.get(b"key2") is None
        assert await shard.get(b"key3") is not None

    @pytest.mark.anyio
    async def test_cleanup_expired_none(self):
        """Cleanup returns 0 when no expired keys."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        count = await shard.cleanup_expired()
        assert count == 0


class TestDatabaseTTL:
    """Test TTL operations at database level."""

    @pytest.mark.anyio
    async def test_expire_at(self):
        """Set expiration at database level."""
        db = Database()
        await db.put("key", Value.string("value"))

        future_time = time.time() + 10.0
        result = await db.expire_at("key", future_time)
        assert result is True

        ttl = await db.ttl("key")
        assert ttl is not None
        assert 9.0 <= ttl <= 10.0

    @pytest.mark.anyio
    async def test_expire(self):
        """Set expiration in seconds from now."""
        db = Database()
        await db.put("key", Value.string("value"))

        result = await db.expire("key", 10.0)
        assert result is True

        ttl = await db.ttl("key")
        assert ttl is not None
        assert 9.0 <= ttl <= 10.0

    @pytest.mark.anyio
    async def test_expire_nonexistent(self):
        """Cannot expire non-existent key."""
        db = Database()

        result = await db.expire("nonexistent", 10.0)
        assert result is False

    @pytest.mark.anyio
    async def test_cleanup_expired_all_shards(self):
        """Cleanup expired keys across all shards."""
        db = Database()

        # Add keys that will go to different shards
        for i in range(10):
            await db.put(f"key_{i}", Value.integer(i))

        # Expire some keys
        past_time = time.time() - 1.0
        for i in range(5):
            await db.expire_at(f"key_{i}", past_time)

        # Cleanup should work across all shards
        count = await db.cleanup_expired()
        assert count == 5

        # Verify cleanup
        for i in range(5):
            assert await db.get(f"key_{i}") is None
        for i in range(5, 10):
            assert await db.get(f"key_{i}") is not None

    @pytest.mark.anyio
    async def test_ttl_with_hash_tag(self):
        """TTL works with hash tags."""
        db = Database()

        await db.put("{user:1}:profile", Value.string("profile_data"))
        await db.put("{user:1}:settings", Value.string("settings_data"))

        await db.expire("{user:1}:profile", 10.0)

        # Keys with same hash tag might be in same shard
        ttl1 = await db.ttl("{user:1}:profile")
        ttl2 = await db.ttl("{user:1}:settings")

        assert ttl1 is not None
        assert ttl2 is None  # No TTL set for settings


class TestTTLConcurrency:
    """Test TTL with concurrent operations."""

    @pytest.mark.anyio
    async def test_concurrent_ttl_reads(self):
        """Multiple concurrent TTL reads."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))
        await shard.expire_at(b"key", time.time() + 10.0)

        results = []
        async with anyio.create_task_group() as tg:

            async def read_ttl():
                ttl = await shard.ttl(b"key")
                results.append(ttl)

            for _ in range(10):
                tg.start_soon(read_ttl)

        # All should get valid TTL
        assert all(ttl is not None for ttl in results)
        assert all(9.0 <= ttl <= 10.0 for ttl in results)

    @pytest.mark.anyio
    async def test_ttl_expiration_during_scan(self):
        """Key expires during scan operation."""
        shard = Shard(shard_idx=0)

        await shard.put(b"user:1", Value.string("alice"))
        await shard.put(b"user:2", Value.string("bob"))

        # Set user:1 to expire very soon
        await shard.expire_at(b"user:1", time.time() + 0.01)

        # Wait for expiration
        await asyncio.sleep(0.02)

        # Scan should filter expired key
        results = await shard.scan_prefix(b"user:")
        keys = [r[0] for r in results]

        assert b"user:1" not in keys
        assert b"user:2" in keys
