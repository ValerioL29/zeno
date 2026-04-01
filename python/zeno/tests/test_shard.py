"""Tests for Shard implementation."""

from __future__ import annotations

import pytest
import anyio

from zeno.shard import Shard
from zeno.types import Value


class TestShardBasic:
    """Test basic shard operations."""

    @pytest.mark.anyio
    async def test_init(self):
        """Shard initializes correctly."""
        shard = Shard(shard_idx=0)
        assert shard.shard_idx == 0

    @pytest.mark.anyio
    async def test_put_and_get(self):
        """Put and get value."""
        shard = Shard(shard_idx=0)

        await shard.put(b"key", Value.string("value"))
        result = await shard.get(b"key")

        assert result is not None
        assert result.as_string() == "value"

    @pytest.mark.anyio
    async def test_get_nonexistent(self):
        """Get non-existent key returns None."""
        shard = Shard(shard_idx=0)

        result = await shard.get(b"missing")
        assert result is None

    @pytest.mark.anyio
    async def test_put_overwrite(self):
        """Put overwrites existing value."""
        shard = Shard(shard_idx=0)

        await shard.put(b"key", Value.string("first"))
        await shard.put(b"key", Value.string("second"))

        result = await shard.get(b"key")
        assert result.as_string() == "second"

    @pytest.mark.anyio
    async def test_delete_existing(self):
        """Delete existing key."""
        shard = Shard(shard_idx=0)

        await shard.put(b"key", Value.string("value"))
        deleted = await shard.delete(b"key")

        assert deleted is True
        assert await shard.get(b"key") is None

    @pytest.mark.anyio
    async def test_delete_nonexistent(self):
        """Delete non-existent key returns False."""
        shard = Shard(shard_idx=0)

        deleted = await shard.delete(b"missing")
        assert deleted is False

    @pytest.mark.anyio
    async def test_exists(self):
        """Check key existence."""
        shard = Shard(shard_idx=0)

        await shard.put(b"key", Value.string("value"))

        assert await shard.exists(b"key") is True
        assert await shard.exists(b"missing") is False


class TestShardMultipleKeys:
    """Test shard with multiple keys."""

    @pytest.mark.anyio
    async def test_multiple_keys(self):
        """Handle multiple keys in same shard."""
        shard = Shard(shard_idx=0)

        keys_values = [
            (b"aaa", Value.integer(1)),
            (b"aab", Value.integer(2)),
            (b"aba", Value.integer(3)),
        ]

        for key, value in keys_values:
            await shard.put(key, value)

        for key, value in keys_values:
            result = await shard.get(key)
            assert result == value


class TestShardConcurrent:
    """Test concurrent operations on shard."""

    @pytest.mark.anyio
    async def test_concurrent_reads(self):
        """Multiple concurrent reads."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key", Value.string("value"))

        results = []
        async with anyio.create_task_group() as tg:

            async def read():
                result = await shard.get(b"key")
                results.append(result)

            for _ in range(10):
                tg.start_soon(read)

        assert all(r.as_string() == "value" for r in results)

    @pytest.mark.anyio
    async def test_concurrent_writes(self):
        """Sequential writes (writes are exclusive)."""
        shard = Shard(shard_idx=0)

        for i in range(10):
            await shard.put(b"key", Value.integer(i))

        result = await shard.get(b"key")
        assert result.as_integer() == 9


class TestShardScan:
    """Test shard scan operations."""

    @pytest.mark.anyio
    async def test_scan_prefix(self):
        """Scan keys with prefix."""
        shard = Shard(shard_idx=0)

        await shard.put(b"user:1", Value.string("alice"))
        await shard.put(b"user:2", Value.string("bob"))
        await shard.put(b"post:1", Value.string("post1"))

        results = await shard.scan_prefix(b"user:")

        keys = [r[0] for r in results]
        assert b"user:1" in keys
        assert b"user:2" in keys
        assert b"post:1" not in keys

    @pytest.mark.anyio
    async def test_scan_range(self):
        """Scan keys in range."""
        shard = Shard(shard_idx=0)

        for i in range(10):
            await shard.put(bytes([97 + i]), Value.integer(i))

        results = await shard.scan_range(b"c", b"g")

        keys = [r[0] for r in results]
        assert keys == [b"c", b"d", b"e", b"f"]
