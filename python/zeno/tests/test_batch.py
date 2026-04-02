"""Tests for batch operations."""

from __future__ import annotations

import pytest

from zeno.batch import Batch, apply_batch
from zeno.shard import Shard
from zeno.types import Value


class TestBatch:
    """Test batch operations."""

    @pytest.mark.anyio
    async def test_batch_put(self):
        """Test batch put operations."""
        batch = Batch()
        batch.put("key1", Value.string("value1"))
        batch.put("key2", Value.string("value2"))

        assert len(batch) == 2

    @pytest.mark.anyio
    async def test_batch_delete(self):
        """Test batch delete operations."""
        batch = Batch()
        batch.put("key1", Value.string("value1"))
        batch.delete("key1")

        assert len(batch) == 2

    @pytest.mark.anyio
    async def test_batch_chaining(self):
        """Test batch method chaining."""
        batch = (
            Batch()
            .put("key1", Value.string("value1"))
            .put("key2", Value.string("value2"))
            .delete("key3")
        )

        assert len(batch) == 3

    @pytest.mark.anyio
    async def test_batch_clear(self):
        """Test batch clear."""
        batch = Batch()
        batch.put("key1", Value.string("value1"))
        batch.clear()

        assert batch.is_empty()

    @pytest.mark.anyio
    async def test_apply_batch_put(self):
        """Test applying batch to shard."""
        shard = Shard(shard_idx=0)
        batch = Batch()
        batch.put(b"key1", Value.string("value1"))
        batch.put(b"key2", Value.string("value2"))

        count = await apply_batch(shard, batch)

        assert count == 2
        assert (await shard.get(b"key1")).as_string() == "value1"
        assert (await shard.get(b"key2")).as_string() == "value2"

    @pytest.mark.anyio
    async def test_apply_batch_delete(self):
        """Test batch delete in shard."""
        shard = Shard(shard_idx=0)
        await shard.put(b"key1", Value.string("value1"))

        batch = Batch()
        batch.delete("key1")

        count = await apply_batch(shard, batch)

        assert count == 1
        assert await shard.get(b"key1") is None

    @pytest.mark.anyio
    async def test_apply_empty_batch(self):
        """Test applying empty batch."""
        shard = Shard(shard_idx=0)
        batch = Batch()

        count = await apply_batch(shard, batch)

        assert count == 0
