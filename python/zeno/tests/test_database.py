"""Tests for Database implementation."""

from __future__ import annotations

import pytest
import anyio

from zeno.database import Database
from zeno.types import Value


class TestDatabaseBasic:
    """Test basic database operations."""

    @pytest.mark.anyio
    async def test_init(self):
        """Database initializes correctly."""
        db = Database()
        assert db is not None

    @pytest.mark.anyio
    async def test_put_and_get(self):
        """Put and get value."""
        db = Database()

        await db.put("key", Value.string("value"))
        result = await db.get("key")

        assert result is not None
        assert result.as_string() == "value"

    @pytest.mark.anyio
    async def test_get_nonexistent(self):
        """Get non-existent key returns None."""
        db = Database()

        result = await db.get("missing")
        assert result is None

    @pytest.mark.anyio
    async def test_put_overwrite(self):
        """Put overwrites existing value."""
        db = Database()

        await db.put("key", Value.string("first"))
        await db.put("key", Value.string("second"))

        result = await db.get("key")
        assert result.as_string() == "second"

    @pytest.mark.anyio
    async def test_delete_existing(self):
        """Delete existing key."""
        db = Database()

        await db.put("key", Value.string("value"))
        deleted = await db.delete("key")

        assert deleted is True
        assert await db.get("key") is None

    @pytest.mark.anyio
    async def test_delete_nonexistent(self):
        """Delete non-existent key returns False."""
        db = Database()

        deleted = await db.delete("missing")
        assert deleted is False

    @pytest.mark.anyio
    async def test_exists(self):
        """Check key existence."""
        db = Database()

        await db.put("key", Value.string("value"))

        assert await db.exists("key") is True
        assert await db.exists("missing") is False


class TestDatabaseMultipleKeys:
    """Test with multiple keys across shards."""

    @pytest.mark.anyio
    async def test_many_keys(self):
        """Handle many keys across different shards."""
        db = Database()

        # Add many keys
        for i in range(100):
            await db.put(f"key_{i}", Value.integer(i))

        # Verify all
        for i in range(100):
            result = await db.get(f"key_{i}")
            assert result.as_integer() == i

    @pytest.mark.anyio
    async def test_binary_keys(self):
        """Handle binary keys."""
        db = Database()

        keys = [
            b"\x00\x01\x02",
            b"\xff\xfe\xfd",
            b"\x7f\x80\x81",
        ]

        for i, key in enumerate(keys):
            await db.put(key, Value.integer(i))

        for i, key in enumerate(keys):
            result = await db.get(key)
            assert result.as_integer() == i


class TestDatabaseScan:
    """Test database scan operations."""

    @pytest.mark.anyio
    async def test_scan_prefix(self):
        """Scan keys with prefix."""
        db = Database()

        await db.put("user:1", Value.string("alice"))
        await db.put("user:2", Value.string("bob"))
        await db.put("post:1", Value.string("post1"))
        await db.put("user:3", Value.string("charlie"))

        results = await db.scan_prefix("user:")

        keys = [r[0] for r in results]
        assert b"user:1" in keys or "user:1" in keys
        assert b"user:2" in keys or "user:2" in keys
        assert b"user:3" in keys or "user:3" in keys

    @pytest.mark.anyio
    async def test_scan_range(self):
        """Scan keys in range."""
        db = Database()

        for i in range(10):
            await db.put(f"key_{i:02d}", Value.integer(i))

        results = await db.scan_range("key_03", "key_07")

        keys = [r[0] for r in results]
        assert len(keys) >= 3  # key_03, key_04, key_05, key_06


class TestDatabaseComplexValues:
    """Test with complex nested values."""

    @pytest.mark.anyio
    async def test_nested_object(self):
        """Store and retrieve nested object."""
        db = Database()

        value = Value.object(
            {
                "name": Value.string("Alice"),
                "age": Value.integer(30),
                "tags": Value.array([Value.string("admin"), Value.string("user")]),
            }
        )

        await db.put("user:1", value)
        result = await db.get("user:1")

        obj = result.as_object()
        assert obj["name"].as_string() == "Alice"
        assert obj["age"].as_integer() == 30
        assert len(obj["tags"].as_array()) == 2

    @pytest.mark.anyio
    async def test_array_value(self):
        """Store and retrieve array."""
        db = Database()

        value = Value.array(
            [
                Value.integer(1),
                Value.integer(2),
                Value.integer(3),
            ]
        )

        await db.put("numbers", value)
        result = await db.get("numbers")

        arr = result.as_array()
        assert len(arr) == 3
        assert arr[0].as_integer() == 1


class TestDatabaseConcurrent:
    """Test concurrent operations."""

    @pytest.mark.anyio
    async def test_concurrent_writes_different_keys(self):
        """Concurrent writes to different keys."""
        db = Database()

        async def write(i):
            await db.put(f"key_{i}", Value.integer(i))

        async with anyio.create_task_group() as tg:
            for i in range(10):
                tg.start_soon(write, i)

        # Verify all writes
        for i in range(10):
            result = await db.get(f"key_{i}")
            assert result.as_integer() == i

    @pytest.mark.anyio
    async def test_concurrent_reads(self):
        """Concurrent reads."""
        db = Database()
        await db.put("key", Value.string("value"))

        results = []
        async with anyio.create_task_group() as tg:

            async def read():
                result = await db.get("key")
                results.append(result)

            for _ in range(10):
                tg.start_soon(read)

        assert all(r.as_string() == "value" for r in results)
