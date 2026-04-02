"""Tests for persistence layer (WAL and Snapshots)."""

from __future__ import annotations

import os
import tempfile

import pytest

from zeno.persistence import CorruptionError, Snapshot, WalRecord, WriteAheadLog
from zeno.types import Value


class TestWalRecord:
    """Test WAL record serialization."""

    def test_put_record_to_bytes(self):
        """Serialize PUT record."""
        record = WalRecord(WalRecord.PUT, b"key", Value.string("value"), lsn=0)
        data = record.to_bytes()

        assert len(data) > 0
        assert data[0] == WalRecord.PUT

    def test_delete_record_to_bytes(self):
        """Serialize DELETE record."""
        record = WalRecord(WalRecord.DELETE, b"key", None, lsn=0)
        data = record.to_bytes()

        assert data[0] == WalRecord.DELETE

    def test_record_roundtrip(self):
        """Serialize and deserialize record."""
        original = WalRecord(WalRecord.PUT, b"test_key", Value.integer(42), lsn=100)
        data = original.to_bytes()

        restored, consumed = WalRecord.from_bytes(data)

        assert restored.record_type == WalRecord.PUT
        assert restored.key == b"test_key"
        assert restored.value.as_integer() == 42
        assert consumed == len(data)

    def test_delete_record_roundtrip(self):
        """Serialize and deserialize DELETE record."""
        original = WalRecord(WalRecord.DELETE, b"delete_key", None, lsn=50)
        data = original.to_bytes()

        restored, consumed = WalRecord.from_bytes(data)

        assert restored.record_type == WalRecord.DELETE
        assert restored.key == b"delete_key"
        assert restored.value is None

    def test_complex_value_roundtrip(self):
        """Serialize record with complex nested value."""
        value = Value.object(
            {
                "name": Value.string("test"),
                "items": Value.array([Value.integer(1), Value.integer(2)]),
                "nested": Value.object({"x": Value.float(3.14)}),
            }
        )

        original = WalRecord(WalRecord.PUT, b"complex", value, lsn=0)
        data = original.to_bytes()

        restored, _ = WalRecord.from_bytes(data)
        assert restored.value == value

    def test_empty_key(self):
        """Record with empty key."""
        record = WalRecord(WalRecord.PUT, b"", Value.null(), lsn=0)
        data = record.to_bytes()

        restored, _ = WalRecord.from_bytes(data)
        assert restored.key == b""

    def test_truncated_record_raises_error(self):
        """Truncated record raises CorruptionError."""
        record = WalRecord(WalRecord.PUT, b"key", Value.string("value"), lsn=0)
        data = record.to_bytes()

        # Try to parse truncated data
        with pytest.raises(CorruptionError):
            WalRecord.from_bytes(data[:5])

    def test_corrupted_record_raises_error(self):
        """Corrupted record raises CorruptionError."""
        record = WalRecord(WalRecord.PUT, b"key", Value.string("value"), lsn=0)
        data = bytearray(record.to_bytes())

        # Corrupt the value length
        data[5] = 0xFF
        data[6] = 0xFF
        data[7] = 0xFF
        data[8] = 0xFF

        with pytest.raises(CorruptionError):
            WalRecord.from_bytes(bytes(data))


class TestWriteAheadLog:
    """Test WAL functionality."""

    def test_create_and_close(self):
        """Create and close WAL."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            wal = WriteAheadLog(path)
            wal.open()
            assert wal._file is not None
            wal.close()
            assert wal._file is None
        finally:
            os.unlink(path)

    def test_context_manager(self):
        """Use WAL as context manager."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            with WriteAheadLog(path) as wal:
                assert wal._file is not None
            assert wal._file is None
        finally:
            os.unlink(path)

    def test_append_put(self):
        """Append PUT record."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            wal = WriteAheadLog(path)
            lsn = wal.append_put(b"key", Value.string("value"))
            assert lsn == 0

            # Second append should have higher LSN
            lsn2 = wal.append_put(b"key2", Value.integer(42))
            assert lsn2 > lsn

            wal.close()
        finally:
            os.unlink(path)

    def test_append_delete(self):
        """Append DELETE record."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            wal = WriteAheadLog(path)
            lsn = wal.append_delete(b"key")
            assert lsn == 0
            wal.close()
        finally:
            os.unlink(path)

    def test_read_all_empty(self):
        """Read from non-existent WAL returns empty list."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        os.unlink(path)  # Delete the file

        wal = WriteAheadLog(path)
        records = wal.read_all()
        assert records == []

    def test_read_all_records(self):
        """Read all records from WAL."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            wal = WriteAheadLog(path)
            wal.append_put(b"key1", Value.string("value1"))
            wal.append_put(b"key2", Value.integer(42))
            wal.append_delete(b"key1")
            wal.close()

            # Read records
            wal2 = WriteAheadLog(path)
            records = wal2.read_all()

            assert len(records) == 3
            assert records[0].record_type == WalRecord.PUT
            assert records[0].key == b"key1"
            assert records[1].key == b"key2"
            assert records[2].record_type == WalRecord.DELETE
        finally:
            os.unlink(path)

    def test_truncate(self):
        """Truncate WAL file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            wal = WriteAheadLog(path)
            wal.append_put(b"key", Value.string("value"))
            wal.close()

            # Verify file exists and has content
            assert os.path.exists(path)
            assert os.path.getsize(path) > 0

            # Truncate
            wal2 = WriteAheadLog(path)
            wal2.truncate()

            assert not os.path.exists(path)
            assert wal2.lsn == 0
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_corrupted_wal(self):
        """Handle corrupted WAL gracefully."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name
            # Write some garbage data
            f.write(b"garbage_data_not_a_valid_record")

        try:
            wal = WriteAheadLog(path)
            # Should stop at corruption point, not crash
            records = wal.read_all()
            assert records == []
        finally:
            os.unlink(path)

    def test_sync(self):
        """Sync WAL to disk."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            wal = WriteAheadLog(path)
            wal.append_put(b"key", Value.string("value"))
            wal.sync()  # Should not raise
            wal.close()
        finally:
            os.unlink(path)


class TestSnapshot:
    """Test Snapshot functionality."""

    def test_save_and_load_empty(self):
        """Save and load empty snapshot."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            snapshot = Snapshot(path)
            snapshot.save([], checkpoint_lsn=0)

            data, lsn = snapshot.load()
            assert data == []
            assert lsn == 0
        finally:
            os.unlink(path)

    def test_save_and_load_data(self):
        """Save and load snapshot with data."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            shard_data = [
                {b"key1": Value.string("value1")},
                {b"key2": Value.integer(42), b"key3": Value.boolean(True)},
            ]

            snapshot = Snapshot(path)
            snapshot.save(shard_data, checkpoint_lsn=100)

            loaded_data, loaded_lsn = snapshot.load()
            assert loaded_lsn == 100
            assert len(loaded_data) == 2
            assert loaded_data[0][b"key1"].as_string() == "value1"
            assert loaded_data[1][b"key2"].as_integer() == 42
            assert loaded_data[1][b"key3"].as_boolean() is True
        finally:
            os.unlink(path)

    def test_load_nonexistent(self):
        """Loading non-existent snapshot raises FileNotFoundError."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        os.unlink(path)  # Delete the file

        snapshot = Snapshot(path)
        with pytest.raises(FileNotFoundError):
            snapshot.load()

    def test_invalid_magic(self):
        """Invalid magic number raises CorruptionError."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name
            f.write(b"INVALID!")  # Wrong magic

        try:
            snapshot = Snapshot(path)
            with pytest.raises(CorruptionError):
                snapshot.load()
        finally:
            os.unlink(path)

    def test_unsupported_version(self):
        """Unsupported version raises CorruptionError."""
        import struct

        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name
            f.write(Snapshot.MAGIC)
            f.write(struct.pack("<I", 999))  # Unsupported version

        try:
            snapshot = Snapshot(path)
            with pytest.raises(CorruptionError):
                snapshot.load()
        finally:
            os.unlink(path)

    def test_complex_values(self):
        """Snapshot with complex nested values."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        try:
            value = Value.object(
                {
                    "array": Value.array([Value.integer(1), Value.integer(2)]),
                    "nested": Value.object({"x": Value.float(3.14)}),
                }
            )

            shard_data = [{b"complex": value}]

            snapshot = Snapshot(path)
            snapshot.save(shard_data, checkpoint_lsn=0)

            loaded_data, _ = snapshot.load()
            assert loaded_data[0][b"complex"] == value
        finally:
            os.unlink(path)


class TestPersistenceIntegration:
    """Integration tests for persistence."""

    def test_wal_recovery_scenario(self):
        """Simulate crash recovery using WAL."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            wal_path = f.name

        try:
            # Simulate some operations
            wal = WriteAheadLog(wal_path)
            wal.append_put(b"user:1", Value.string("alice"))
            wal.append_put(b"user:2", Value.string("bob"))
            wal.append_delete(b"user:1")
            wal.close()

            # "Recover" by replaying WAL
            wal2 = WriteAheadLog(wal_path)
            records = wal2.read_all()

            # Rebuild state
            state = {}
            for record in records:
                if record.record_type == WalRecord.PUT:
                    state[record.key] = record.value
                elif record.record_type == WalRecord.DELETE:
                    state.pop(record.key, None)

            assert state == {b"user:2": Value.string("bob")}
        finally:
            os.unlink(wal_path)
