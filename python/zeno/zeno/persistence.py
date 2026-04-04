"""Persistence layer for zeno KV store.

Provides WAL (Write-Ahead Log) and Snapshot functionality for durability.
"""

from __future__ import annotations

import pickle
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from zeno.types import Value


class WalRecord:
    """A single WAL record.

    Format:
    - 1 byte: record type (PUT=1, DELETE=2)
    - 4 bytes: key length (uint32)
    - N bytes: key data
    - 4 bytes: value length (uint32, 0 for DELETE)
    - N bytes: value data (pickle, empty for DELETE)
    """

    PUT = 1
    DELETE = 2

    def __init__(
        self,
        record_type: int,
        key: bytes,
        value: Optional[Value] = None,
        lsn: int = 0,
    ) -> None:
        self.record_type = record_type
        self.key = key
        self.value = value
        self.lsn = lsn  # Log Sequence Number

    def to_bytes(self) -> bytes:
        """Serialize record to bytes."""
        data = struct.pack("<B", self.record_type)
        data += struct.pack("<I", len(self.key))
        data += self.key

        if self.value is not None:
            value_bytes = pickle.dumps(self.value)
            data += struct.pack("<I", len(value_bytes))
            data += value_bytes
        else:
            data += struct.pack("<I", 0)

        return data

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> Tuple[WalRecord, int]:
        """Deserialize record from bytes.

        Returns:
            Tuple of (record, bytes_consumed)
        """
        record_type = struct.unpack("<B", data[offset : offset + 1])[0]
        offset += 1

        key_len = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4
        key = data[offset : offset + key_len]
        offset += key_len

        value_len = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4

        value = None
        if value_len > 0:
            value = pickle.loads(data[offset : offset + value_len])
            offset += value_len

        return cls(record_type, key, value), offset


class WriteAheadLog:
    """Append-only write-ahead log for durability.

    Records all mutations before they are applied to the database,
    allowing recovery after crashes.
    """

    def __init__(self, wal_path: str) -> None:
        """Initialize WAL.

        Args:
            wal_path: Path to WAL file
        """
        self.wal_path = Path(wal_path)
        self.lsn = 0  # Log Sequence Number
        self._file: Optional[Any] = None

    def open(self) -> None:
        """Open WAL file for writing."""
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.wal_path, "ab")

        # Read current LSN
        if self.wal_path.exists():
            self.lsn = self.wal_path.stat().st_size

    def close(self) -> None:
        """Close WAL file."""
        if self._file:
            self._file.close()
            self._file = None

    def append_put(self, key: bytes, value: Value) -> int:
        """Append a PUT record to WAL.

        Args:
            key: Key that was written
            value: Value that was written

        Returns:
            LSN of the record
        """
        record = WalRecord(WalRecord.PUT, key, value, self.lsn)
        return self._append_record(record)

    def append_delete(self, key: bytes) -> int:
        """Append a DELETE record to WAL.

        Args:
            key: Key that was deleted

        Returns:
            LSN of the record
        """
        record = WalRecord(WalRecord.DELETE, key, None, self.lsn)
        return self._append_record(record)

    def _append_record(self, record: WalRecord) -> int:
        """Append a record to WAL."""
        if self._file is None:
            self.open()

        data = record.to_bytes()
        self._file.write(data)
        self._file.flush()
        # os.fsync(self._file.fileno())  # Uncomment for durability

        start_lsn = self.lsn
        self.lsn += len(data)

        return start_lsn

    def read_all(self) -> List[WalRecord]:
        """Read all records from WAL.

        Returns:
            List of records in order
        """
        if not self.wal_path.exists():
            return []

        records = []
        with open(self.wal_path, "rb") as f:
            data = f.read()

        offset = 0
        while offset < len(data):
            try:
                record, consumed = WalRecord.from_bytes(data, offset)
                records.append(record)
                offset += consumed
            except (struct.error, pickle.UnpicklingError):
                # Incomplete or corrupted record, stop here
                break

        return records

    def truncate(self) -> None:
        """Truncate WAL file (typically after snapshot)."""
        self.close()
        if self.wal_path.exists():
            self.wal_path.unlink()
        self.lsn = 0


class Snapshot:
    """Point-in-time snapshot of database state.

    Uses pickle for serialization. Format:
    - 8 bytes: magic number
    - 4 bytes: version
    - 8 bytes: checkpoint LSN
    - 4 bytes: number of shards
    - For each shard:
      - 4 bytes: number of keys
      - For each key:
        - 4 bytes: key length
        - N bytes: key data
        - 4 bytes: value length
        - N bytes: value data (pickle)
    """

    MAGIC = b"ZENO\x00\x00\x00"
    VERSION = 1

    def __init__(self, snapshot_path: str) -> None:
        """Initialize snapshot.

        Args:
            snapshot_path: Path to snapshot file
        """
        self.snapshot_path = Path(snapshot_path)

    def save(self, shard_data: List[Dict[bytes, Value]], checkpoint_lsn: int) -> None:
        """Save snapshot to file.

        Args:
            shard_data: List of shard dictionaries (key -> value)
            checkpoint_lsn: LSN at which snapshot was taken
        """
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.snapshot_path, "wb") as f:
            # Header
            f.write(self.MAGIC)
            f.write(struct.pack("<I", self.VERSION))
            f.write(struct.pack("<Q", checkpoint_lsn))
            f.write(struct.pack("<I", len(shard_data)))

            # Shard data
            for shard in shard_data:
                f.write(struct.pack("<I", len(shard)))
                for key, value in shard.items():
                    f.write(struct.pack("<I", len(key)))
                    f.write(key)
                    value_bytes = pickle.dumps(value)
                    f.write(struct.pack("<I", len(value_bytes)))
                    f.write(value_bytes)

    def load(self) -> Tuple[List[Dict[bytes, Value]], int]:
        """Load snapshot from file.

        Returns:
            Tuple of (shard_data, checkpoint_lsn)
        """
        if not self.snapshot_path.exists():
            return [], 0

        with open(self.snapshot_path, "rb") as f:
            # Header
            magic = f.read(8)
            if magic != self.MAGIC:
                raise ValueError("Invalid snapshot file")

            version = struct.unpack("<I", f.read(4))[0]
            if version != self.VERSION:
                raise ValueError(f"Unsupported snapshot version: {version}")

            checkpoint_lsn = struct.unpack("<Q", f.read(8))[0]
            num_shards = struct.unpack("<I", f.read(4))[0]

            # Shard data
            shard_data = []
            for _ in range(num_shards):
                num_keys = struct.unpack("<I", f.read(4))[0]
                shard: Dict[bytes, Value] = {}

                for _ in range(num_keys):
                    key_len = struct.unpack("<I", f.read(4))[0]
                    key = f.read(key_len)
                    value_len = struct.unpack("<I", f.read(4))[0]
                    value_bytes = f.read(value_len)
                    value = pickle.loads(value_bytes)
                    shard[key] = value

                shard_data.append(shard)

        return shard_data, checkpoint_lsn
