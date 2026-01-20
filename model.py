from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Tuple


@dataclass(frozen=True, order=True)
class Timestamp:
    """
    Total order: (counter, replica_id).
    """
    counter: int
    replica_id: str

    def to_json(self) -> list:
        return [self.counter, self.replica_id]

    @staticmethod
    def from_json(x: list) -> "Timestamp":
        return Timestamp(int(x[0]), str(x[1]))


@dataclass
class Record:
    value: Any
    deleted: bool
    ts: Timestamp

    def to_json(self) -> dict:
        return {"value": self.value, "deleted": self.deleted, "ts": self.ts.to_json()}

    @staticmethod
    def from_json(x: dict) -> "Record":
        return Record(value=x.get("value"), deleted=bool(x.get("deleted")), ts=Timestamp.from_json(x["ts"]))


@dataclass(frozen=True)
class Operation:
    op_id: str            # e.g. "R3:17"
    op: str               # "SET" or "DEL"
    key: str
    value: Optional[Any]  # None for DEL
    ts: Timestamp
    origin: str           # replica id

    def to_json(self) -> dict:
        return {
            "op_id": self.op_id,
            "op": self.op,
            "key": self.key,
            "value": self.value,
            "ts": self.ts.to_json(),
            "origin": self.origin,
        }

    @staticmethod
    def from_json(x: dict) -> "Operation":
        return Operation(
            op_id=str(x["op_id"]),
            op=str(x["op"]),
            key=str(x["key"]),
            value=x.get("value", None),
            ts=Timestamp.from_json(x["ts"]),
            origin=str(x["origin"]),
        )
