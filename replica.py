from __future__ import annotations
import random
from typing import Dict, Optional, Set, Tuple, Any, List

from model import Record, Operation, Timestamp


class Replica:
    def __init__(self, replica_id: str, store: Dict[str, Record], seed: int):
        self.id = replica_id
        self.store: Dict[str, Record] = store
        self.seen_ops: Set[str] = set()
        self.rnd = random.Random(seed)

        # rumor state: op_id -> remaining_budget, plus "already_seen" counters
        self.active_rumors: Dict[str, int] = {}
        self.rumor_already_seen_hits: Dict[str, int] = {}

        # metrics
        self.ops_applied = 0
        self.ops_received = 0
        self.ops_sent = 0

    def apply(self, op: Operation) -> bool:
        """
        Apply operation using LWW + tombstone.
        Returns True if it changed local store (i.e., op was newer than current record).
        """
        cur = self.store.get(op.key)
        incoming_ts = op.ts
        if cur is None or incoming_ts > cur.ts:
            if op.op == "SET":
                self.store[op.key] = Record(value=op.value, deleted=False, ts=incoming_ts)
            elif op.op == "DEL":
                self.store[op.key] = Record(value=None, deleted=True, ts=incoming_ts)
            else:
                raise ValueError(f"Unknown op {op.op}")
            self.ops_applied += 1
            return True
        return False

    def on_receive(self, op: Operation) -> Tuple[bool, bool]:
        """
        Process incoming op with dedup.
        Returns (was_new_op, changed_state)
        """
        self.ops_received += 1
        if op.op_id in self.seen_ops:
            return False, False
        self.seen_ops.add(op.op_id)
        changed = self.apply(op)
        return True, changed

    def activate_rumor(self, op_id: str, budget: int) -> None:
        # only if still active or new
        if op_id not in self.active_rumors:
            self.active_rumors[op_id] = budget
            self.rumor_already_seen_hits[op_id] = 0

    def pick_peer(self, peers: List[str]) -> str:
        return self.rnd.choice(peers)
