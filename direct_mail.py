from __future__ import annotations
import random
from typing import Dict, List

from model import Operation
from replica import Replica
from network import Network


class DirectMail:
    def __init__(self, seed: int):
        self.rnd = random.Random(seed)

    def tick(self, now: int, r: Replica, all_ids: List[str], net: Network, op_index: Dict[str, Operation]) -> None:
        # Direct mail does not perform periodic actions
        return

    def handle_message(self, now: int, dst: Replica, msg: dict, net: Network,
                       op_index: Dict[str, Operation], src_id: str) -> None:
        kind = msg.get("kind")
        if kind == "OP":
            op = Operation.from_json(msg["op"])
            was_new, _changed = dst.on_receive(op)
            # index new op for global bookkeeping
            if was_new and op.op_id not in op_index:
                op_index[op.op_id] = op
        else:
            raise ValueError(f"DirectMail: unknown message kind {kind}")
