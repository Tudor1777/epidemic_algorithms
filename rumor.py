from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List
import random

from model import Operation
from replica import Replica
from network import Network

@dataclass
class MsgOp:
    kind: str  # "OP"
    op: dict

@dataclass
class MsgAck:
    kind: str  # "ACK"
    op_id: str
    status: str  # "NEW" or "SEEN"


class RumorMongering():
    def __init__(self, seed: int, rumor_budget: int = 30, rumor_fanout: int = 1, stop_threshold: int = 4):
        self.rnd = random.Random(seed)
        self.rumor_budget = rumor_budget
        self.rumor_fanout = rumor_fanout
        self.stop_threshold = stop_threshold

    def tick(self, now: int, r: Replica, all_ids: List[str], net: Network, op_index: Dict[str, Operation]) -> None:
        peers = [x for x in all_ids if x != r.id]
        if not peers:
            return

        for op_id in list(r.active_rumors.keys()):
            budget = r.active_rumors[op_id]
            if budget <= 0:
                r.active_rumors.pop(op_id, None)
                r.rumor_already_seen_hits.pop(op_id, None)
                continue

            op = op_index.get(op_id)
            if op is None:
                r.active_rumors.pop(op_id, None)
                r.rumor_already_seen_hits.pop(op_id, None)
                continue

            for _ in range(self.rumor_fanout):
                dst = r.pick_peer(peers)
                net.send(now, r.id, dst, MsgOp(kind="OP", op=op.to_json()).__dict__)
                r.ops_sent += 1

            r.active_rumors[op_id] = budget - 1
            if r.rumor_already_seen_hits.get(op_id, 0) >= self.stop_threshold:
                r.active_rumors.pop(op_id, None)
                r.rumor_already_seen_hits.pop(op_id, None)

    def handle_message(self, now: int, dst: Replica, msg: dict, net: Network,
                       op_index: Dict[str, Operation], src_id: str) -> None:
        kind = msg.get("kind")

        if kind == "OP":
            op = Operation.from_json(msg["op"])
            was_new, _ = dst.on_receive(op)

            status = "NEW" if was_new else "SEEN"
            net.send(now, dst.id, src_id, MsgAck(kind="ACK", op_id=op.op_id, status=status).__dict__)
            dst.ops_sent += 1

            if was_new and op.op_id not in op_index:
                op_index[op.op_id] = op
            if was_new:
                dst.activate_rumor(op.op_id, budget=self.rumor_budget)

        elif kind == "ACK":
            if msg["status"] == "SEEN":
                op_id = msg["op_id"]
                dst.rumor_already_seen_hits[op_id] = dst.rumor_already_seen_hits.get(op_id, 0) + 1

        else:
            raise ValueError(f"RumorMongering: unknown message kind {kind}")
