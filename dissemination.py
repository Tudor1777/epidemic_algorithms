from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Any, Tuple, Optional
import random

from model import Operation, Timestamp, Record
from replica import Replica
from network import Network


@dataclass
class MsgOp:
    kind: str  # "OP"
    op: dict   # Operation json


@dataclass
class MsgAck:
    kind: str  # "ACK"
    op_id: str
    status: str  # "NEW" or "SEEN"


@dataclass
class MsgDigest:
    kind: str  # "DIGEST"
    # list of (key, ts_json)
    items: list
    sample_size: int


@dataclass
class MsgRecords:
    kind: str  # "RECORDS"
    # list of (key, record_json)
    items: list


class Dissemination:
    def __init__(
        self,
        seed: int,
        rumor_budget: int = 30,
        rumor_fanout: int = 1,
        rumor_stop_threshold: int = 4,
        anti_entropy_interval: int = 25,
        anti_entropy_sample: int = 2000,
    ):
        self.rnd = random.Random(seed)
        self.rumor_budget = rumor_budget
        self.rumor_fanout = rumor_fanout
        self.rumor_stop_threshold = rumor_stop_threshold
        self.anti_entropy_interval = anti_entropy_interval
        self.anti_entropy_sample = anti_entropy_sample

    def rumor_tick(self, now: int, r: Replica, all_replica_ids: List[str], net: Network, op_index: Dict[str, Operation]) -> None:
        peers = [x for x in all_replica_ids if x != r.id]
        if not peers:
            return

        # iterate over a copy because we may delete
        for op_id in list(r.active_rumors.keys()):
            budget = r.active_rumors[op_id]
            if budget <= 0:
                r.active_rumors.pop(op_id, None)
                r.rumor_already_seen_hits.pop(op_id, None)
                continue

            op = op_index.get(op_id)
            if op is None:
                # shouldn't happen
                r.active_rumors.pop(op_id, None)
                r.rumor_already_seen_hits.pop(op_id, None)
                continue

            # push to fanout peers
            for _ in range(self.rumor_fanout):
                dst = r.pick_peer(peers)
                net.send(now, r.id, dst, MsgOp(kind="OP", op=op.to_json()).__dict__)
                r.ops_sent += 1

            r.active_rumors[op_id] = budget - 1

            # stop early if many peers said "SEEN"
            if r.rumor_already_seen_hits.get(op_id, 0) >= self.rumor_stop_threshold:
                r.active_rumors.pop(op_id, None)
                r.rumor_already_seen_hits.pop(op_id, None)

    def anti_entropy_tick(self, now: int, r: Replica, all_replica_ids: List[str], net: Network) -> None:
        if self.anti_entropy_interval <= 0:
            return
        if now % self.anti_entropy_interval != 0:
            return

        peers = [x for x in all_replica_ids if x != r.id]
        if not peers:
            return
        dst = r.pick_peer(peers)

        # sample keys to keep it cheap
        keys = list(r.store.keys())
        if not keys:
            return
        sample = keys if len(keys) <= self.anti_entropy_sample else self.rnd.sample(keys, self.anti_entropy_sample)

        items = [(k, r.store[k].ts.to_json()) for k in sample]
        net.send(now, r.id, dst, MsgDigest(kind="DIGEST", items=items, sample_size=self.anti_entropy_sample).__dict__)
        r.ops_sent += 1

    def handle_message(
        self,
        now: int,
        dst_replica: Replica,
        msg: dict,
        net: Network,
        op_index: Dict[str, Operation],
        replicas_by_id: Dict[str, Replica],
        src_id: str,
    ) -> None:
        kind = msg.get("kind")

        if kind == "OP":
            op = Operation.from_json(msg["op"])
            was_new, changed = dst_replica.on_receive(op)

            # respond with ACK so sender can stop rumors earlier
            status = "NEW" if was_new else "SEEN"
            net.send(now, dst_replica.id, src_id, MsgAck(kind="ACK", op_id=op.op_id, status=status).__dict__)
            dst_replica.ops_sent += 1

            # store op in index if new (for later rumor push)
            if was_new and op.op_id not in op_index:
                op_index[op.op_id] = op

            # if it was new, activate rumor at receiver too
            if was_new:
                dst_replica.activate_rumor(op.op_id, budget=self.rumor_budget)

        elif kind == "ACK":
            op_id = msg["op_id"]
            status = msg["status"]
            if status == "SEEN":
                dst_replica.rumor_already_seen_hits[op_id] = dst_replica.rumor_already_seen_hits.get(op_id, 0) + 1

        elif kind == "DIGEST":
            # sender gives (key, ts). we respond with newer records for those keys.
            items = msg["items"]
            resp_items = []
            for key, ts_json in items:
                their_ts = Timestamp.from_json(ts_json)
                ours = dst_replica.store.get(key)
                if ours is not None and ours.ts > their_ts:
                    resp_items.append((key, ours.to_json()))
            # also add our own sample digest effect by piggybacking a small random subset
            net.send(now, dst_replica.id, src_id, MsgRecords(kind="RECORDS", items=resp_items).__dict__)
            dst_replica.ops_sent += 1

        elif kind == "RECORDS":
            # Apply newer records (convert to SET/DEL style op for uniformity is possible; here apply directly via synthetic op)
            for key, rec_json in msg["items"]:
                rec = Record.from_json(rec_json)
                cur = dst_replica.store.get(key)
                if cur is None or rec.ts > cur.ts:
                    dst_replica.store[key] = rec
                    dst_replica.ops_applied += 1

        else:
            raise ValueError(f"Unknown message kind: {kind}")
