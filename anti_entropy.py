from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List
import random

from model import Operation, Timestamp, Record
from replica import Replica
from network import Network


@dataclass
class MsgDigest:
    kind: str
    items: list


@dataclass
class MsgRecords:
    kind: str
    items: list


class AntiEntropy:
    def __init__(self, seed: int, interval: int = 25, sample_size: int = 2000):
        self.rnd = random.Random(seed)
        self.interval = interval
        self.sample_size = sample_size

    def tick(self, now: int, r: Replica, all_ids: List[str], net: Network, op_index: Dict[str, Operation]) -> None:
        if self.interval <= 0 or (now % self.interval) != 0:
            return

        peers = [x for x in all_ids if x != r.id]
        if not peers:
            return

        dst = r.pick_peer(peers)
        keys = list(r.store.keys())
        if not keys:
            return

        if len(keys) <= self.sample_size:
            sample = keys
        else:
            sample = self.rnd.sample(keys, self.sample_size)

        items = [(k, r.store[k].ts.to_json()) for k in sample]
        net.send(now, r.id, dst, MsgDigest(kind="DIGEST", items=items).__dict__)
        r.ops_sent += 1

    def handle_message(
        self,
        now: int,
        dst: Replica,
        msg: dict,
        net: Network,
        op_index: Dict[str, Operation],
        src_id: str
    ) -> None:
        kind = msg.get("kind")

        if kind == "DIGEST":
            resp_items = []
            for key, ts_json in msg["items"]:
                their_ts = Timestamp.from_json(ts_json)
                ours = dst.store.get(key)
                if ours is not None and ours.ts > their_ts:
                    resp_items.append((key, ours.to_json()))

            net.send(now, dst.id, src_id, MsgRecords(kind="RECORDS", items=resp_items).__dict__)
            dst.ops_sent += 1

        elif kind == "RECORDS":
            for key, rec_json in msg["items"]:
                rec = Record.from_json(rec_json)
                cur = dst.store.get(key)
                if cur is None or rec.ts > cur.ts:
                    dst.store[key] = rec
                    dst.ops_applied += 1

        else:
            raise ValueError(f"AntiEntropy: unknown message kind {kind}")
