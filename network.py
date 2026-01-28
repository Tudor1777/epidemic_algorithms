from __future__ import annotations
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class InFlightMsg:
    deliver_at: int
    src: str
    dst: str
    payload: Any


class Network:
    def __init__(self, seed: int, drop_rate: float = 0.05):
        self.rnd = random.Random(seed)
        self.drop_rate = drop_rate
        self.queue: List[InFlightMsg] = []
        self.msgs_sent = 0
        self.msgs_dropped = 0

    def send(self, now: int, src: str, dst: str, payload: Any) -> None:
        self.msgs_sent += 1
        if self.rnd.random() < self.drop_rate:
            self.msgs_dropped += 1
            return
        self.queue.append(InFlightMsg(deliver_at=now, src=src, dst=dst, payload=payload))

    def deliver_ready(self, now: int) -> List[InFlightMsg]:
        ready = [m for m in self.queue if m.deliver_at <= now]
        if ready:
            # keep the rest
            self.queue = [m for m in self.queue if m.deliver_at > now]
        return ready
