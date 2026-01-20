from __future__ import annotations
import argparse
import json
import os
import random
from typing import Any, List

from model import Timestamp, Record, Operation


def zipf_choice(n: int, s: float) -> int:
    """
    Returns an index in [0, n-1] with Zipf-like bias.
    Uses rejection sampling on precomputed weights (fast enough for demo sizes).
    """
    # Precompute weights once per run outside if you want speed; for simplicity keep local in generator usage.
    raise RuntimeError("zipf_choice should not be called directly; use ZipfSampler.")


class ZipfSampler:
    def __init__(self, n: int, s: float, seed: int):
        self.n = n
        rnd = random.Random(seed)
        weights = [1.0 / ((i + 1) ** s) for i in range(n)]
        total = sum(weights)
        probs = [w / total for w in weights]

        # Build CDF for binary search sampling
        cdf = []
        acc = 0.0
        for p in probs:
            acc += p
            cdf.append(acc)
        cdf[-1] = 1.0
        self.cdf = cdf
        self.rnd = rnd

    def sample_index(self) -> int:
        x = self.rnd.random()
        lo, hi = 0, self.n - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if x <= self.cdf[mid]:
                hi = mid
            else:
                lo = mid + 1
        return lo


def random_value(rnd: random.Random) -> Any:
    # mix ints and short strings
    if rnd.random() < 0.6:
        return rnd.randint(0, 10_000_000)
    else:
        letters = "abcdefghijklmnopqrstuvwxyz"
        return "".join(rnd.choice(letters) for _ in range(rnd.randint(4, 10)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data", help="output directory (default: data)")
    ap.add_argument("--replicas", type=int, default=20, help="number of replicas (default: 20)")
    ap.add_argument("--keys", type=int, default=30000, help="number of keys (default: 30000)")
    ap.add_argument("--ops", type=int, default=120000, help="number of operations (default: 120000)")
    ap.add_argument("--del_ratio", type=float, default=0.10, help="fraction of deletes (default: 0.10)")
    ap.add_argument("--zipf_s", type=float, default=1.1, help="Zipf skew (default: 1.1)")
    ap.add_argument("--seed", type=int, default=7, help="random seed (default: 7)")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    rnd = random.Random(args.seed)

    # Keys
    keys: List[str] = [f"k{i:06d}" for i in range(args.keys)]

    # Initial snapshot: give each key a value with ts=[0,"INIT"]
    snapshot = {}
    for k in keys:
        rec = Record(value=random_value(rnd), deleted=False, ts=Timestamp(0, "INIT"))
        snapshot[k] = rec.to_json()

    with open(os.path.join(args.outdir, "initial_snapshot.json"), "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)

    # Workload
    sampler = ZipfSampler(n=len(keys), s=args.zipf_s, seed=args.seed + 1)

    # Per-replica counter to generate unique op_id and timestamp
    counters = [0 for _ in range(args.replicas)]

    workload_path = os.path.join(args.outdir, "workload.jsonl")
    with open(workload_path, "w", encoding="utf-8") as f:
        for _ in range(args.ops):
            ridx = rnd.randrange(args.replicas)
            replica_id = f"R{ridx}"
            counters[ridx] += 1
            counter = counters[ridx]
            ts = Timestamp(counter, replica_id)

            key = keys[sampler.sample_index()]
            if rnd.random() < args.del_ratio:
                op = Operation(op_id=f"{replica_id}:{counter}", op="DEL", key=key, value=None, ts=ts, origin=replica_id)
            else:
                op = Operation(op_id=f"{replica_id}:{counter}", op="SET", key=key, value=random_value(rnd), ts=ts, origin=replica_id)

            f.write(json.dumps(op.to_json(), ensure_ascii=False) + "\n")

    print("Generated:")
    print(f" - {os.path.join(args.outdir, 'initial_snapshot.json')}")
    print(f" - {workload_path}")


if __name__ == "__main__":
    main()
