from __future__ import annotations

import argparse
import json
import os
import random
from typing import Dict, List
from datetime import datetime

from model import Record, Operation, Timestamp
from replica import Replica
from network import Network
from direct_mail import DirectMail
from rumor import RumorMongering
from anti_entropy import AntiEntropy

from metrics import residue

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def make_database(keys: int) -> Dict[str, Record]:
    base: Dict[str, Record] = {}
    for i in range(keys):
        k = f"k{i}"
        base[k] = Record(value=0, deleted=False, ts=Timestamp(0, "R0"))
    return base


def make_updates(rnd: random.Random, replicas: int, keys: int, ops: int) -> List[Operation]:
    w: List[Operation] = []
    for i in range(ops):
        origin_id = f"R{rnd.randrange(replicas)}"
        key = f"k{rnd.randrange(keys)}"
        value = rnd.randrange(100)

        ts = Timestamp(i + 1, origin_id)
        w.append(
            Operation(
                op_id=f"{origin_id}:{i+1}",
                op="SET",
                key=key,
                value=value,
                ts=ts,
                origin=origin_id,
            )
        )
    return w


def main() -> None:

    ap = argparse.ArgumentParser()

    # small synthetic data
    ap.add_argument("--keys", type=int, default=30)
    ap.add_argument("--ops", type=int, default=200)

    # sim
    ap.add_argument("--replicas", type=int, default=10)
    ap.add_argument("--ticks", type=int, default=400)
    ap.add_argument("--inject_per_tick", type=int, default=1)

    # output
    ap.add_argument("--outdir", default="out/run_001")

    # network
    ap.add_argument("--drop_rate", type=float, default=0.02)

    # progress logging (rare, simple)
    ap.add_argument("--progress_every", type=int, default=10)

    # algo
    ap.add_argument("--algo", choices=["rumor", "anti_entropy", "direct_mail"], default="rumor")

    # rumor params
    ap.add_argument("--rumor_budget", type=int, default=30)
    ap.add_argument("--rumor_fanout", type=int, default=1)
    ap.add_argument("--rumor_stop_threshold", type=int, default=4)

    # anti-entropy params 
    ap.add_argument("--anti_interval", type=int, default=25)
    ap.add_argument("--anti_sample", type=int, default=2000)

    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()

    # auto outdir: algo + timestamp
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    args.outdir = os.path.join(args.outdir, f"{args.algo}_{run_id}")

    ensure_dir(args.outdir)

    rnd = random.Random(args.seed)

    # build data
    database = make_database(args.keys)
    updates = make_updates(rnd, args.replicas, args.keys, args.ops)

    # build replicas
    replicas: List[Replica] = []
    for i in range(args.replicas):
        rid = f"R{i}"
        store_copy = {k: Record(value=v.value, deleted=v.deleted, ts=v.ts) for k, v in database.items()}
        replicas.append(Replica(rid, store_copy, seed=args.seed * 1000 + i))

    replicas_by_id = {r.id: r for r in replicas}
    all_ids = [r.id for r in replicas]

    net = Network(seed=args.seed + 1, drop_rate=args.drop_rate)

    if args.algo == "rumor":
        algo = RumorMongering(
            seed=args.seed + 2,
            rumor_budget=args.rumor_budget,
            rumor_fanout=args.rumor_fanout,
            stop_threshold=args.rumor_stop_threshold,
        )
    elif args.algo == "anti_entropy":
        algo = AntiEntropy(
            seed=args.seed + 2,
            interval=args.anti_interval,
            sample_size=args.anti_sample,
        )
    elif args.algo == "direct_mail":
        algo = DirectMail(seed=args.seed + 2)
    else:
        raise ValueError("Unsupported algo")

    # keep interface compatibility (rumor needs it; others can ignore)
    op_index: Dict[str, Operation] = {}

    progress_path = os.path.join(args.outdir, "progress.jsonl")
    inject_cursor = 0

    with open(progress_path, "w", encoding="utf-8") as pfile:
        for tick in range(args.ticks):

            # 1) inject small number of ops
            for _ in range(args.inject_per_tick):
                if inject_cursor >= len(updates):
                    break

                op = updates[inject_cursor]
                inject_cursor += 1

                origin = replicas_by_id[op.origin]
                was_new, _changed = origin.on_receive(op)

                if was_new:
                    op_index[op.op_id] = op

                    if args.algo == "rumor":
                        origin.activate_rumor(op.op_id, budget=args.rumor_budget)

                    elif args.algo == "direct_mail":
                        # broadcast to all others
                        for dst_id in all_ids:
                            if dst_id != origin.id:
                                net.send(tick, origin.id, dst_id, {"kind": "OP", "op": op.to_json()})
                                origin.ops_sent += 1


            # 2) periodic step
            for r in replicas:
                algo.tick(tick, r, all_ids, net, op_index)

            # 3) deliver messages
            for msg in net.deliver_ready(tick):
                dst = replicas_by_id[msg.dst]
                algo.handle_message(tick, dst, msg.payload, net, op_index, src_id=msg.src)

            # 4) progress logging (rare)
            if args.progress_every > 0 and tick % args.progress_every == 0:
                div = residue([r.store for r in replicas])
                pfile.write(
                    json.dumps(
                        {
                            "tick": tick,
                            "residue": div,
                            "msgs_sent": net.msgs_sent,
                            "msgs_dropped": net.msgs_dropped,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    final_residue = residue([r.store for r in replicas])

    summary = {
        "algo": args.algo,
        "replicas": args.replicas,
        "ticks": args.ticks,
        "keys": args.keys,
        "ops_total": len(updates),
        "ops_injected": inject_cursor,
        "final_residue": final_residue,
        "network": {
            "msgs_sent": net.msgs_sent,
            "msgs_dropped": net.msgs_dropped,
            "drop_rate": args.drop_rate,
        },
    }

    with open(os.path.join(args.outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Done.")
    print(f"Algo: {args.algo}")
    print(f"Progress: {progress_path}")
    print(f"Summary: {os.path.join(args.outdir, 'summary.json')}")
    print(f"Final exact residue: {final_residue}")


if __name__ == "__main__":
    main()
