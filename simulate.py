from __future__ import annotations

import argparse
import json
import os
import random
from typing import Dict, List

from model import Record, Operation, Timestamp
from replica import Replica
from network import Network

# metrics.py should contain residue() + residue_fast() as discussed
from metrics import residue, residue

from rumor import RumorMongering
from anti_entropy import AntiEntropy


def load_snapshot(path: str) -> Dict[str, Record]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {k: Record.from_json(v) for k, v in raw.items()}


def load_workload(path: str) -> List[Operation]:
    ops: List[Operation] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ops.append(Operation.from_json(json.loads(line)))
    return ops


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--snapshot", default="data/initial_snapshot.json")
    ap.add_argument("--workload", default="data/workload.jsonl")
    ap.add_argument("--outdir", default="out/run_001")
    ap.add_argument("--replicas", type=int, default=20)

    ap.add_argument("--algo", choices=["rumor", "anti_entropy"], default="rumor")

    ap.add_argument("--ticks", type=int, default=800)
    ap.add_argument("--inject_per_tick", type=int, default=4)

    ap.add_argument("--min_delay", type=int, default=1)
    ap.add_argument("--max_delay", type=int, default=5)
    ap.add_argument("--drop_rate", type=float, default=0.05)

    # how often to compute metrics (performance)
    ap.add_argument("--metrics_every", type=int, default=10)

    # rumor params
    ap.add_argument("--rumor_budget", type=int, default=30)
    ap.add_argument("--rumor_fanout", type=int, default=1)
    ap.add_argument("--rumor_stop_threshold", type=int, default=4)

    # anti-entropy params
    ap.add_argument("--anti_interval", type=int, default=25)
    ap.add_argument("--anti_sample", type=int, default=2000)

    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()

    ensure_dir(args.outdir)
    ensure_dir(os.path.join(args.outdir, "final_states"))

    # Save config for reproducibility
    with open(os.path.join(args.outdir, "config.json"), "w", encoding="utf-8") as f:
        json.dump(vars(args), f, ensure_ascii=False, indent=2)

    base_store = load_snapshot(args.snapshot)
    workload = load_workload(args.workload)

    rnd = random.Random(args.seed)

    # Build replicas (each starts from same snapshot)
    replicas: List[Replica] = []
    for i in range(args.replicas):
        rid = f"R{i}"
        store_copy = {k: Record(value=v.value, deleted=v.deleted, ts=v.ts) for k, v in base_store.items()}
        replicas.append(Replica(rid, store_copy, seed=args.seed * 1000 + i))

    replicas_by_id = {r.id: r for r in replicas}
    all_ids = [r.id for r in replicas]

    net = Network(seed=args.seed + 1, min_delay=args.min_delay, max_delay=args.max_delay, drop_rate=args.drop_rate)

    # Choose algorithm
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
    else:
        raise ValueError("Unsupported algo")

    # Needed by rumor; anti-entropy ignores it, but we keep same interface
    op_index: Dict[str, Operation] = {}

    metrics_path = os.path.join(args.outdir, "metrics.jsonl")
    with open(metrics_path, "w", encoding="utf-8") as mfile:
        inject_cursor = 0

        for tick in range(args.ticks):
            # 1) Inject local ops into their origin replica
            for _ in range(args.inject_per_tick):
                if inject_cursor >= len(workload):
                    break

                op = workload[inject_cursor]
                inject_cursor += 1

                origin = replicas_by_id.get(op.origin)
                if origin is None:
                    # if workload has IDs beyond our replica count, remap safely
                    origin = replicas[rnd.randrange(len(replicas))]
                    op = Operation(
                        op_id=f"{origin.id}:{op.ts.counter}",
                        op=op.op,
                        key=op.key,
                        value=op.value,
                        ts=Timestamp(op.ts.counter, origin.id),
                        origin=origin.id,
                    )

                was_new, _ = origin.on_receive(op)
                if was_new:
                    # for rumor
                    op_index[op.op_id] = op
                    if args.algo == "rumor":
                        origin.activate_rumor(op.op_id, budget=args.rumor_budget)

            # 2) Algorithm periodic step
            for r in replicas:
                algo.tick(tick, r, all_ids, net, op_index)

            # 3) Deliver messages ready
            ready = net.deliver_ready(tick)
            for msg in ready:
                dst = replicas_by_id[msg.dst]
                algo.handle_message(tick, dst, msg.payload, net, op_index, src_id=msg.src)

            # 4) Metrics (compute less often)
            if tick % args.metrics_every == 0:
                # fast approximate: 0 means likely converged; 1 means differences exist
                res_fast = residue([r.store for r in replicas])

                m = {
                    "tick": tick,
                    "residue": res_fast,
                    "msgs_sent": net.msgs_sent,
                    "msgs_dropped": net.msgs_dropped,
                    "ops_sent": sum(r.ops_sent for r in replicas),
                    "ops_received": sum(r.ops_received for r in replicas),
                }
                mfile.write(json.dumps(m, ensure_ascii=False) + "\n")

    # Save final states
    for r in replicas:
        out_state = {k: v.to_json() for k, v in r.store.items()}
        with open(os.path.join(args.outdir, "final_states", f"{r.id}.json"), "w", encoding="utf-8") as f:
            json.dump(out_state, f, ensure_ascii=False)

    # Exact residue at end (expensive, but only once)
    final_exact_residue = residue([r.store for r in replicas])

    summary = {
        "algo": args.algo,
        "replicas": args.replicas,
        "ticks": args.ticks,
        "workload_ops_total": len(workload),
        "workload_ops_injected": inject_cursor,
        "final_exact_residue": final_exact_residue,
        "network_msgs_sent": net.msgs_sent,
        "network_msgs_dropped": net.msgs_dropped,
        "replica_ops_sent_total": sum(r.ops_sent for r in replicas),
        "replica_ops_received_total": sum(r.ops_received for r in replicas),
        "replica_ops_applied_total": sum(r.ops_applied for r in replicas),
    }

    with open(os.path.join(args.outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Done.")
    print(f"Algo: {args.algo}")
    print(f"Metrics: {metrics_path}")
    print(f"Summary: {os.path.join(args.outdir, 'summary.json')}")
    print(f"Final exact residue: {final_exact_residue}")
    print(f"Final states: {os.path.join(args.outdir, 'final_states')}")


if __name__ == "__main__":
    main()
