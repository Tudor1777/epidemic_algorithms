from __future__ import annotations
import argparse
import json
import os
import time
import random
from typing import Dict, List

from model import Record, Operation, Timestamp
from replica import Replica
from network import Network
from dissemination import Dissemination
from metrics import residue


def load_snapshot(path: str) -> Dict[str, Record]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {k: Record.from_json(v) for k, v in raw.items()}


def load_workload(path: str) -> List[Operation]:
    ops = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ops.append(Operation.from_json(json.loads(line)))
    return ops


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default="data/initial_snapshot.json")
    ap.add_argument("--workload", default="data/workload.jsonl")
    ap.add_argument("--outdir", default="out/run_001")
    ap.add_argument("--replicas", type=int, default=20)

    ap.add_argument("--ticks", type=int, default=800)
    ap.add_argument("--inject_per_tick", type=int, default=4, help="how many ops to inject into the system per tick")

    ap.add_argument("--min_delay", type=int, default=1)
    ap.add_argument("--max_delay", type=int, default=5)
    ap.add_argument("--drop_rate", type=float, default=0.05)

    ap.add_argument("--rumor_budget", type=int, default=30)
    ap.add_argument("--rumor_fanout", type=int, default=1)
    ap.add_argument("--rumor_stop_threshold", type=int, default=4)

    ap.add_argument("--anti_entropy_interval", type=int, default=25)
    ap.add_argument("--anti_entropy_sample", type=int, default=2000)

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

    replicas: List[Replica] = []
    for i in range(args.replicas):
        rid = f"R{i}"
        # each replica starts with a deep-ish copy (records are objects; clone to avoid shared refs)
        store_copy = {k: Record(value=v.value, deleted=v.deleted, ts=v.ts) for k, v in base_store.items()}
        replicas.append(Replica(rid, store_copy, seed=args.seed * 1000 + i))

    replicas_by_id = {r.id: r for r in replicas}
    all_ids = [r.id for r in replicas]

    net = Network(seed=args.seed + 1, min_delay=args.min_delay, max_delay=args.max_delay, drop_rate=args.drop_rate)
    dis = Dissemination(
        seed=args.seed + 2,
        rumor_budget=args.rumor_budget,
        rumor_fanout=args.rumor_fanout,
        rumor_stop_threshold=args.rumor_stop_threshold,
        anti_entropy_interval=args.anti_entropy_interval,
        anti_entropy_sample=args.anti_entropy_sample,
    )

    # op_index is needed so rumor push can re-send op payloads
    op_index: Dict[str, Operation] = {}

    metrics_path = os.path.join(args.outdir, "metrics.jsonl")
    mfile = open(metrics_path, "w", encoding="utf-8")

    inject_cursor = 0
    converged_at = None

    for tick in range(args.ticks):
        # 1) inject new ops to random replicas
        for _ in range(args.inject_per_tick):
            if inject_cursor >= len(workload):
                break
            op = workload[inject_cursor]
            inject_cursor += 1

            # deliver operation directly to its origin replica (local update)
            origin = replicas_by_id.get(op.origin)
            if origin is None:
                # if workload has R ids beyond replicas count, remap
                origin = replicas[rnd.randrange(len(replicas))]
                op = Operation(
                    op_id=f"{origin.id}:{op.ts.counter}",
                    op=op.op,
                    key=op.key,
                    value=op.value,
                    ts=Timestamp(op.ts.counter, origin.id),
                    origin=origin.id
                )

            was_new, _ = origin.on_receive(op)
            if was_new:
                op_index[op.op_id] = op
                origin.activate_rumor(op.op_id, budget=args.rumor_budget)

        # 2) rumor dissemination tick
        for r in replicas:
            dis.rumor_tick(tick, r, all_ids, net, op_index)

        # 3) anti-entropy periodic
        for r in replicas:
            dis.anti_entropy_tick(tick, r, all_ids, net)

        # 4) deliver messages that arrived
        ready = net.deliver_ready(tick)
        for msg in ready:
            dst = replicas_by_id[msg.dst]
            dis.handle_message(tick, dst, msg.payload, net, op_index, replicas_by_id, src_id=msg.src)

        # 5) metrics
        res = residue([r.store for r in replicas])
        msgs_sent = net.msgs_sent
        msgs_dropped = net.msgs_dropped
        ops_sent = sum(r.ops_sent for r in replicas)

        m = {"tick": tick, "residue": res, "msgs_sent": msgs_sent, "msgs_dropped": msgs_dropped, "ops_sent": ops_sent}
        mfile.write(json.dumps(m, ensure_ascii=False) + "\n")

        # convergence detection (strict)
        if res == 0 and converged_at is None and inject_cursor >= len(workload):
            converged_at = tick

    mfile.close()

    # save final states
    for r in replicas:
        out_state = {k: v.to_json() for k, v in r.store.items()}
        with open(os.path.join(args.outdir, "final_states", f"{r.id}.json"), "w", encoding="utf-8") as f:
            json.dump(out_state, f, ensure_ascii=False)

    summary = {
        "replicas": args.replicas,
        "ticks": args.ticks,
        "workload_ops_total": len(workload),
        "workload_ops_injected": inject_cursor,
        "converged_at_tick": converged_at,
        "network_msgs_sent": net.msgs_sent,
        "network_msgs_dropped": net.msgs_dropped,
        "replica_ops_sent_total": sum(r.ops_sent for r in replicas),
        "replica_ops_received_total": sum(r.ops_received for r in replicas),
        "replica_ops_applied_total": sum(r.ops_applied for r in replicas),
    }

    with open(os.path.join(args.outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Done.")
    print(f"Metrics: {metrics_path}")
    print(f"Summary: {os.path.join(args.outdir, 'summary.json')}")
    print(f"Final states: {os.path.join(args.outdir, 'final_states')}")


if __name__ == "__main__":
    main()
