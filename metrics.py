from __future__ import annotations
from typing import Dict, List, Tuple
from model import Record


def residue(replicas: List[Dict[str, Record]]) -> int:
    """
    Counts keys whose records differ across replicas.
    We compare (deleted, value, ts) tuples.
    """
    if not replicas:
        return 0
    # union of keys
    keys = set()
    for st in replicas:
        keys.update(st.keys())

    diff = 0
    for k in keys:
        baseline = None
        same = True
        for st in replicas:
            r = st.get(k)
            tup = None if r is None else (r.deleted, r.value, (r.ts.counter, r.ts.replica_id))
            if baseline is None:
                baseline = tup
            else:
                if tup != baseline:
                    same = False
                    break
        if not same:
            diff += 1
    return diff
