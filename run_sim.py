# python
# file: run_sim.py
# Simple smoke test: import Dissemination and print a summary.

from dissemination import Dissemination

def main():
    d = Dissemination(seed=42, rumor_budget=10, rumor_fanout=2, anti_entropy_interval=10)
    print("Dissemination instance created")
    print("  seed-controlled random:", bool(d.rnd))
    print("  rumor_budget:", d.rumor_budget)
    print("  rumor_fanout:", d.rumor_fanout)
    print("  anti_entropy_interval:", d.anti_entropy_interval)

if __name__ == "__main__":
    main()