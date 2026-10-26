import argparse
from collections import Counter
from .parser import parse_tasks

def main() -> None:
    p = argparse.ArgumentParser(description="Inspect completed NNTile compute tasks")
    p.add_argument("trace")
    args = p.parse_args()
    tasks = parse_tasks(args.trace)
    print(f"Compute tasks: {len(tasks):,}")
    print(f"Unique handles: {len({a.handle for t in tasks for a in t.accesses}):,}")
    print("Iteration groups:")
    for group, n in sorted(Counter(t.iteration for t in tasks).items(), key=lambda x: (x[0] is None, x[0] or ())):
        print(f"  {group}: {n:,}")
    print("Access modes:", dict(Counter(a.mode for t in tasks for a in t.accesses)))

if __name__ == "__main__":
    main()
