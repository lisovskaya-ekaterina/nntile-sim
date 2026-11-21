"""Run from nntile-sim with PYTHONPATH=simulator-v2/src."""
import argparse
from nntile_sim.core.exact_small import exact_us
from check_d2h_versions import check_versions
from validate_global_schedule import CAPACITY, CHUNK, TRACE, validate
from nntile_sim.parser import parse_tasks
from nntile_sim.core.engine import Config
from nntile_sim.core import global_scheduler_v9
from nntile_sim.core.global_scheduler_v8 import compute_gaps, lower_bound_us


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", nargs="?", default=TRACE)
    parser.add_argument("--exact", action="store_true", help="exhaustive search on tiny traces")
    args = parser.parse_args()
    tasks = parse_tasks(args.trace)
    cfg = Config(CAPACITY, 12000, 12000, chunk_size_bytes=CHUNK)
    planner = global_scheduler_v9
    result = planner.schedule(tasks, cfg)
    bound = lower_bound_us(tasks, cfg)
    print(f"Lower bound {bound / 1e6:.6f} s, gap {max(0.0, result.makespan_us-bound)/1000:.6f} ms")
    for job, idle, blockers in compute_gaps(tasks, result):
        print(f"Compute {job} waited {idle:.3f} us, simultaneous completions: {blockers}")
    if args.exact:
        print(f"Exact optimum {float(exact_us(tasks, cfg)):.6f} us")
    errors, _ = validate(tasks, cfg, result)
    checked, conflicts = check_versions(tasks, cfg, result)
    if errors or conflicts:
        raise RuntimeError((errors + conflicts)[:5])
    busy = sum(t.duration_us for t in tasks)
    idle_us = max(0.0, result.makespan_us - busy)
    print(f'Compute idle {idle_us / 1000:.6f} ms')
    print(planner.__name__, len(tasks), f'{result.makespan_us / 1e6:.6f} s',
          f'Compute {100 * busy / result.makespan_us:.4f}%',
          f'peak {result.peak_bytes / 2**20:.2f} MiB',
          f'H2D/D2H {result.h2d_bytes / 2**20:.2f}/{result.d2h_bytes / 2**20:.2f} MiB',
          f'{checked} D2H checked, PASS', flush=True)

if __name__ == '__main__':
    main()
