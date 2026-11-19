"""Independent event-timeline checks for nntile_sim.core.global_scheduler.

Usage: PYTHONPATH=simulator-v2/src python /path/to/validate_global_schedule.py
"""
from collections import defaultdict
from math import isclose

from nntile_sim.parser import parse_tasks
from nntile_sim.core.engine import Config, order_tasks
from nntile_sim.core.global_scheduler import schedule
from nntile_sim.chunks import split_into_chunks

TRACE = 'examples/tasks_mbs1.rec'
CAPACITY = 768 * 1024 * 1024
CHUNK = 768 * 1024
EPS = 1e-6


def validate(tasks, cfg, result):
    errors = []
    def check(condition, message):
        if not condition and len(errors) < 25:
            errors.append(message)

    ordered = order_tasks(tasks, cfg.compute_order, cfg.custom_order)
    by_id = {t.job_id: t for t in ordered}
    sizes = {}
    first_read = {}
    for t in ordered:
        for a in t.accesses:
            sizes.setdefault(a.handle, a.size_bytes)
            check(sizes[a.handle] == a.size_bytes, f'inconsistent size: {a.handle}')
            first_read.setdefault(a.handle, a.reads_previous_value)
    overrides = dict(cfg.chunk_sizes_by_handle or ())
    chunks_by_handle = {h: split_into_chunks(h, size, overrides.get(h, cfg.chunk_size_bytes or size)) for h, size in sizes.items()}
    chunks = {f'{c.handle}:{c.index}': c for group in chunks_by_handle.values() for c in group}
    required = {t.job_id: {f'{c.handle}:{c.index}' for a in t.accesses for c in chunks_by_handle[a.handle]} for t in ordered}
    cpu = {f'{c.handle}:{c.index}' for h, group in chunks_by_handle.items() if first_read[h] for c in group}
    gpu = set()
    ready = set()
    dirty = set()
    in_h2d = set()
    in_d2h = set()
    pinned = set()
    used = peak = 0
    jobs_started = set()
    jobs_done = set()
    job_starts = {}
    task_index = {t.job_id: i for i, t in enumerate(ordered)}
    last_read = {}
    for i, t in enumerate(ordered):
        for a in t.accesses:
            if a.reads_previous_value:
                last_read[a.handle] = i
    last_started_index = -1
    lane_intervals = defaultdict(list)
    actions = []
    for e in result.events:
        check(e.end_us + EPS >= e.start_us >= -EPS, f'invalid event time: {e}')
        if e.lane in ('Compute', 'H2D', 'D2H'):
            lane_intervals[e.lane].append((e.start_us, e.end_us, e))
        if e.lane == 'Compute':
            actions.extend([(e.start_us, 5, 'compute_start', e), (e.end_us, 0, 'compute_end', e)])
        elif e.lane == 'H2D':
            actions.extend([(e.start_us, 4, 'h2d_start', e), (e.end_us, 0, 'h2d_end', e)])
        elif e.lane == 'D2H':
            actions.extend([(e.start_us, 4, 'd2h_start', e), (e.end_us, 0, 'd2h_end', e)])
        elif e.action == 'DROP':
            actions.append((e.start_us, 2, 'drop', e))
        elif e.action == 'ALLOC':
            actions.append((e.start_us, 3, 'alloc', e))
        else:
            check(False, f'unknown event: {e}')

    for lane, intervals in lane_intervals.items():
        intervals.sort(key=lambda v: (v[0], v[1]))
        for prev, cur in zip(intervals, intervals[1:]):
            check(cur[0] + EPS >= prev[1], f'{lane} lane overlap: {prev[2]} / {cur[2]}')

    # At equal timestamps, completed transfers/compute release locks first.
    # DROP precedes ALLOC; H2D reserves memory at transfer start.
    for time, _, action, e in sorted(actions, key=lambda x: (x[0], x[1])):
        key = e.handle
        if action == 'compute_end':
            t = by_id.get(e.job_id)
            if t:
                for a in t.accesses:
                    if a.writes_value:
                        for c in chunks_by_handle[a.handle]:
                            ck = f'{c.handle}:{c.index}'
                            check(ck in gpu, f'write output absent at {time}: {ck}')
                            dirty.add(ck)
                            cpu.discard(ck)
                pinned.difference_update(required[t.job_id])
                jobs_done.add(t.job_id)
        elif action == 'h2d_end':
            check(key in in_h2d, f'H2D completion without start: {key}')
            in_h2d.discard(key)
            ready.add(key)
        elif action == 'd2h_start':
            check(key in gpu, f'D2H of nonresident chunk at {time}: {key}')
            check(key in ready, f'D2H of unready chunk at {time}: {key}')
            check(key not in pinned, f'D2H of pinned chunk at {time}: {key}')
            check(key not in in_d2h, f'Duplicate D2H at {time}: {key}')
            in_d2h.add(key)
        elif action == 'd2h_end':
            check(key in in_d2h, f'D2H completion without start: {key}')
            in_d2h.discard(key)
            cpu.add(key)
            dirty.discard(key)
        elif action == 'drop':
            check(key in gpu, f'DROP of nonresident chunk at {time}: {key}')
            check(key not in pinned, f'DROP pinned chunk at {time}: {key}')
            check(key not in in_h2d | in_d2h, f'DROP during transfer at {time}: {key}')
            # Dirty chunks with no future read can be discarded; check future reads below.
            if key in dirty:
                future_reads = last_read.get(chunks[key].handle, -1) > last_started_index
                check(not future_reads, f'dirty chunk discarded before future read at {time}: {key}')
            if key in gpu:
                gpu.remove(key)
                ready.discard(key)
                dirty.discard(key)
                used -= chunks[key].reserved_bytes
        elif action in ('alloc', 'h2d_start'):
            check(key in chunks, f'unknown chunk: {key}')
            if key not in chunks:
                continue
            check(key not in gpu, f'double allocation at {time}: {key}')
            if action == 'h2d_start':
                check(key in cpu, f'H2D without valid CPU data at {time}: {key}')
                in_h2d.add(key)
            else:
                ready.add(key)
            gpu.add(key)
            used += chunks[key].reserved_bytes
            peak = max(peak, used)
            check(used <= cfg.capacity_bytes, f'GPU capacity exceeded at {time}: {used} > {cfg.capacity_bytes}')
        elif action == 'compute_start':
            t = by_id.get(e.job_id)
            check(t is not None, f'unknown job: {e.job_id}')
            if not t:
                continue
            check(t.job_id not in jobs_started, f'duplicate Compute: {t.job_id}')
            check(all(dep not in by_id or dep in jobs_done for dep in t.depends_on), f'dependency violation: {t.job_id}')
            for ck in required[t.job_id]:
                check(ck in gpu and ck in ready, f'Compute {t.job_id} missing/not ready chunk at {time}: {ck}')
                check(ck not in in_d2h, f'Compute {t.job_id} races D2H: {ck}')
            for a in t.accesses:
                if a.reads_previous_value:
                    for c in chunks_by_handle[a.handle]:
                        ck = f'{c.handle}:{c.index}'
                        check(ck in dirty or ck in cpu, f'Compute {t.job_id} reads unavailable value: {ck}')
            pinned.update(required[t.job_id])
            jobs_started.add(t.job_id)
            last_started_index = max(last_started_index, task_index[t.job_id])
            job_starts[t.job_id] = time

    check(len(jobs_done) == len(tasks), f'completed jobs: {len(jobs_done)} != {len(tasks)}')
    check(result.completed_jobs == len(jobs_done), 'completed_jobs metric mismatch')
    check(result.peak_bytes == peak, f'peak_bytes metric mismatch: {result.peak_bytes} != {peak}')
    max_end = max((e.end_us for e in result.events), default=0)
    check(isclose(result.makespan_us, max_end, abs_tol=EPS), f'makespan metric mismatch: {result.makespan_us} != {max_end}')
    check(result.h2d_bytes == sum(chunks[e.handle].size_bytes for e in result.events if e.lane == 'H2D'), 'H2D bytes mismatch')
    check(result.d2h_bytes == sum(chunks[e.handle].size_bytes for e in result.events if e.lane == 'D2H'), 'D2H bytes mismatch')
    return errors, peak


def main():
    tasks = parse_tasks(TRACE)
    cfg = Config(capacity_bytes=CAPACITY, h2d_bytes_per_us=12000,
                 d2h_bytes_per_us=12000, chunk_size_bytes=CHUNK,
                 memory_policy='belady-demand')
    print('Building schedule...', flush=True)
    result = schedule(tasks, cfg)
    print('Validating event timeline...', flush=True)
    errors, peak = validate(tasks, cfg, result)
    if errors:
        print(f'FAIL: {len(errors)} issue(s) shown (maximum 25):')
        for error in errors:
            print(' -', error)
        raise SystemExit(1)
    print(f'PASS: {len(tasks)} tasks; peak {peak / 1024**2:.2f} MiB; makespan {result.makespan_us / 1e6:.3f} s')


if __name__ == '__main__':
    main()
