#!/usr/bin/env python3
"""Build a first feasible, non-optimizing schedule from a GPU operation plan.

Example durations default to Compute=10 us, H2D/D2H=4 us. The
scheduler honors explicit dependencies, chunk versions, GPU capacity,
and one concurrent operation per execution lane.
"""
import argparse
import json
import sys
from pathlib import Path

LANES = {'Compute': 'Compute', 'H2D': 'H2D', 'D2H': 'D2H',
         'ALLOC': 'Memory', 'DROP': 'Memory'}


def build(plan, compute_us=10, transfer_us=4):
    operations = plan['operations']
    ops = {op['id']: op for op in operations}
    if len(ops) != len(operations):
        raise ValueError('Duplicate operation IDs')
    chunks = {c['id']: c for c in plan['chunks']}
    if len(chunks) != len(plan['chunks']):
        raise ValueError('Duplicate chunk IDs')
    for op in operations:
        if op['type'] not in LANES:
            raise ValueError(f"Unknown operation type: {op['type']}")
        for dep in op.get('depends_on', []):
            if dep not in ops:
                raise ValueError(f"{op['id']}: unknown dependency {dep}")

    cpu = dict(plan.get('initial_cpu_versions', {}))
    gpu = {}  # chunk -> version (None while uninitialized or loading)
    active_chunks = set()
    busy_lanes = {}  # lane -> operation ID
    running = {}  # ID -> end time
    done = set()
    scheduled = {}
    t = 0
    peak = 0
    used = 0

    def size(chunk):
        return chunks[chunk]['reserved_bytes']

    def reserve(chunk):
        nonlocal used, peak
        gpu[chunk] = None
        used += size(chunk)
        peak = max(peak, used)

    def ready(op):
        kind = op['type']
        if any(dep not in done for dep in op.get('depends_on', [])):
            return False
        if kind == 'ALLOC':
            c = op['chunk']
            return c not in gpu and used + size(c) <= plan['capacity_bytes']
        if kind == 'DROP':
            c = op['chunk']
            if c not in gpu or c in active_chunks:
                return False
            v = gpu[c]
            if v is not None and cpu.get(c) != v:
                # Do not discard a version required by a future Compute.
                for other in operations:
                    if other['id'] in done:
                        continue
                    if any(a['chunk'] == c and a['version'] == v
                           for a in other.get('reads', [])):
                        return False
            return True
        if kind == 'H2D':
            c = op['chunk']
            return (c not in gpu and cpu.get(c) == op['version']
                    and used + size(c) <= plan['capacity_bytes'])
        if kind == 'D2H':
            c = op['chunk']
            if gpu.get(c) != op['version'] or c in active_chunks:
                return False
            return True
        if kind == 'Compute':
            accesses = op.get('reads', []) + op.get('writes', [])
            if any(a['chunk'] not in gpu or a['chunk'] in active_chunks
                   for a in accesses):
                return False
            if any(gpu[a['chunk']] != a['version']
                   for a in op.get('reads', [])):
                return False
            return True
        return False

    def duration(op):
        if 'duration_us' in op:
            return op['duration_us']
        return compute_us if op['type'] == 'Compute' else transfer_us

    def start(op):
        oid, kind = op['id'], op['type']
        lane = LANES[kind]
        if kind == 'ALLOC':
            reserve(op['chunk'])
            scheduled[oid] = dict(id=oid, lane=lane, start_us=t, end_us=t)
            done.add(oid)
            return
        if kind == 'DROP':
            c = op['chunk']
            del gpu[c]
            scheduled[oid] = dict(id=oid, lane=lane, start_us=t, end_us=t)
            done.add(oid)
            return
        if kind == 'H2D':
            reserve(op['chunk'])
            active_chunks.add(op['chunk'])
        elif kind == 'D2H':
            active_chunks.add(op['chunk'])
        elif kind == 'Compute':
            for a in op.get('reads', []) + op.get('writes', []):
                active_chunks.add(a['chunk'])
        d = duration(op)
        if d <= 0:
            raise ValueError(f'{oid}: duration must be positive')
        busy_lanes[lane] = oid
        running[oid] = t + d
        scheduled[oid] = dict(id=oid, lane=lane, start_us=t, end_us=t+d)

    def finish(op):
        oid, kind = op['id'], op['type']
        if kind == 'H2D':
            gpu[op['chunk']] = op['version']
            active_chunks.remove(op['chunk'])
        elif kind == 'D2H':
            cpu[op['chunk']] = op['version']
            active_chunks.remove(op['chunk'])
        elif kind == 'Compute':
            for a in op.get('writes', []):
                gpu[a['chunk']] = a['version']
            for c in {a['chunk'] for a in op.get('reads', []) + op.get('writes', [])}:
                active_chunks.remove(c)
        del busy_lanes[LANES[kind]]
        del running[oid]
        done.add(oid)

    # Priority: complete Compute as early as possible; save dirty data;
    # then transfer new inputs. Memory operations are instantaneous.
    priority = {'DROP': 0, 'ALLOC': 1, 'Compute': 2, 'D2H': 3, 'H2D': 4}
    ordered = sorted(enumerate(operations), key=lambda pair: (priority[pair[1]['type']], pair[0]))
    while len(done) < len(operations):
        for oid in list(running):
            if running[oid] == t:
                finish(ops[oid])

        progressed = True
        while progressed:
            progressed = False
            for _, op in ordered:
                oid, kind = op['id'], op['type']
                if oid in scheduled:
                    continue
                if kind not in ('ALLOC', 'DROP') and LANES[kind] in busy_lanes:
                    continue
                if not ready(op):
                    continue
                if kind == 'DROP':
                    used -= size(op['chunk'])
                start(op)
                progressed = True
                break

        if len(done) == len(operations):
            break
        if not running:
            blocked = [op['id'] for op in operations if op['id'] not in scheduled]
            raise ValueError(f'No feasible next operation at {t} us; blocked: {blocked}')
        t = min(running.values())

    ordered_schedule = [scheduled[op['id']] for op in operations]
    makespan = max(e['end_us'] for e in ordered_schedule) if ordered_schedule else 0
    return {
        'schema_version': plan.get('schema_version', '0.1-draft'),
        'time_unit': 'us',
        'operations': ordered_schedule,
        'metrics': {
            'makespan_us': makespan,
            'peak_gpu_bytes': peak,
            'h2d_bytes': sum(chunks[o['chunk']]['size_bytes'] for o in operations if o['type'] == 'H2D'),
            'd2h_bytes': sum(chunks[o['chunk']]['size_bytes'] for o in operations if o['type'] == 'D2H'),
            'completed_jobs': sum(o['type'] == 'Compute' for o in operations),
        },
    }


def main():
    root = Path(__file__).resolve().parents[1]
    default_plan = root / 'docs' / 'scheduling' / 'example-plan.json'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, default=default_plan)
    parser.add_argument('--output', type=Path, default=root / 'docs' / 'scheduling' / 'generated-schedule.json')
    parser.add_argument('--compute-us', type=float, default=10)
    parser.add_argument('--transfer-us', type=float, default=4)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    schedule = build(plan, args.compute_us, args.transfer_us)
    schedule['plan'] = args.plan.name
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from validate_schedule import validate
    errors = validate(plan, schedule)
    if errors:
        raise SystemExit('Schedule invalid:\n' + '\n'.join(errors))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(schedule, indent=2, ensure_ascii=False) + '\n')
    print(f'Schedule validation OK; makespan = {schedule["metrics"]["makespan_us"]:g} us')
    print(f'Wrote {args.output}')


if __name__ == '__main__':
    main()
