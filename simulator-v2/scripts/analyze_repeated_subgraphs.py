"""Descriptive analysis of trace iteration groups; does not assume they are identical or sequential."""
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from nntile_sim.parser import parse_tasks


def groups_for(tasks):
    groups = defaultdict(list)
    for task in tasks:
        if task.iteration is not None:
            groups[tuple(task.iteration)].append(task)
    return dict(sorted(groups.items()))


def shared_handles(tasks):
    membership = defaultdict(set)
    for iteration, group in groups_for(tasks).items():
        for task in group:
            for access in task.accesses:
                membership[access.handle].add(iteration)
    return {handle for handle, iterations in membership.items() if len(iterations) > 1}


def analyze(tasks):
    groups = groups_for(tasks)
    membership = defaultdict(set)
    size = {}
    accesses = Counter()
    for iteration, group in groups.items():
        for task in group:
            for a in task.accesses:
                membership[a.handle].add(iteration)
                size[a.handle] = a.size_bytes
                accesses[a.handle] += 1
    summary = []
    for iteration, group in groups.items():
        handles = {a.handle for t in group for a in t.accesses}
        names = Counter(t.name for t in group)
        summary.append({'iteration': list(iteration), 'tasks': len(group),
                        'handles': len(handles), 'unique_to_group': sum(len(membership[h]) == 1 for h in handles),
                        'shared_with_other_groups': sum(len(membership[h]) > 1 for h in handles),
                        'task_names': dict(sorted(names.items())),
                        'start_min': min(t.start_time for t in group),
                        'start_max': max(t.start_time for t in group)})
    histogram = Counter(len(v) for v in membership.values())
    shared = sorted((h for h in membership if len(membership[h]) > 1),
                    key=lambda h: (-len(membership[h]), -accesses[h], -size[h], h))
    return {'groups': summary, 'tasks_total': len(tasks),
            'tasks_without_iteration': sum(t.iteration is None for t in tasks),
            'handles_in_iteration_groups': len(membership),
            'membership_group_count_histogram': {str(k): v for k, v in sorted(histogram.items())},
            'shared_handles': [{'handle': h, 'size_bytes': size[h], 'accesses': accesses[h],
                                'iterations': [list(x) for x in sorted(membership[h])]} for h in shared],
            'note': 'Groups use Iteration metadata, not inferred graph isomorphism or execution boundaries.'}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('trace', type=Path)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    result = analyze(parse_tasks(args.trace))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(f'groups={len(result["groups"])} shared_handles={len(result["shared_handles"])} -> {args.output}')

if __name__ == '__main__':
    main()
