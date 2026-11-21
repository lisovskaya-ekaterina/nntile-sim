from unittest import TestCase

from nntile_sim.core import global_scheduler_v7 as v7, global_scheduler_v8 as v8
from nntile_sim.core.engine import Config
from nntile_sim.core.exact_small import exact_us
from nntile_sim.model import Task, Access


class BoundsTest(TestCase):
    config = Config(2, 1, 1, chunk_size_bytes=1, compute_order='trace')

    def check_case(self, rows, expected):
        tasks = [Task(str(i), 'op', (), tuple(Access(h, 1, m) for h, m in row),
                      i / 1e6, (i+1) / 1e6, i, ()) for i, row in enumerate(rows)]
        times = (v8.lower_bound_us(tasks, self.config), float(exact_us(tasks, self.config)),
                 v7.schedule(tasks, self.config).makespan_us,
                 v8.schedule(tasks, self.config).makespan_us)
        for measured, reference in zip(times, expected):
            self.assertAlmostEqual(measured, reference)

    def test_compulsory_first_load(self):
        self.check_case([[('a', 'R')]], (2, 2, 2, 2))

    def test_eviction_choice(self):
        rows = [[('C', 'W'), ('A', 'R')], [('A', 'R')], [('C', 'W'), ('B', 'RW')],
                [('A', 'RW')], [('C', 'RW')]]
        self.check_case(rows, (6, 7, 10, 7))

    def test_remaining_gap(self):
        rows = [[('C', 'W')], [('A', 'R'), ('B', 'R')], [('A', 'R'), ('C', 'W')],
                [('B', 'RW'), ('A', 'R')], [('C', 'RW'), ('A', 'W')]]
        self.check_case(rows, (6, 9, 12, 10))
