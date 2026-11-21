from unittest import TestCase

from nntile_sim.core.engine import Config
from nntile_sim.core.exact_small import exact_us
from nntile_sim.core import global_scheduler_v8, global_scheduler_v9
from nntile_sim.model import Task, Access


class CancelPrefetchTest(TestCase):
    def test_eviction_before_first_use(self):
        rows = [[('C','W')], [('A','R'),('B','R')], [('A','R'),('C','W')],
                [('B','RW'),('A','R')], [('C','RW'),('A','W')]]
        tasks = [Task(str(i), 'op', (), tuple(Access(h, 1, m) for h, m in row),
                      i/1e6, (i+1)/1e6, i, ()) for i, row in enumerate(rows)]
        cfg = Config(2, 1, 1, chunk_size_bytes=1, compute_order='trace')
        old = global_scheduler_v8.schedule(tasks, cfg)
        new = global_scheduler_v9.schedule(tasks, cfg)
        self.assertAlmostEqual(old.makespan_us, 10)
        self.assertAlmostEqual(new.makespan_us, 9)
        self.assertAlmostEqual(new.makespan_us, float(exact_us(tasks, cfg)))
        self.assertEqual(new.h2d_bytes, old.h2d_bytes - 1)
