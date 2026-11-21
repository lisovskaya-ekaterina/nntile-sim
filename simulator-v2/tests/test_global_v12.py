from unittest import TestCase

from nntile_sim.core import global_scheduler_v11, global_scheduler_v12
from nntile_sim.core.engine import Config
from nntile_sim.core.exact_small import exact_us
from nntile_sim.model import Access, Task


class ReleaseOrderTest(TestCase):
    def test_equal_reservations_unequal_payloads(self):
        sizes = {'A': 2, 'B': 1, 'C': 3, 'D': 2}
        rows = [(3, 'BW CW DW'), (4, 'CR ARW'), (2, 'ARW BRW'),
                (1, 'DR CW AW'), (1, 'AW DR'), (1, 'CR DRW AW'), (3, 'AW')]
        tasks = []
        for i, (duration, modes) in enumerate(rows):
            accesses = tuple(Access(s[0], sizes[s[0]], s[1:]) for s in modes.split())
            tasks.append(Task(str(i), 'op', (), accesses, i*8/1e6,
                              (i*8+duration)/1e6, i, ()))
        cfg = Config(8, 1, 2, chunk_size_bytes=2, compute_order='trace')
        result = global_scheduler_v12.schedule(tasks, cfg)
        self.assertAlmostEqual(global_scheduler_v11.schedule(tasks, cfg).makespan_us, 18)
        self.assertAlmostEqual(float(exact_us(tasks, cfg)), 17.5)
        self.assertAlmostEqual(result.makespan_us, 17.5)
        saves = [e for e in result.events if e.lane == 'D2H']
        self.assertEqual([e.handle for e in saves], ['B:0', 'D:0'])
        reload = next(e for e in result.events if e.handle == 'B:0' and e.action == 'LOAD')
        reader = next(e for e in result.events if e.job_id == '1')
        self.assertGreaterEqual(reload.start_us, reader.start_us)
        self.assertLessEqual(reload.end_us, reader.end_us)
