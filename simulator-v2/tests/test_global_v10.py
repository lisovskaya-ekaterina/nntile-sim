from random import Random
from unittest import TestCase

from nntile_sim.core.engine import Config
from nntile_sim.core.exact_small import exact_us
from nntile_sim.core import global_scheduler_v9, global_scheduler_v10
from nntile_sim.model import Access, Task


class CounterexamplesTest(TestCase):
    def test_ten_residency_counterexamples(self):
        config = Config(2, 1, 1, chunk_size_bytes=1, compute_order='trace')
        for seed in (6, 14, 25, 36, 50, 58, 61, 69, 90, 93):
            with self.subTest(seed=seed):
                rng = Random(seed)
                tasks = []
                for i in range(5):
                    handles = rng.sample('ABC', rng.randint(1, 2))
                    accesses = tuple(Access(h, 1, rng.choice(['W', 'R', 'RW'])) for h in handles)
                    tasks.append(Task(str(i), 'op', (), accesses, i/1e6, (i+1)/1e6, i, ()))
                old = global_scheduler_v9.schedule(tasks, config)
                new = global_scheduler_v10.schedule(tasks, config)
                self.assertAlmostEqual(new.makespan_us, float(exact_us(tasks, config)))
                self.assertAlmostEqual(old.makespan_us - new.makespan_us, 1)
                if seed == 50:
                    self.assertGreater(new.d2h_bytes + new.h2d_bytes, old.d2h_bytes + old.h2d_bytes)
