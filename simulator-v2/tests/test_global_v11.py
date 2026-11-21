from random import Random
from unittest import TestCase

from nntile_sim.core.engine import Config
from nntile_sim.core.exact_small import exact_us
from nntile_sim.core import global_scheduler_v10, global_scheduler_v11
from nntile_sim.model import Access, Task


def make_graph(seed):
    rng = Random(seed)
    sizes = {h: rng.randint(1, 4) for h in 'ABCD'}
    cfg = Config(12, rng.choice([1, 2, 4]), rng.choice([1, 2, 4]),
                 chunk_size_bytes=4, compute_order='trace')
    tasks = []
    for i in range(6):
        accesses = tuple(Access(h, sizes[h], rng.choice(['R', 'RW', 'W']))
                         for h in rng.sample('ABCD', rng.randint(1, 3)))
        duration = rng.randint(1, 5)
        tasks.append(Task(str(i), 'op', (), accesses, i*8/1e6,
                          (i*8+duration)/1e6, i, ()))
    return tasks, cfg


class AsymmetricTransfersTest(TestCase):
    def test_six_counterexamples(self):
        for seed, expected_gain in ((12, 2), (22, 1), (70, 1), (79, 1), (107, 1), (121, .25)):
            with self.subTest(seed=seed):
                tasks, cfg = make_graph(seed)
                old = global_scheduler_v10.schedule(tasks, cfg)
                new = global_scheduler_v11.schedule(tasks, cfg)
                self.assertAlmostEqual(new.makespan_us, float(exact_us(tasks, cfg)))
                self.assertAlmostEqual(old.makespan_us - new.makespan_us, expected_gain)
