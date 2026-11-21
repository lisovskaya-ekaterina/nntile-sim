"""Regression and exact-optimum tests for the current global planner."""

from random import Random
from unittest import TestCase

from nntile_sim.core.engine import Config
from nntile_sim.core.exact_small import exact_us
from nntile_sim.core.global_planner import schedule
from nntile_sim.model import Access, Task


def make_tasks(rows, sizes=None, durations=None):
    sizes = sizes or {}
    durations = durations or [1] * len(rows)
    tasks = []
    for i, row in enumerate(rows):
        accesses = tuple(
            Access(handle, sizes.get(handle, 1), mode)
            for handle, mode in row
        )
        tasks.append(Task(
            str(i), "op", (), accesses,
            i * 8 / 1e6,
            (i * 8 + durations[i]) / 1e6,
            i, (),
        ))
    return tasks


class GlobalPlannerExactTests(TestCase):

    def assert_optimal(self, tasks, cfg):
        actual = schedule(tasks, cfg)
        optimum = float(exact_us(tasks, cfg))
        self.assertAlmostEqual(actual.makespan_us, optimum)
        self.assertEqual(actual.completed_jobs, len(tasks))
        self.assertLessEqual(actual.peak_bytes, cfg.capacity_bytes)
        return actual

    def test_basic_scenarios(self):
        cfg = Config(2, 1, 1, chunk_size_bytes=1, compute_order="trace")
        cases = [
            [[("a", "R")]],
            [
                [("C", "W"), ("A", "R")],
                [("A", "R")],
                [("C", "W"), ("B", "RW")],
                [("A", "RW")],
                [("C", "RW")],
            ],
            [
                [("C", "W")],
                [("A", "R"), ("B", "R")],
                [("A", "R"), ("C", "W")],
                [("B", "RW"), ("A", "R")],
                [("C", "RW"), ("A", "W")],
            ],
        ]
        for i, rows in enumerate(cases):
            with self.subTest(case=i):
                self.assert_optimal(make_tasks(rows), cfg)

    def test_prefetch_cleanup(self):
        cfg = Config(2, 1, 1, chunk_size_bytes=1, compute_order="trace")
        rows = [
            [("C", "W")],
            [("A", "R"), ("B", "R")],
            [("A", "R"), ("C", "W")],
            [("B", "RW"), ("A", "R")],
            [("C", "RW"), ("A", "W")],
        ]
        result = self.assert_optimal(make_tasks(rows), cfg)
        self.assertAlmostEqual(result.makespan_us, 9)

    def test_residency_counterexamples(self):
        cfg = Config(2, 1, 1, chunk_size_bytes=1, compute_order="trace")
        for seed in (6, 14, 25, 36, 50, 58, 61, 69, 90, 93):
            with self.subTest(seed=seed):
                rng = Random(seed)
                rows = []
                for _ in range(5):
                    handles = rng.sample("ABC", rng.randint(1, 2))
                    rows.append([
                        (h, rng.choice(["W", "R", "RW"]))
                        for h in handles
                    ])
                self.assert_optimal(make_tasks(rows), cfg)

    def test_asymmetric_transfers(self):
        for seed in (12, 22, 70, 79, 107, 121):
            with self.subTest(seed=seed):
                rng = Random(seed)
                sizes = {h: rng.randint(1, 4) for h in "ABCD"}
                cfg = Config(
                    12,
                    rng.choice([1, 2, 4]),
                    rng.choice([1, 2, 4]),
                    chunk_size_bytes=4,
                    compute_order="trace",
                )
                rows = []
                durations = []
                for _ in range(6):
                    handles = rng.sample("ABCD", rng.randint(1, 3))
                    rows.append([
                        (h, rng.choice(["R", "RW", "W"]))
                        for h in handles
                    ])
                    durations.append(rng.randint(1, 5))
                self.assert_optimal(
                    make_tasks(rows, sizes, durations), cfg
                )

    def test_unequal_payloads_and_release_order(self):
        sizes = {"A": 2, "B": 1, "C": 3, "D": 2}
        encoded = [
            (3, "BW CW DW"),
            (4, "CR ARW"),
            (2, "ARW BRW"),
            (1, "DR CW AW"),
            (1, "AW DR"),
            (1, "CR DRW AW"),
            (3, "AW"),
        ]
        rows = [
            [(token[0], token[1:]) for token in modes.split()]
            for _, modes in encoded
        ]
        durations = [duration for duration, _ in encoded]
        cfg = Config(
            8, 1, 2,
            chunk_size_bytes=2,
            compute_order="trace",
        )
        result = self.assert_optimal(
            make_tasks(rows, sizes, durations), cfg
        )
        self.assertAlmostEqual(result.makespan_us, 17.5)
        saves = [e.handle for e in result.events if e.lane == "D2H"]
        self.assertEqual(saves, ["B:0", "D:0"])
