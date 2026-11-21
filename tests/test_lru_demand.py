from nntile_sim.model import Access, Task
from nntile_sim.core.engine import Config, Simulator


def task(jid, access, dur=10):
    return Task(jid, jid, None, tuple(Access(*x) for x in access), 0, dur, None, ())


def run(tasks, cap=100):
    return Simulator(tasks, Config(cap, 10, 10)).run()


def test_no_prefetch_and_no_transfer_compute_overlap():
    r = run([task('1', [('a', 40, 'R')]), task('2', [('b', 40, 'R')])])
    compute = [e for e in r.events if e.lane == 'Compute']
    loads = [e for e in r.events if e.lane == 'H2D']
    assert loads[1].start_us >= compute[0].end_us
    assert compute[1].start_us >= loads[1].end_us


def test_lru_evicts_oldest_not_largest():
    r = run([task('1', [('a', 30, 'R')]), task('2', [('b', 40, 'R')]),
             task('3', [('a', 30, 'R')]), task('4', [('c', 40, 'R')])], cap=100)
    drops = [e.handle for e in r.events if e.action == 'DROP']
    assert drops[0] == 'b'


def test_dirty_live_writeback_and_reload():
    r = run([task('1', [('x', 60, 'W')]), task('2', [('y', 60, 'R')]),
             task('3', [('x', 60, 'R')])], cap=60)
    assert r.d2h_bytes == 60
    assert r.h2d_bytes == 120


def test_write_only_does_not_fetch_old_value():
    r = run([task('1', [('x', 60, 'W')])], cap=60)
    assert r.h2d_bytes == 0


def test_legacy_mode_still_available():
    tasks = [task('1', [('x', 40, 'R')])]
    r = Simulator(tasks, Config(100, 10, 10, memory_policy='legacy-baseline')).run()
    assert r.completed_jobs == 1
