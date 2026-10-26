from pathlib import Path
from tempfile import TemporaryDirectory
from nntile_sim.parser import parse_tasks, TraceFormatError

def test_completed_tasks_only_and_eof():
    with TemporaryDirectory() as td:
        path = Path(td) / 'tiny.rec'
        path.write_text('Control: WontUse\nJobId: 1\n\nName: clear\nJobId: 2\nStartTime: 5\nEndTime: 9\nHandles: abc\nModes: W\nSizes: 64')
        tasks = parse_tasks(path)
        assert len(tasks) == 1 and tasks[0].duration_us == 4
        assert not tasks[0].accesses[0].reads_previous_value

def test_order_and_rwc():
    with TemporaryDirectory() as td:
        path = Path(td) / 'tiny.rec'
        path.write_text('Name: a\nJobId: 2\nStartTime: 8\nEndTime: 10\nHandles: abc\nModes: RWC\nSizes: 4\n\nName: b\nJobId: 1\nStartTime: 3\nEndTime: 4\nHandles: abc\nModes: R\nSizes: 4\n')
        tasks = parse_tasks(path)
        assert [t.job_id for t in tasks] == ['1', '2']
        assert tasks[1].accesses[0].reads_previous_value and tasks[1].accesses[0].writes_value
