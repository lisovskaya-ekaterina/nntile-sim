from pathlib import Path
from nntile_sim.sim_cli import main, build_parser


def test_defaults():
    args = build_parser().parse_args(["trace.rec", "--gpu-memory-mib", "1", "--h2d-gbps", "12", "--d2h-gbps", "12"])
    assert args.compute_order == "topological"


def test_rejects_missing_trace(tmp_path, capsys):
    code = main([str(tmp_path / "missing.rec"), "--gpu-memory-mib", "1", "--h2d-gbps", "12", "--d2h-gbps", "12"])
    assert code == 1
    assert "Simulation failed" in capsys.readouterr().err


def test_rejects_custom_without_order_file():
    import pytest
    with pytest.raises(SystemExit) as exc:
        main(["trace.rec", "--gpu-memory-mib", "1", "--h2d-gbps", "12", "--d2h-gbps", "12", "--compute-order", "custom"])
    assert exc.value.code == 2


def test_runs_simulation_and_exports_csv(tmp_path, monkeypatch, capsys):
    from nntile_sim.model import Access, Task
    from nntile_sim import sim_cli
    tasks = [Task(job_id="1", name="first", iteration=None,
                  accesses=(Access("x", 40, "R"),), start_time=0.0,
                  end_time=10.0, submit_order=0, depends_on=())]
    monkeypatch.setattr(sim_cli, "parse_tasks", lambda _: tasks)
    output = tmp_path / "events.csv"
    code = main(["unused.rec", "--gpu-memory-mib", "1", "--h2d-gbps", "1", "--d2h-gbps", "1",
                 "--events-csv", str(output)])
    assert code == 0
    assert "Completed tasks: 1 / 1" in capsys.readouterr().out
    assert "H2D" in output.read_text()
