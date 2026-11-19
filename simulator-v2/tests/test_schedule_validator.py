"""Regression tests for the draft global schedule validator."""

import copy
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate_schedule.py"
EXAMPLES = ROOT / "docs" / "scheduling"

spec = importlib.util.spec_from_file_location("schedule_validator", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def examples():
    plan = json.loads((EXAMPLES / "example-plan.json").read_text())
    schedule = json.loads(
        (EXAMPLES / "example-schedule.json").read_text()
    )
    return copy.deepcopy(plan), copy.deepcopy(schedule)


def operation(document, op_id):
    return next(op for op in document["operations"] if op["id"] == op_id)


def test_valid_schedule():
    plan, schedule = examples()
    assert module.validate(plan, schedule) == []


def test_gpu_capacity_exceeded():
    plan, schedule = examples()
    plan["capacity_bytes"] = 786432
    assert any(
        "GPU capacity exceeded" in error
        for error in module.validate(plan, schedule)
    )


def test_stale_gpu_version():
    plan, schedule = examples()
    operation(plan, "compute_t3")["reads"][0]["version"] = 0
    assert any(
        "missing or stale GPU version" in error
        for error in module.validate(plan, schedule)
    )


def test_drop_during_d2h():
    plan, schedule = examples()
    drop = operation(schedule, "drop_x")
    drop["start_us"] = 12
    drop["end_us"] = 12
    assert any(
        "DROP during active operation" in error
        for error in module.validate(plan, schedule)
    )


def test_drop_during_compute():
    plan, schedule = examples()
    drop = operation(schedule, "drop_y")
    drop["start_us"] = 20
    drop["end_us"] = 20
    assert any(
        "DROP during active operation" in error
        for error in module.validate(plan, schedule)
    )


def test_cpu_version_mismatch():
    plan, schedule = examples()
    operation(plan, "h2d_x")["version"] = 0
    assert any(
        "CPU version mismatch" in error
        for error in module.validate(plan, schedule)
    )


def test_dependency_violation():
    plan, schedule = examples()
    operation(schedule, "compute_t3")["start_us"] = 30
    assert any(
        "dependency" in error
        for error in module.validate(plan, schedule)
    )


def test_compute_cannot_overwrite_during_d2h():
    plan, schedule = examples()

    # T3 overwrites x while D2H is still transferring x@1.
    # Move T3 earlier and extend D2H to create the conflict.
    operation(plan, "compute_t3")["reads"] = []
    operation(plan, "compute_t3")["writes"] = [
        {"chunk": "x:0", "version": 2}
    ]

    operation(schedule, "d2h_x")["end_us"] = 38
    operation(schedule, "compute_t3")["start_us"] = 32
    operation(schedule, "compute_t3")["end_us"] = 42

    errors = module.validate(plan, schedule)

    assert any(
        "active operation" in error
        or "D2H source changed" in error
        or "D2H source version" in error
        for error in errors
    ), errors


def test_compute_cannot_read_during_h2d():
    plan, schedule = examples()

    # T2 starts while z is still being transferred.
    operation(schedule, "h2d_z")["end_us"] = 22
    operation(schedule, "compute_t2")["start_us"] = 18

    errors = module.validate(plan, schedule)

    assert any(
        "missing or stale GPU version" in error
        for error in errors
    ), errors


def test_compute_write_conflicts_with_active_d2h():
    plan = {
        "capacity_bytes": 786432,
        "chunks": [
            {
                "id": "x:0",
                "size_bytes": 786432,
                "reserved_bytes": 786432,
            }
        ],
        "initial_cpu_versions": {},
        "operations": [
            {
                "id": "alloc_x",
                "type": "ALLOC",
                "chunk": "x:0",
                "depends_on": [],
            },
            {
                "id": "write_v1",
                "type": "Compute",
                "reads": [],
                "writes": [{"chunk": "x:0", "version": 1}],
                "depends_on": ["alloc_x"],
            },
            {
                "id": "save_v1",
                "type": "D2H",
                "chunk": "x:0",
                "version": 1,
                "depends_on": ["write_v1"],
            },
            {
                "id": "write_v2",
                "type": "Compute",
                "reads": [],
                "writes": [{"chunk": "x:0", "version": 2}],
                "depends_on": ["write_v1"],
            },
        ],
    }

    schedule = {
        "operations": [
            {"id": "alloc_x", "lane": "Memory", "start_us": 0, "end_us": 0},
            {"id": "write_v1", "lane": "Compute", "start_us": 0, "end_us": 10},
            {"id": "save_v1", "lane": "D2H", "start_us": 10, "end_us": 30},
            {"id": "write_v2", "lane": "Compute", "start_us": 15, "end_us": 25},
        ]
    }

    errors = module.validate(plan, schedule)

    assert any(
        "conflict" in error.lower()
        or "active operation" in error.lower()
        for error in errors
    ), errors


def test_d2h_conflicts_with_active_compute_write():
    plan = {
        "capacity_bytes": 786432,
        "chunks": [
            {"id": "x:0", "size_bytes": 786432, "reserved_bytes": 786432}
        ],
        "initial_cpu_versions": {"x:0": 0},
        "operations": [
            {
                "id": "h2d_x",
                "type": "H2D",
                "chunk": "x:0",
                "version": 0,
                "depends_on": [],
            },
            {
                "id": "write_x",
                "type": "Compute",
                "reads": [],
                "writes": [{"chunk": "x:0", "version": 1}],
                "depends_on": ["h2d_x"],
            },
            {
                "id": "save_x",
                "type": "D2H",
                "chunk": "x:0",
                "version": 0,
                "depends_on": ["h2d_x"],
            },
        ],
    }

    schedule = {
        "operations": [
            {"id": "h2d_x", "lane": "H2D", "start_us": 0, "end_us": 4},
            {"id": "write_x", "lane": "Compute", "start_us": 4, "end_us": 14},
            {"id": "save_x", "lane": "D2H", "start_us": 8, "end_us": 12},
        ]
    }

    errors = module.validate(plan, schedule)

    assert any(
        "conflict" in error.lower()
        for error in errors
    ), errors
