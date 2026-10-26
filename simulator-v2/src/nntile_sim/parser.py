"""Streaming parser for StarPU/NNTile .rec files.

Control records and incomplete jobs are not compute tasks. Preserve task order
by observed StartTime (with SubmitOrder/JobId as stable tie breakers).
"""
from collections.abc import Iterator
from pathlib import Path
from .model import Access, Task

class TraceFormatError(ValueError):
    pass

def records(path: str | Path) -> Iterator[dict[str, str]]:
    with open(path, encoding="utf-8") as stream:
        rec: dict[str, str] = {}
        for line_no, raw in enumerate(stream, 1):
            line = raw.strip()
            if not line:
                if rec:
                    yield rec
                    rec = {}
                continue
            if ": " not in line:
                raise TraceFormatError(f"{path}:{line_no}: expected 'Key: value'")
            key, value = line.split(": ", 1)
            rec[key] = value
        if rec:
            yield rec

def parse_tasks(path: str | Path) -> list[Task]:
    result: list[Task] = []
    for rec in records(path):
        required = ("JobId", "Name", "StartTime", "EndTime", "Handles", "Modes", "Sizes")
        if "Control" in rec or not all(k in rec for k in required):
            continue
        handles = rec["Handles"].split()
        modes = rec["Modes"].split()
        sizes = rec["Sizes"].split()
        if not (len(handles) == len(modes) == len(sizes)):
            raise TraceFormatError(f"JobId {rec['JobId']}: mismatched Handles/Modes/Sizes")
        accesses = tuple(Access(h, int(s), m) for h, s, m in zip(handles, sizes, modes))
        start, end = float(rec["StartTime"]), float(rec["EndTime"])
        if end < start:
            raise TraceFormatError(f"JobId {rec['JobId']}: EndTime < StartTime")
        result.append(Task(
            job_id=rec["JobId"], name=rec["Name"],
            iteration=tuple(map(int, rec["Iteration"].split())) if rec.get("Iteration") else None,
            accesses=accesses, start_time=start, end_time=end,
            submit_order=int(rec["SubmitOrder"]) if rec.get("SubmitOrder") else None,
            depends_on=tuple(rec.get("DependsOn", "").split()),
        ))
    result.sort(key=lambda t: (t.start_time, t.submit_order if t.submit_order is not None else -1, t.job_id))
    ids = [t.job_id for t in result]
    if len(ids) != len(set(ids)):
        raise TraceFormatError("Duplicate completed compute JobId in trace")
    return result
