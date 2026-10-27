"""Immutable trace entities. Raw StarPU timestamps are in seconds; duration_us is in microseconds."""
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Access:
    handle: str
    size_bytes: int
    mode: str

    @property
    def reads_previous_value(self) -> bool:
        return self.mode in {"R", "RW", "RWC"}

    @property
    def writes_value(self) -> bool:
        return self.mode in {"W", "RW", "RWC"}

@dataclass(frozen=True, slots=True)
class Task:
    job_id: str
    name: str
    iteration: tuple[int, ...] | None
    accesses: tuple[Access, ...]
    start_time: float
    end_time: float
    submit_order: int | None
    depends_on: tuple[str, ...]

    @property
    def duration_us(self) -> float:
        # StarPU tasks.rec timestamps are in seconds.
        return (self.end_time - self.start_time) * 1_000_000
