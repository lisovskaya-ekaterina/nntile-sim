from dataclasses import dataclass, field
from typing import List
from .const import *

@dataclass
class Task:
    """
    Represents a task with its attributes.
    """
    id: int
    name: str
    task_duration: float
    size: int
    depends_on: List['Task'] = field(default_factory=list)
    status: str = STATUS_INIT
    unused_time: int = 0