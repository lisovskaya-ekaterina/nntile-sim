from dataclasses import dataclass
from .const import *

@dataclass
class Task:
    id: int
    name : str
    task_duration : float
    size : int
    depends_on : list
    status : str = STATUS_INIT
    unused_time : int = 0