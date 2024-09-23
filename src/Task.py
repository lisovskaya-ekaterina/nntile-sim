from .const import *
from random import randint
class Task: 
    def __init__(self, id, name, task_duration : float, depends_on: list, size):
        self.id = id
        self.name = name 
        self.task_duration = task_duration
        self.status = STATUS_INIT
        self.depends_on = depends_on
        self.size = size
        self.unused_time = 0 

###TODO Добавить данные ? Куда ?