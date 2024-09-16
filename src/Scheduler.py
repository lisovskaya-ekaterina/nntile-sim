from .Task import Task
from .Worker import Worker
from .const import *

class Scheduler: 
    def __init__(self):
        pass
    
    def push_task(self, task, workers, task_list):
        '''
        Select the best worker and add tasks to the worker queue
        '''
        # TODO: choice worker_id
        best_worker = 0
        
        if (task.status == STATUS_DONE or len(task.depends_on) == 0) and task not in workers[best_worker].memory and task not in workers[best_worker].cpu.memory:
            workers[best_worker].cpu.memory.append(task)
            
        elif len(task.depends_on) > 0:
            for data_id in task.depends_on:
                if task_list[data_id] not in workers[best_worker].memory and task_list[data_id].status == STATUS_DONE:
                    if task_list[data_id] not in workers[best_worker].cpu.memory:
                        workers[best_worker].cpu.memory.append(task_list[data_id])
                    workers[best_worker].load_data(task_list[data_id])
            workers[best_worker].queue.append(task)

    def do_work(self, task_list, workers):
        for task in task_list.values():
            self.push_task(task, workers, task_list)

        for i in range(len(workers)):
            for item in workers[i].queue:
                temp = item.depends_on
                item.depends_on = [task_list[key] for key in temp]