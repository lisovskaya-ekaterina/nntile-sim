#from .Task import Task
#from .Worker import Worker
from .const import *
import bisect
import time

def key_wrapper(task):
    return task.priority

class Scheduler: 
    def __init__(self, push_task_mode):
        self.push_task_mode = push_task_mode
        self.prio_gpu = []
    
    def push_task(self, task, cpu, data_list):
        if self.push_task_mode == PUSH_TASK_GRAPH_TEST:
            return self.push_task_graph_test(task, cpu, data_list)
        elif self.push_task_mode == PUSH_TASK_NEW_V2:
            return self.push_task_new_v2(task, cpu, data_list)
        
    def push_task_graph_test(self, task, cpu, data_list):

        for data_id in task.depends_on:
            data_item = data_list.get(data_id)
            if data_item and data_item.status == STATUS_INIT:
                cpu.memory.append(data_item)
                data_item.status = STATUS_DONE
                
        self.prio_gpu.append(task)
    
    def push_task_new_v2(self, task, cpu, data_list):
        pass

    def do_work(self, task_list, data_list, cpu):
        start_time = time.time()
        for task in task_list.values():
            self.push_task(task, cpu, data_list)
        self.prio_gpu = sorted(self.prio_gpu, key = lambda x: x.priority, reverse = True)

        data_task_list= {**task_list, **data_list}

        for item in self.prio_gpu:
            item.depends_on = [data_task_list[d_id] for d_id in item.depends_on if d_id in data_task_list]

        print(f'{len(self.prio_gpu)} tasks in prio_gpu')
        print(f'{time.time() - start_time} seconds\npush task -- done.')
        print('-'*10)