#from .Task import Task
#from .Worker import Worker
from .const import *
import sys
import time

class Scheduler: 
    def __init__(self, push_task_mode):
        self.push_task_mode = push_task_mode
    
    def push_task(self, task, workers, data_list):
        if self.push_task_mode == PUSH_TASK_DMDASD:
            return self.push_task_dmdasd(task, workers, data_list)
        elif self.push_task_mode == PUSH_TASK_NEW_V1:
            return self.push_task_new_v1(task, workers, data_list)
        
    def push_task_dmdasd(self, task, workers, data_list):
        '''
        Select the best worker and add tasks to the worker queue
        '''

        best_worker = self.calculate_worker(workers, task)

        for data_id in task.depends_on:
            if data_id in data_list.keys() and data_list[data_id].status == STATUS_INIT:
                workers[best_worker].cpu.memory.append(data_list[data_id])
                workers[best_worker].load_data(data_list[data_id], workers)
                data_list[data_id].status = STATUS_DONE
        workers[best_worker].queue.append(task)

    def push_task_new_v1(self, task, workers, data_list):
        '''
        The same as dmdasd, but without prefetch the data
        '''
        
        best_worker = self.calculate_worker(workers, task)

        for data_id in task.depends_on:
            if data_id in data_list.keys() and data_list[data_id].status == STATUS_INIT:
                workers[best_worker].cpu.memory.append(data_list[data_id])
                data_list[data_id].status = STATUS_DONE
        workers[best_worker].queue.append(task)

    def do_work(self, task_list, data_list, workers):
        start_time = time.time()
        for task in task_list.values():
            self.push_task(task, workers, data_list)

        data_task_list= {**task_list, **data_list}

        for i in range(len(workers)):
            for item in workers[i].queue:
                temp = item.depends_on
                item.depends_on = [data_task_list[key] for key in temp]
        print(f'{time.time() - start_time} seconds\npush task -- done.')
        print('-'*10)

    def calculate_worker(self, workers, task):
        '''
        Selects the best worker
        '''
        min_time = sys.float_info.max
        best_worker = None
        for worker in workers:
            curr_time = 0
            for t in worker.queue:
                curr_time += t.task_duration
            curr_time += task.task_duration
            for w in workers: 
                if w.name != worker.name: 
                    for t in worker.queue: 
                        if t in task.depends_on: 
                            curr_time+= t.size / TIME_DELIVERY_DATA
            if curr_time <= min_time: 
                min_time = curr_time
                best_worker = worker.name
        return best_worker