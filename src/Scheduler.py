from .Task import Task
from .Worker import Worker
from .const import *
import sys

class Scheduler: 
    def __init__(self, push_task_mode):
        self.text = open('real_log.txt', mode = 'w')  
        self.text.write('real_JobID\n')
        self.push_task_mode = push_task_mode
    
    def push_task(self, task, workers, task_list):
        if self.push_task_mode == PUSH_TASK_DMDASD:
            return self.push_task_dmdasd(task, workers, task_list)
        elif self.push_task_mode == PUSH_TASK_NEW_V1:
            return self.push_task_new_v1(task, workers, task_list)
        
    def push_task_dmdasd(self, task, workers, task_list):
        '''
        Select the best worker and add tasks to the worker queue
        '''
        # TODO: choice worker_id
        best_worker = 0
        
        if (task.status == STATUS_DONE or len(task.depends_on) == 0) and task not in workers[best_worker].memory.memory and task not in workers[best_worker].cpu.memory:
            workers[best_worker].cpu.memory.append(task)
            
        elif len(task.depends_on) > 0:
            self.text.write(f'{task.id}\n')
            for data_id in task.depends_on:
                if task_list[data_id] not in workers[best_worker].memory.memory and task_list[data_id].status == STATUS_DONE:
                    if task_list[data_id] not in workers[best_worker].cpu.memory:
                        workers[best_worker].cpu.memory.append(task_list[data_id])
                    workers[best_worker].load_data(task_list[data_id])
            workers[best_worker].queue.append(task)

    def push_task_new_v1(self, task, workers, task_list):
        '''
        The same as dmdasd, but without prefetch the data
        '''
        
        min_time = sys.float_info.max
        best_worker = 0
        for worker in worker:
            curr_time = 0
            for t in worker.queue:
                curr_time += t.task_duration
                # TODO: calculate time duration with transactions
            if curr_time <= min_time: 
                min_time = curr_time
                best_worker = worker.name
                
        
        if (task.status == STATUS_DONE or len(task.depends_on) == 0) and task not in workers[best_worker].memory.memory and task not in workers[best_worker].cpu.memory:
            workers[best_worker].cpu.memory.append(task)
            
        elif len(task.depends_on) > 0:
            self.text.write(f'{task.id}\n')
            for data_id in task.depends_on:
                if task_list[data_id] not in workers[best_worker].memory.memory and task_list[data_id].status == STATUS_DONE:
                    if task_list[data_id] not in workers[best_worker].cpu.memory:
                        workers[best_worker].cpu.memory.append(task_list[data_id])
            workers[best_worker].queue.append(task)

    def do_work(self, task_list, workers):
        for task in task_list.values():
            self.push_task(task, workers, task_list)

        for i in range(len(workers)):
            for item in workers[i].queue:
                temp = item.depends_on
                item.depends_on = [task_list[key] for key in temp]
        print('push task -- done')