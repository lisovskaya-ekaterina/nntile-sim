#from .Task import Task
#from .Worker import Worker
from .const import *
import sys
import time
from operator import attrgetter

class Scheduler: 
    def __init__(self, push_task_mode):
        self.push_task_mode = push_task_mode
    
    def push_task(self, task, workers, data_list):
        if self.push_task_mode == PUSH_TASK_DMDASD:
            return self.push_task_dmdasd(task, workers, data_list)
        elif self.push_task_mode == PUSH_TASK_NEW_V1:
            return self.push_task_new_v1(task, workers, data_list)
        elif self.push_task_mode == PUSH_TASK_RANDOM:
            return self.push_task_random(task, workers, data_list)
        elif self.push_task_mode == PUSH_TEST:
            return self.push_task_test(task, workers, data_list)
        
    def push_task_test(self, task, workers, data_list):
        '''
        с загрузкой каждой задачи загружаются данные для нее 
        после определения места задачи в очереди 
        загружаются данные для самой первой задачи. 
        
        TODO наверное можно это вынести куда-то в отдельную функцию 
        пока не могу придумать куда
        
        При изменении знака, т.е. сортируем мы от большего к меньшему приоритету 
        или наоборот ничего не меняется, может это такие данные? 
        '''
        
        best_worker = self.calculate_worker(workers, task)
    
        for data_id in task.depends_on:
            if data_id in data_list.keys() and data_list[data_id].status == STATUS_INIT:
                workers[best_worker].cpu.memory.append(data_list[data_id])
                workers[best_worker].load_data(data_list[data_id], workers)
                data_list[data_id].status = STATUS_DONE
        
        if len(workers[best_worker].queue) == 0:
            workers[best_worker].queue.append(task)
        else: 
            transition_flag = 0
            for i in range(len(workers[best_worker].queue)):
                if task.param >= workers[best_worker].queue[i].param:
                    workers[best_worker].queue.insert(i, task)
                    transition_flag = 1 
                    break
            if not transition_flag:
                workers[best_worker].queue.append(task)
                    
        for data_id in workers[best_worker].queue[0].depends_on:
            if data_id in data_list.keys() and data_list[data_id].status == STATUS_INIT:
                workers[best_worker].cpu.memory.append(data_list[data_id])
                workers[best_worker].load_data(data_list[data_id], workers)
                data_list[data_id].status = STATUS_DONE
                
        # print(f'first task {workers[best_worker].queue[0].name} -- priority {workers[best_worker].queue[0].param}')
        # print(workers[best_worker].queue[0].depends_on)
        # print(len( workers[best_worker].memory.memory))
        # for i in workers[best_worker].queue:
        #     print(len(workers[best_worker].queue))
                    
        
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

    def push_task_random(self, task, workers, data_list):
        '''
        The same as dmdasd, but without prefetch the data
        '''
        
        best_worker = task.best_worker

        for data_id in task.depends_on:
            if data_id in data_list.keys() and data_list[data_id].status == STATUS_INIT:
                workers[best_worker].cpu.memory.append(data_list[data_id])
                data_list[data_id].status = STATUS_DONE
        workers[best_worker].queue.append(task)

    def do_work(self, task_list, data_list, workers):
        start_time = time.time()
        # print(f"length of task_list -- {len(task_list)}")
        for task in task_list.values():
            # print(f"curr task param -- {task.param}")
            self.push_task(task, workers, data_list)
            # print(f"worker 0 queue length -- {len(workers[0].queue)}")

        
        
        # for i in workers[0].queue:
        #     print(i.id)
        data_task_list= {**task_list, **data_list}

        for worker in workers:
            for item in worker.queue:
                item.depends_on = [data_task_list[d_id] for d_id in item.depends_on if d_id in data_task_list]


        # for w in workers:
        #     for d in w.queue[0].depends_on:
        #         if d not in w.memory.memory:
        #             w.load_data(d, workers)
                    
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