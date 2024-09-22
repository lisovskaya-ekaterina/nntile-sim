from .Task import Task
from .CPU import CPU
from .const import *
import random
import sys


sys.setrecursionlimit(3000000)

class Worker:
    def __init__(self, name : int, memory_size : float, memory: list[Task], cpu : CPU, eviction_mode : str, pop_task_mode : str):
        self.memory_size = memory_size
        self.name = name
        self.memory = memory
        self.current_task = None
        self.work_time = 0
        self.n_load = 0
        self.cpu = cpu
        self.queue = []
        self.planned_tasks = []
        self.eviction_mode = eviction_mode
        self.pop_task_mode = pop_task_mode
        self.text = open('sim_log.txt', mode = 'w')
        self.text.write('sim_JobID\n')
        

    def eviction(self):
        if self.eviction_mode == EVICTION_LRU:
            return self.eviction_LRU()
        elif self.eviction_mode == EVICTION_NEW_V1:
            return self.eviction_new_v1()
    
    def pop_task(self):
        if self.pop_task_mode == POP_TASK_DMDASD:
            return self.pop_task_dmdasd()
        elif self.pop_task_mode == POP_TASK_NEW_V1:
            return self.pop_task_new_v1()
    
    def eviction_LRU(self):
        '''
        Least Recently Used eviction policy
        By default in StarPU scheduling policies
        Evictees data from worker memory that has not been used for the longest time
        '''
        data = self.memory[-1]
        self.memory.remove(data)
        self.cpu.memory.append(data)
            
    def pop_task_dmdasd(self):
        if self.current_task:
            self.text.write(f'{self.current_task.id}\n') 
            self.queue.remove(self.current_task)
            self.current_task.status = STATUS_DONE
            self.memory.append(self.current_task)
            self.work_time += self.current_task.task_duration
        for task in self.queue: 
            if task.status == STATUS_READY: 
                self.current_task = task
                break 
            else: 
                flag = True
                for data in task.depends_on: 
                    if data.status != STATUS_DONE: 
                        flag = False
                        break
                if flag == True: 
                    self.current_task = task
                    break
        for d in self.current_task.depends_on:
            if d not in self.memory:
                self.load_data(d)
        
        self.update_usless_data(self.current_task.depends_on)
    
    def eviction_new_v1(self):
        '''
        A place to develop a new policy of eviction from the worker's memory. 
        When there is not enough space to load any new data.
        '''
        pass

    def pop_task_new_v1(self):
        '''
        A place to develop a new task selection policy from the queue. 
        When the worker finishes completing the current task and is ready to take on a new one.
        '''
        if self.current_task:
            self.queue.remove(self.current_task)
            self.current_task.status = STATUS_DONE
            self.memory.append(self.current_task)
            self.work_time += self.current_task.task_duration
        top_flag = True
        for task in self.queue:
            flag = True
            for data in task.depends_on:
                if data.status != STATUS_DONE or data not in self.memory:
                    flag = False
                    break
            if flag == True:
                self.current_task = task
                top_flag = False
                break
        if top_flag and len(self.queue) > 0:
            for task in self.queue: 
                if task.status == STATUS_READY: 
                    self.current_task = task
                    break 
                else: 
                    flag = True
                    for data in task.depends_on: 
                        if data.status != STATUS_DONE: 
                            flag = False
                            break
                    if flag == True: 
                        self.current_task = task
                        break
        for d in self.current_task.depends_on:
            if d not in self.memory:
                self.load_data(d)
        
        self.update_usless_data(self.current_task.depends_on)


    def check_busy_space(self):
        '''
        A function that returns the current amount of occupied memory on the worker.
        '''
        self.busy_space = 0
        for elem in self.memory:
            self.busy_space += elem.size
        return self.busy_space
    
    def update_usless_data(self, data_need_to_work : list [Task]):
        '''
        A function that, at the task's start time, updates the least recently used counter for each piece of data in memory.
        '''
        for task in self.memory:
            if task not in data_need_to_work:
                task.unused_time += 1
            else:
                task.unused_time = 0
        self.memory = sorted(self.memory, key = lambda x: x.unused_time, reverse = True)

    def load_data(self, data):
        '''
        A function that has as input a data that needs to be loaded into the worker's memory. 
        First, it check whether there is an opportunity to download, and, 
        if necessary, evictees (according to the eviction policy) data from the worker's memory 
        until there is enough space for new data.
        After that, it loads the data into the worker's memory and deletes the data from the CPU's memory.
        The variables for collecting statistics (runtime and number of loads) are increased accordingly.
        '''
        while data.size + self.check_busy_space() > self.memory_size:
            self.eviction()
        self.memory.append(data)
        self.cpu.memory.remove(data)
        self.work_time += data.size / TIME_DELIVERY_DATA
        self.n_load += 1