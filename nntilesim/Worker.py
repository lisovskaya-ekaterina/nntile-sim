from .Task import Task
from .CPU import CPU
from .const import *
import sys
sys.setrecursionlimit(3000000)

class WorkerMemory: 
    def __init__(self, memory_size :float, name: int,memory: list[Task]):
        self.memory_size = memory_size
        self.name = name
        self.memory = memory
        
class Worker:
    def __init__(self, name : int, memory_size : float, memory: list[Task], cpu : CPU, eviction_mode : str, pop_task_mode : str):
        self.name = name
        self.current_task = None
        self.work_time = 0
        self.n_load = 0
        self.cpu = cpu
        self.queue = []
        self.planned_tasks = []
        self.eviction_mode = eviction_mode
        self.pop_task_mode = pop_task_mode
        self.memory = WorkerMemory(memory_size, name, memory)
        self.lru_threshold = LRU_THRESHOLD
        self.memory_evict_percent = MEMORY_EVICT_PERSENT

    def eviction(self):
        if self.eviction_mode == EVICTION_LRU:
            self.eviction_LRU()
        elif self.eviction_mode == EVICTION_MRU:
            self.eviction_MRU()
        elif self.eviction_mode == EVICTION_THRESHOLD:
            self.eviction_threshold()
    
    def pop_task(self, workers, scheduler):
        if self.pop_task_mode == POP_TASK_GRAPH_TEST:
            self.pop_task_graph_test(workers, scheduler)
        elif self.pop_task_mode == POP_TASK_NEW_V2:
            self.pop_task_new_v2(workers, scheduler)
    
    def evict_after_task(self):
        if self.current_task:
            data_to_remove = [data for data in self.memory.memory]
            for data in data_to_remove:
                self.memory.memory.remove(data)
                self.cpu.memory.append(data)

    def eviction_threshold(self):
        num_to_evict = int(len(self.memory.memory) * self.memory_evict_percent)
        for _ in range(num_to_evict):
            data = next((data for data in self.memory.memory if data.protected == False), None)
            if data:
                self.memory.memory.remove(data)
                self.cpu.memory.append(data)
    
    def eviction_LRU(self):
        data = next((data for data in self.memory.memory if data.protected == False), None)
        if data:
            self.memory.memory.remove(data)
            self.cpu.memory.append(data)

    def eviction_MRU(self):
        data = next((data for data in self.memory.memory[::-1] if data.protected == False), None)
        if data:
            self.memory.memory.remove(data)
            self.cpu.memory.append(data)
            
    def pop_task_graph_test(self, workers, scheduler):
        if self.current_task:
            scheduler.prio_gpu.remove(self.current_task)
            self.current_task.status = STATUS_DONE
            self.memory.memory.append(self.current_task)
            self.work_time += self.current_task.task_duration

        def can_execute(task):
            return all(data.status == STATUS_DONE for data in task.depends_on)
        
        self.current_task = next((task for task in scheduler.prio_gpu 
                                  if task.status == STATUS_READY or can_execute(task)), None)

        if not self.current_task:
            return

        for d in self.current_task.depends_on:
            if d not in self.memory.memory:
                self.load_data(d, workers, d.size / TIME_DELIVERY_DATA)
        
        self.update_usless_data(self.current_task.depends_on)

        if self.eviction_mode == EVICT_AFTER_TASK:
            self.evict_after_task()

    def pop_task_new_v2(self, workers, scheduler):

        if self.current_task:
            scheduler.prio_gpu.remove(self.current_task)
            self.current_task.status = STATUS_DONE
            self.memory.memory.append(self.current_task)
            self.work_time += self.current_task.task_duration

        def can_execute(task):
            return all(data.status == STATUS_DONE for data in task.depends_on)

        self.current_task = next((task for task in scheduler.prio_gpu
                                if task.status == STATUS_READY or can_execute(task)), None)

        if not self.current_task:
            return

        current_task_data_ids = set(id(d) for d in self.current_task.depends_on)

        lookahead_data_ids = set()
        lookahead_tasks = [
            task for task in scheduler.prio_gpu
            if task != self.current_task and (task.status == STATUS_READY or can_execute(task))
        ][:PRELOAD_N]

        memory_ids_set = set(id(data) for data in self.memory.memory)

        all_data = {id(d): d for d in self.current_task.depends_on}
        all_data.update({id(d): d for task in lookahead_tasks for d in task.depends_on})

        for next_task in lookahead_tasks:
            for d in next_task.depends_on:
                if id(d) not in memory_ids_set and id(d) not in current_task_data_ids:
                    lookahead_data_ids.add(id(d))

        for data_id in current_task_data_ids:
            data = all_data.get(data_id)
            if data and data not in self.memory.memory:
                self.load_data(data, workers, data.size / TIME_DELIVERY_DATA)

        for data_id in lookahead_data_ids:
            data = all_data.get(data_id)
            if data:
                self.load_data(data, workers, data.size / TIME_DELIVERY_DATA)
                data.protected = True

        all_loaded_data_ids = current_task_data_ids.union(lookahead_data_ids)
        all_loaded_data = [all_data[data_id] for data_id in all_loaded_data_ids if data_id in all_data]
        self.update_usless_data(all_loaded_data)

        for data_id in lookahead_data_ids:
            data = all_data.get(data_id)
            if data:
                data.protected = False

        if self.eviction_mode == EVICT_AFTER_TASK:
            self.evict_after_task()

    def check_busy_space(self):
        self.busy_space = 0
        for elem in self.memory.memory:
            self.busy_space += elem.size
        return self.busy_space
    
    def update_usless_data(self, data_need_to_work : list [Task]):
        for task in self.memory.memory:
            if not hasattr(task, 'unused_time'):
                task.unused_time = 0
            if task not in data_need_to_work:
                task.unused_time += 1

                if self.eviction_mode == EVICT_BY_UNUSED_TH:
                    if task.unused_time >= self.lru_threshold:
                        self.memory.memory.remove(task)
                        self.cpu.memory.append(task)
                        continue
                    
            else:
                task.unused_time = 0
        self.memory.memory = sorted(self.memory.memory, key = lambda x: x.unused_time, reverse = True)

    def load_data(self, data, workers, time):
        while data.size + self.check_busy_space() > self.memory.memory_size:
            self.eviction()
        self.memory.memory.append(data)
        if data in self.cpu.memory:
            self.cpu.memory.remove(data)
        else: 
            for w in workers:
                if w.name != self.name and data in w.memory.memory: 
                    w.memory.memory.remove(data)
        self.work_time += time
        self.n_load += 1