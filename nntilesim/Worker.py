from .Task import Task
from .CPU import CPU
from .const import *
import sys
from typing import List
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Уровень логирования: DEBUG для подробного вывода
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f'worker_{1}.log')  # Логирование в файл
        
    ]# logging.StreamHandler()  # Логирование в консоль
)

sys.setrecursionlimit(3000000)

class WorkerMemory:
    def __init__(self, memory_size: float, name: int, memory: List[Task]):
        self.memory_size = memory_size
        self.name = name
        self.memory = memory

class Worker:
    def __init__(self, name: int, memory_size: float, memory: List[Task], cpu: CPU, eviction_mode: str, pop_task_mode: str):
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

    def eviction(self) -> None:
        logging.debug(f"Worker {self.name}: Starting eviction with mode {self.eviction_mode}.")
        if self.eviction_mode == EVICTION_LRU:
            self.eviction_LRU()
        elif self.eviction_mode == EVICTION_NEW_V1:
            self.eviction_new_v1()
        elif self.eviction_mode == EVICTION_MEMORY_THRESHOLD:
            self.eviction_by_memory_usage(memory_threshold=0.7, free_memory_percentage=0.4)  # X% и Y%
        logging.debug(f"Worker {self.name}: Eviction completed.")

    def pop_task(self, workers: List['Worker']) -> None:
        logging.debug(f"Worker {self.name}: Popping task with mode {self.pop_task_mode}.")
        if self.pop_task_mode == POP_TASK_DMDASD:
            self.pop_task_dmdasd(workers)
        elif self.pop_task_mode == POP_TASK_NEW_V1:
            self.pop_task_new_v1(workers)
        elif self.pop_task_mode == POP_TASK_FUNCTION1: 
            self.pop_task_function1(workers)
        logging.debug(f"Worker {self.name}: Task popped successfully.")

    def preload_data_for_tasks(self, workers: List['Worker'], preload_count: int) -> None:
        '''
        Preloads data for the current task and the next X tasks in the queue.
        Ensures that data required for the current task is not evicted.
        '''
        if not self.current_task:
            return

        # Collect data needed for the current task
        current_task_data = self.current_task.depends_on

        # Collect data for the next X tasks in the queue
        preload_tasks = [task for task in self.queue if task.status == STATUS_READY][:preload_count]
        preload_data = set()
        for task in preload_tasks:
            preload_data.update(task.depends_on)

        # Load data for the current task and preload tasks
        for data in preload_data:
            if data not in self.memory.memory and data not in current_task_data:
                self.load_data(data, workers)
            
    def pop_task_function1(self, workers: List['Worker']) -> None:
        if self.current_task:
            self.queue.remove(self.current_task)
            self.current_task.status = STATUS_DONE
            self.memory.memory.append(self.current_task)
            self.work_time += self.current_task.task_duration
            
        def can_execute(task: Task) -> bool:
            return all(data.status == STATUS_DONE for data in task.depends_on)

        self.current_task = next((task for task in self.queue if task.status == STATUS_READY or can_execute(task)), None)

        if not self.current_task:
            return
   
        self.preload_data_for_tasks(workers, preload_count=50)  
        
        for d in self.current_task.depends_on:
            if d not in self.memory.memory:
                self.load_data(d, workers)
                
        self.update_useless_data(self.current_task.depends_on)
    def eviction_LRU(self) -> None:
        '''
        Least Recently Used eviction policy
        By default in StarPU scheduling policies
        Evicts data from worker memory that has not been used for the longest time
        '''
        logging.debug(f"Worker {self.name}: Performing LRU eviction.")
        if self.memory.memory:
            data = self.memory.memory.pop(0)
            self.cpu.memory.append(data)
            self.work_time += data.size / TIME_DELIVERY_DATA
            logging.info(f"Worker {self.name}: Evicted data {data.id} using LRU policy.")

    def pop_task_dmdasd(self, workers: List['Worker']) -> None:
        '''
        Select the first task from the queue for which all the depends on tasks are done
        '''
       
        if self.current_task:
            self.queue.remove(self.current_task)
            self.current_task.status = STATUS_DONE
            self.memory.memory.append(self.current_task)
            self.work_time += self.current_task.task_duration

        def can_execute(task: Task) -> bool:
            return all(data.status == STATUS_DONE for data in task.depends_on)

        self.current_task = next((task for task in self.queue if task.status == STATUS_READY or can_execute(task)), None)

        if not self.current_task:
            return

        for d in self.current_task.depends_on:
            if d not in self.memory.memory:
                self.load_data(d, workers)

        self.update_useless_data(self.current_task.depends_on)

    def eviction_new_v1(self) -> None:
        '''
        A place to develop a new policy of eviction from the worker's memory.
        When there is not enough space to load any new data.
        '''
        pass

    def pop_task_new_v1(self, workers: List['Worker']) -> None:
        '''
        Select the first task for which all depends on tasks are already in the worker's memory.
        If there is no such task, then select the first task from the queue for which all the depends on tasks are done
        '''
        if self.current_task:
            self.queue.remove(self.current_task)
            self.current_task.status = STATUS_DONE
            self.memory.memory.append(self.current_task)
            self.work_time += self.current_task.task_duration

        done_data = set(task.id for task in self.memory.memory)

        def can_execute_in_mem(task: Task) -> bool:
            return all(data.status == STATUS_DONE and data.id in done_data for data in task.depends_on)

        self.current_task = next((task for task in self.queue if can_execute_in_mem(task)), None)

        def can_execute(task: Task) -> bool:
            return all(data.status == STATUS_DONE for data in task.depends_on)

        if not self.current_task:
            self.current_task = next((task for task in self.queue if task.status == STATUS_READY or can_execute(task)), None)

        if not self.current_task:
            return

        for d in self.current_task.depends_on:
            if d not in self.memory.memory:
                self.load_data(d, workers)

        self.update_useless_data(self.current_task.depends_on)

    def check_busy_space(self) -> float:
        '''
        A function that returns the current amount of occupied memory on the worker.
        '''
        self.busy_space = sum(elem.size for elem in self.memory.memory)
        logging.debug(f"Worker {self.name}: Current memory usage: {self.busy_space}.")
        return self.busy_space

    def update_useless_data(self, data_need_to_work: List[Task]) -> None:
        '''
        A function that, at the task's start time, updates the least recently used counter for each piece of data in memory.
        '''
        logging.debug(f"Worker {self.name}: Updating LRU counters for memory data.")
        for task in self.memory.memory:
            if task not in data_need_to_work:
                task.unused_time += 1
            else:
                task.unused_time = 0
        self.memory.memory = sorted(self.memory.memory, key=lambda x: x.unused_time, reverse=True)
        # logging.debug(f"Worker {self.name}: LRU counters updated.")

    def load_data(self, data: Task, workers: List['Worker']) -> None:
        '''
        A function that has as input a data that needs to be loaded into the worker's memory.
        First, it checks whether there is an opportunity to download, and,
        if necessary, evicts (according to the eviction policy) data from the worker's memory
        until there is enough space for new data.
        After that, it loads the data into the worker's memory and deletes the data from the CPU's memory.
        The variables for collecting statistics (runtime and number of loads) are increased accordingly.
        '''
        logging.debug(f"Worker {self.name}: Attempting to load data {data.id} of size {data.size}.")
        while data.size + self.check_busy_space() > self.memory.memory_size:
            logging.warning(f"Worker {self.name}: Memory full. Current usage: {self.check_busy_space()}. Evicting data.")
            self.eviction()
        self.memory.memory.append(data)
        if data in self.cpu.memory:
            self.cpu.memory.remove(data)
        else:
            for w in workers:
                if w.name != self.name and data in w.memory.memory:
                    w.memory.memory.remove(data)
        self.work_time += data.size / TIME_DELIVERY_DATA
        self.n_load += 1
        logging.info(f"Worker {self.name}: Data {data.id} loaded successfully. Current memory usage: {self.check_busy_space()}.")

    def eviction_by_memory_usage(self, memory_threshold: float, free_memory_percentage: float) -> None:
        '''
        Evicts tasks from GPU memory if the memory usage exceeds the given threshold (X%).
        Frees up the specified percentage (Y%) of memory.
        
        :param memory_threshold: The memory usage threshold (e.g., 0.8 for 80%).
        :param free_memory_percentage: The percentage of memory to free (e.g., 0.2 for 20%).
        '''
        logging.debug(f"Worker {self.name}: Checking memory usage for threshold-based eviction.")
        current_usage = self.check_busy_space() / self.memory.memory_size
        if current_usage > memory_threshold:
            logging.warning(f"Worker {self.name}: Memory usage {current_usage:.2f} exceeds threshold {memory_threshold}.")
            # Calculate the amount of memory to free
            memory_to_free = self.memory.memory_size * free_memory_percentage
            freed_memory = 0

            # Evict tasks until the required memory is freed
            while freed_memory < memory_to_free and self.memory.memory:
                # Evict the least recently used task (first in the sorted list)
                task_to_evict = self.memory.memory.pop(0)
                self.cpu.memory.append(task_to_evict)
                self.work_time += task_to_evict.size / TIME_DELIVERY_DATA
                freed_memory += task_to_evict.size
                logging.info(f"Worker {self.name}: Evicted data {task_to_evict.id}. Freed {freed_memory} bytes.")
        else:
            logging.debug(f"Worker {self.name}: Memory usage {current_usage:.2f} is within acceptable limits.")