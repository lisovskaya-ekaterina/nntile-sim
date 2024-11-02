from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU
from nntilesim.const import *
from nntilesim.random_sched import random_sched
import time
from nntilesim.tasks_generator import generate_task
import multiprocessing

def process_task(eviction_mode, pop_task_mode, push_task_mode, gpu_memory_size, n_workers, logs_file_name, i_epoch, i_batch):
    print('Configs:')
    print(f"eviction_mode: {eviction_mode}")
    print(f"pop_task_mode: {pop_task_mode}")
    print(f"push_task_mode: {push_task_mode}")
    print(f"gpu_memory_size: {gpu_memory_size / 1024 / 1024 / 1024} Gb")
    print(f"n_workers: {n_workers}")
    print(f"i_epoch: {i_epoch}")
    print(f"i_batch: {i_batch}")
    print('-'*10)

    start_time = time.time()

    task_list, data_list = generate_task(logs_file_name, i_epoch, i_batch)

    cpu = CPU()
    workers = [Worker(name=i, 
                    memory_size=gpu_memory_size,
                    memory=[], 
                    cpu=cpu, 
                    eviction_mode=eviction_mode,
                    pop_task_mode=pop_task_mode)
                    for i in range(n_workers)
            ]
    if push_task_mode == PUSH_TASK_RANDOM:
        random_sched(task_list, workers)
        
    scheduler = Scheduler(push_task_mode)

    scheduler.do_work(task_list, data_list, workers)

    empty_workers = 0
    for worker in workers:
        print(f'{worker.name} : {len(worker.queue)} tasks')
    while empty_workers != len(workers):
        for worker in workers:
            worker.pop_task(workers)
            if len(worker.queue) == 0:
                empty_workers += 1
    for worker in workers:        
        print(f'{worker.name} : work time : {worker.work_time} ')
        print(f'{worker.name} : n_load : {worker.n_load}')
        
    print(f"{time.time() - start_time} seconds")
    print('pop task -- done')
    print('-'*10)

process_modes = [
    ('LRU', 'dmdasd', 'random', 85899345920, 2, 'tasks-2.rec', 0, 0),
    ('LRU', 'dmdasd', 'random', 85899345920, 2, 'tasks-2.rec', 0, 1),
]

if __name__ == "__main__":
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.starmap(process_task, process_modes)