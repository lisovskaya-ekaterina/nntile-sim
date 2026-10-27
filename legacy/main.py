from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU
from nntilesim.const import *
from nntilesim.random_sched import random_sched
import time 
import argparse
from nntilesim.tasks_generator import generate_task
from nntilesim.tasks_generator_old import generate_task_old

def main(eviction_mode, pop_task_mode, push_task_mode, gpu_memory_size, n_workers, logs_file_name, i_epoch, i_batch, task_generator_mode):
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

    if task_generator_mode == 'new':
        task_list, data_list = generate_task(logs_file_name, i_epoch, i_batch)
    elif task_generator_mode == 'old':
        task_list, data_list = generate_task_old(logs_file_name)

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

    all_tasks_completed = False
    for worker in workers:
        print(f'{worker.name} : {len(worker.queue)} tasks')
    while not all_tasks_completed:
        for worker in workers:
            worker.pop_task(workers)
            if all(len(worker.queue) == 0 for worker in workers):
                all_tasks_completed = True
                break
    for worker in workers:
        print(f'{worker.name} : work time : {worker.work_time} ')
        print(f'{worker.name} : n_load : {worker.n_load}')
        
    print(f"{time.time() - start_time} seconds")
    print('pop task -- done')
    print('-'*10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulation's config")
    parser.add_argument("--eviction_mode", type=str, default=EVICTION_LRU, help="Eviction mode: LRU, evict_new_v1")
    parser.add_argument("--pop_task_mode", type=str, default=POP_TASK_DMDASD, help="pop_task mode: dmdasd, pop_new_v1")
    parser.add_argument("--push_task_mode", type=str, default=PUSH_TASK_DMDASD, help="push_task mode: dmdasd, push_new_v1, random")
    parser.add_argument("--gpu_memory_size", type=int, default=GPU_MEMORY_SIZE, help="GPU memory size (bytes)") 
    parser.add_argument("--n_workers", type=int, default=N_WORKERS, help="Number of workers (GPU)")
    parser.add_argument("--logs_file_name", type=str, default='tasks-2.rec', help="Name of file with logs: *.rec") 
    parser.add_argument("--i_epoch", type=int, default=0, help="Number of epoch for simulation") 
    parser.add_argument("--i_batch", type=int, default=0, help="Number of batch for simulation") 
    parser.add_argument("--task_generator_mode", type=str, default='new', help="new -- for new developments, old -- for the old version of nntile") 

    args = parser.parse_args()
    
    main(
        args.eviction_mode,
        args.pop_task_mode,
        args.push_task_mode,
        args.gpu_memory_size,
        args.n_workers,
        args.logs_file_name,
        args.i_epoch,
        args.i_batch,
        args.task_generator_mode
        )