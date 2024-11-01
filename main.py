from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU
from nntilesim.const import *
import time 
import argparse
from nntilesim.tasks_generator import generate_task


def main(eviction_mode, pop_task_mode, push_task_mode, gpu_memory_size, n_workers, logs_file_name):
    print('Configs:')
    print(f"eviction_mode: {eviction_mode}")
    print(f"pop_task_mode: {pop_task_mode}")
    print(f"push_task_mode: {push_task_mode}")
    print(f"gpu_memory_size: {gpu_memory_size / 1024 / 1024 / 1024} Gb")
    print(f"n_workers: {n_workers}")
    print('-'*10)

    start_time = time.time()

    task_list = generate_task(logs_file_name)

    cpu = CPU()
    workers = [Worker(name=i, 
                    memory_size=gpu_memory_size,
                    memory=[], 
                    cpu=cpu, 
                    eviction_mode=eviction_mode,
                    pop_task_mode=pop_task_mode)
                    for i in range(n_workers)
            ]

    scheduler = Scheduler(push_task_mode)

    scheduler.do_work(task_list, workers)

    empty_workers = 0
    while empty_workers != len(workers):
        for worker in workers:
            worker.pop_task(workers)
            if len(worker.queue) == 0:
                empty_workers += 1
    for worker in workers:        
        print(f'{worker.name} : work time : {worker.work_time} ')
        print(f'{worker.name} : n_load : {worker.n_load}\n')
        
        
    print(f"--- {time.time() - start_time} seconds ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Configs")
    parser.add_argument("--eviction_mode", type=str, default=EVICTION_LRU, help="Eviction mode")
    parser.add_argument("--pop_task_mode", type=str, default=POP_TASK_DMDASD, help="pop_task mode")
    parser.add_argument("--push_task_mode", type=str, default=PUSH_TASK_DMDASD, help="push_task mode")
    parser.add_argument("--gpu_memory_size", type=int, default=GPU_MEMORY_SIZE, help="GPU memory size (bytes)") 
    parser.add_argument("--n_workers", type=int, default=N_WORKERS, help="Number of workers (GPU)")
    parser.add_argument("--logs_file_name", type=str, default='tasks-2.rec', help="Name of file with logs")  

    args = parser.parse_args()
    
    main(args.eviction_mode, args.pop_task_mode, args.push_task_mode, args.gpu_memory_size, args.n_workers, args.logs_file_name)