from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU
from nntilesim.const import *
from nntilesim.random_sched import random_sched
import time
import argparse
from nntilesim.tasks_generator import generate_task

def main(eviction_mode, pop_task_mode, push_task_mode, gpu_memory_size, n_workers, logs_file_name, i_epoch, i_batch):
    print('Configs:')
    print(f"eviction_mode: {eviction_mode}")
    print(f"pop_task_mode: {pop_task_mode}")
    print(f"push_task_mode: {push_task_mode}")
    print(f"gpu_memory_size: {gpu_memory_size / 1024 / 1024 / 1024} Gb")
    print(f"n_workers: {n_workers}")
    print(f"i_epoch: {i_epoch}")
    print(f"i_batch: {i_batch}")
    print('-'*10)

    assert push_task_mode == PUSH_TASK_RANDOM, "This is only test for user-defined task assignment"
    assert n_workers == 2, "Only to workers for now"

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

    fn = "f_in_fail.txt"
    with open(fn, "r") as f:
        task_ass = [int(i) for i in f.read().split(',')]

    for task, assignment in zip(task_list.values(), task_ass):
        task.best_worker = assignment

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulation's config")
    parser.add_argument("--eviction_mode", type=str, default=EVICTION_LRU, help="Eviction mode: LRU, evict_new_v1")
    parser.add_argument("--pop_task_mode", type=str, default=POP_TASK_DMDASD, help="pop_task mode: dmdasd, pop_new_v1")
    parser.add_argument("--push_task_mode", type=str, default=PUSH_TASK_RANDOM, help="push_task mode: dmdasd, push_new_v1, random")
    parser.add_argument("--gpu_memory_size", type=int, default=GPU_MEMORY_SIZE, help="GPU memory size (bytes)")
    parser.add_argument("--n_workers", type=int, default=2, help="Number of workers (GPU)")
    parser.add_argument("--logs_file_name", type=str, default='tasks-2.rec', help="Name of file with logs: *.rec")
    parser.add_argument("--i_epoch", type=int, default=0, help="Number of epoch for simulation")
    parser.add_argument("--i_batch", type=int, default=0, help="Number of batch for simulation")

    args = parser.parse_args()

    main(
        args.eviction_mode,
        args.pop_task_mode,
        args.push_task_mode,
        args.gpu_memory_size,
        args.n_workers,
        args.logs_file_name,
        args.i_epoch,
        args.i_batch
        )
