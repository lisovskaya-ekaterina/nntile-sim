from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU
from nntilesim.const import *
from nntilesim.PROTES_shed import get_f_Scheduler, ProtesScheduler

import time
import argparse
from nntilesim.tasks_generator import generate_task


def loss(workers):
    """
    We shall MINIMIZE this
    """
    return max([i.work_time for i in workers])



def main(eviction_mode, pop_task_mode,  gpu_memory_size, n_workers, logs_file_name, i_epoch, i_batch, log=False):
    print('Configs:')
    print(f"eviction_mode: {eviction_mode}")
    print(f"pop_task_mode: {pop_task_mode}")
    print(f"gpu_memory_size: {gpu_memory_size / 1024 / 1024 / 1024} Gb")
    print(f"n_workers: {n_workers}")
    print(f"i_epoch: {i_epoch}")
    print(f"i_batch: {i_batch}")
    print('-'*10)

    f = get_f_Scheduler(eviction_mode, pop_task_mode, gpu_memory_size, n_workers, logs_file_name, i_epoch, i_batch, loss=loss, log=log)
    task_list, data_list = generate_task(logs_file_name, i_epoch, i_batch)

    p_shed = ProtesScheduler(n=n_workers, func=f, d=len(task_list), rank=3, max_ask=1e3,  debug=True, protes_params=dict(k=100, k_top=5, k_gd=1))
    p_shed.optimize()
    p_shed.results() # it will print the results



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulation's config")
    parser.add_argument("--eviction_mode", type=str, default=EVICTION_LRU, help="Eviction mode: LRU, evict_new_v1")
    parser.add_argument("--pop_task_mode", type=str, default=POP_TASK_DMDASD, help="pop_task mode: dmdasd, pop_new_v1")
    parser.add_argument("--gpu_memory_size", type=int, default=GPU_MEMORY_SIZE, help="GPU memory size (bytes)")
    parser.add_argument("--n_workers", type=int, default=N_WORKERS, help="Number of workers (GPU)")
    parser.add_argument("--logs_file_name", type=str, default='tasks-2.rec', help="Name of file with logs: *.rec")
    parser.add_argument("--i_epoch", type=int, default=0, help="Number of epoch for simulation")
    parser.add_argument("--i_batch", type=int, default=0, help="Number of batch for simulation")

    args = parser.parse_args()

    main(
        args.eviction_mode,
        args.pop_task_mode,
        args.gpu_memory_size,
        args.n_workers,
        args.logs_file_name,
        args.i_epoch,
        args.i_batch,
        log=True
        )
