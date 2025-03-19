from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU
from nntilesim.const import *
import time 
import argparse
from nntilesim.tasks_generator import generate_task

def main(eviction_mode, pop_task_mode, push_task_mode, gpu_memory_size, n_workers, logs_file_name, i_batch, graph_test_descendants, n_minibatch_hyper):
    print('Configs:')
    print(f"eviction_mode: {eviction_mode}")
    print(f"pop_task_mode: {pop_task_mode}")
    print(f"push_task_mode: {push_task_mode}")
    print(f"gpu_memory_size: {gpu_memory_size / 1024 / 1024 / 1024} Gb")
    print(f"n_workers: {n_workers}")
    print(f"i_batch: {i_batch}")
    if push_task_mode == PUSH_TASK_GRAPH_TEST:
        print(f"graph_test_descendants: {graph_test_descendants}")
    elif push_task_mode == PUSH_TASK_MPHASE_MINIBATCH:
        print(f"n_minibatch_hyper: {n_minibatch_hyper}")
    print('-'*10)

    start_time = time.time()

    task_list, data_list = generate_task(logs_file_name, i_batch, graph_test_descendants)

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

    scheduler.do_work(task_list, data_list, cpu, n_minibatch_hyper)

    i = 0
    while len(scheduler.prio_gpu) > 0:
        for worker in workers:
            worker.pop_task(workers, scheduler)
            i += 1

    print(f'Execute {i - 1} tasks')
            
    for worker in workers:
        print(f'{worker.name} : work time : {worker.work_time} ')
        print(f'{worker.name} : n_load : {worker.n_load}')
        
    print(f"{time.time() - start_time} seconds")
    print('pop task -- done')
    print('-'*10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulation's config")
    parser.add_argument("--eviction_mode", type=str, default=EVICTION_LRU, help="Eviction mode: LRU, evict_new_v1")
    parser.add_argument("--pop_task_mode", type=str, default=POP_TASK_GRAPH_TEST, help="pop_task mode: graph_test")
    parser.add_argument("--push_task_mode", type=str, default=PUSH_TASK_MPHASE_MINIBATCH, help="push_task mode: graph_test, new_v2")
    parser.add_argument("--gpu_memory_size", type=int, default=GPU_MEMORY_SIZE, help="GPU memory size (bytes)")
    parser.add_argument("--n_workers", type=int, default=N_WORKERS, help="Number of workers (GPU)")
    parser.add_argument("--logs_file_name", type=str, default='iteration_tasks.rec', help="Name of file with logs: *.rec")
    parser.add_argument("--i_batch", type=int, default=0, help="Number of batch for simulation")
    parser.add_argument("--graph_test_descendants", type=int, default=GRAPH_TEST_DESCENDANTS, help="Hyperparameter of graph_test policy: 0, 1, 2")
    parser.add_argument("--n_minibatch_hyper", type=int, default=N_MINIBATCH_HYPER, help="Hyperparameter of new_v2 policy")
    
    args = parser.parse_args()
    
    main(
        args.eviction_mode,
        args.pop_task_mode,
        args.push_task_mode,
        args.gpu_memory_size,
        args.n_workers,
        args.logs_file_name,
        args.i_batch,
        args.graph_test_descendants,
        args.n_minibatch_hyper
        )