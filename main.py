from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU
from nntilesim.const import *
import time 
from nntilesim.tasks_generator import generate_task

''' 
Below: need to select eviction mode and pop task mode

EVICTION_LRU the standard eviction policy in StarPU
POP_TASK_DMDASD the pop policy of dmdasd Scheduler
PUSH_TASK_DMDASD the push policy of dmdasd Scheduler
'''

eviction_mode = EVICTION_LRU
pop_task_mode = POP_TASK_DMDASD
# eviction_mode = EVICTION_NEW_V1
# pop_task_mode = POP_TASK_NEW_V1
push_task_mode = PUSH_TASK_DMDASD
# push_task_mode = PUSH_TASK_NEW_V1

start_time = time.time()

task_list = generate_task()

cpu = CPU()
workers = [Worker(name=i, 
                  memory_size=GPU_MEMORY_SIZE,
                  memory=[], 
                  cpu=cpu, 
                  eviction_mode=eviction_mode,
                  pop_task_mode=pop_task_mode)
                  for i in range(N_WORKERS)
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