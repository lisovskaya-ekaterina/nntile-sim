from src.Worker import Worker
from src.Scheduler import Scheduler
from src.CPU import CPU
from src.const import *
import time 
from src.tasks_generator import generate_task

''' 
Below: need to select eviction mode and pop task mode

EVICTION_LRU the standard eviction policy in StarPU
POP_TASK_DMDASD the pop policy of dmdasd Scheduler
PUSH_TASK_DMDASD the push policy of dmdasd Scheduler
'''

eviction_mode = EVICTION_LRU
# pop_task_mode = POP_TASK_DMDASD
# eviction_mode = EVICTION_NEW_V1
pop_task_mode = POP_TASK_NEW_V1
# push_task_mode = PUSH_TASK_DMDASD
push_task_mode = PUSH_TASK_NEW_V1



task_list = generate_task()
print(len(task_list))

cpu = CPU()
workers = [Worker(name=0, 
                  memory_size=GPU_MEMORY_SIZE,
                  memory=[], 
                  cpu=cpu, 
                  eviction_mode=eviction_mode,
                  pop_task_mode=pop_task_mode)]
scheduler = Scheduler(push_task_mode)

scheduler.do_work(task_list, workers)

max_s = len(workers[0].queue)

for worker in workers:
    while True:
        worker.pop_task()
        print(f'{max_s-len(worker.queue)} of {max_s} ')
        if len(worker.queue) == 0:
            break
    print(f'{worker.name} : work time : {worker.work_time}')
    print(f'{worker.name} : n_load : {worker.n_load}')