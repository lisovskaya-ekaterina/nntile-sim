from .const import *
from collections import defaultdict
import time

def key_wrapper(task):
    return task.priority

class Scheduler: 
    def __init__(self, push_task_mode):
        self.push_task_mode = push_task_mode
        self.prio_gpu = []
    
    def push_task(self, task, cpu, data_list):

        for data_id in task.depends_on:
            data_item = data_list.get(data_id)
            if data_item and data_item.status == STATUS_INIT:
                cpu.memory.append(data_item)
                data_item.status = STATUS_DONE
                
        self.prio_gpu.append(task)
    
    def do_work(self, task_list, data_list, cpu, n_minibatch_hyper):
        start_time = time.time()
        if self.push_task_mode == PUSH_TASK_GRAPH_TEST:
            for task in task_list.values():
                self.push_task(task, cpu, data_list)
            self.prio_gpu = sorted(self.prio_gpu, key = lambda x: x.priority, reverse = True)

        elif self.push_task_mode == PUSH_TASK_NEW_V2:
            bag_of_tasks = defaultdict(list)
            for task in task_list.values():
                bag_of_tasks[task.i_minibatch].append(task)

            n_bags = len(bag_of_tasks)

            for task in bag_of_tasks[0]:
                task.priority = 0
                self.push_task(task, cpu, data_list)

            for i in range(1, n_bags - 1, n_minibatch_hyper):
                for j in range(len(bag_of_tasks[i])):
                    for k in range(min(n_minibatch_hyper, n_bags - 1 - i)):
                        bag_of_tasks[i + k][j].priority = 0
                        self.push_task(bag_of_tasks[i + k][j], cpu, data_list)


            for task in bag_of_tasks[max(bag_of_tasks.keys())]:
                task.priority = 0
                self.push_task(task, cpu, data_list)

        data_task_list= {**task_list, **data_list}

        for item in self.prio_gpu:
            item.depends_on = [data_task_list[d_id] for d_id in item.depends_on if d_id in data_task_list]

        print(f'{len(self.prio_gpu)} tasks in prio_gpu')
        print(f'{time.time() - start_time} seconds\npush task -- done.')
        print('-'*10)