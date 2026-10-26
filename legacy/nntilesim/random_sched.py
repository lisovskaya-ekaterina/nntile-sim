import numpy as np

def random_sched(task_list, workers):
    for task in task_list.values():
        task.best_worker = np.random.choice([i for i in range(len(workers))])