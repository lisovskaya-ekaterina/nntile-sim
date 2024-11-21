from protes import protes
import numpy as np

from functools import lru_cache
from nntilesim.tasks_generator import generate_task
from nntilesim.Worker import Worker
from nntilesim.Scheduler import Scheduler
from nntilesim.CPU import CPU

import time

class BmBudgetOverException(Exception):
    """
    taken from https://github.com/AIRI-Institute/teneva_bm/blob/main/teneva_bm/bm.py
    """
    def __init__(self, m, is_cache=False):
        self.m = m
        self.is_cache = is_cache

        self.message = 'Computation budget '
        if self.is_cache:
            self.message += 'for cache '
        self.message += f'(m={self.m}) exceeded'

        super().__init__(self.message)



def my_cach(*args, **kwargs):
    """
    modified from https://github.com/G-Ryzhakov/HTbb/blob/main/utils.py
    """
    def my_cach_inner(func, cache_size=9.E+6, cache_max=5*9.E+6, log=False,
                      is_max=True, check_cach=True, dtype=int):
        func = lru_cache(maxsize=int(cache_size))(func)
        def f(I):
            hits = func.cache_info().hits
            if check_cach:
                if hits >= cache_max:
                    raise BmBudgetOverException(hits, is_cache=True)

            I = np.asarray(I, dtype=dtype)
            if I.ndim == 1:
                y = func(tuple(I))
                if (is_max and y > f.loc_max) or (not is_max and y < f.loc_max):
                    f.loc_max = y
                    # print(f">>> loc max: {f.loc_max}")

                if (is_max and y > f.max) or (not is_max and y < f.max):
                    f.max = y
                    if log:
                        print(f">>> max: {f.max} ({func.cache_info().misses} evals) (I = {I})")
                return y
            elif I.ndim == 2:
                y = [func(tuple(i)) for i in I]
                max_y = max(y) if is_max else min(y)
                if (is_max and max_y > f.loc_max) or (not is_max and max_y < f.loc_max):
                    f.loc_max = max_y
                    # print(f">>> loc max: {f.loc_max}")

                if (is_max and max_y > f.max) or (not is_max and max_y < f.max):
                    f.max = max_y
                    if log:
                        print(f">>> max: {f.max}")
                return y
            else:
                raise TypeError('Bad argument')

        f.max = -np.inf if is_max else np.inf
        f.loc_max = -np.inf if is_max else np.inf
        f.func = func

        return f

    if len(args) == 1 and callable(args[0]):
        return my_cach_inner(args[0])

    return lambda func: my_cach_inner(func, *args, **kwargs)

def cache_func(f):
    def f(X):
        cache = f.cache
        res = np.empty(X.shape[0])
        for i, x in enumerate(X):
            tx = tuple(x)
            try:
                res[i] = cache[tx]
            except:
                res[i] = cache[tx] = f(x)

        return res

    f.cache = dict()
    return f


class ProtesScheduler():

    def __init__(self, n, func, d=None, rank=3, max_ask=1e5, *,
                 transpose_func=False,
                 protes_params=None, debug=False,
                 func_is_vector=False, need_cache=False):
        self.was_opt = False
        self.x, self.y = [None]*2

        assert not (func_is_vector and need_cache), "If func is vectorized, I cannot use cache"
        assert (not transpose_func) or func_is_vector, "If func is not vectorized, I cannot use transpose"

        try:
            len(n)
            n_is_list = True

        except TypeError:
            n_is_list = False
            try:
                n = int(n)
            except:
                assert False, f'bad param n : it is of type {type(n)}, but integer of listable expected'

        if n_is_list:
            if d is not None:
                if len(n) != d:
                    assert False, 'len of n do not equal to d: {len(n)} != {d}'
            else:
                d = len(n)
        else:
            if d is None:
                assert False, f'if n is not listable, d must not be None'
            n = [n]*d

        assert len(set(n))==1, f"for now we can handle only identical modes, but {n} is given ({len(set(n))} different elements in it)"

        self.d = d
        self.n = n
        self.rank = int(rank)

        if transpose_func:
            func = lambda x: func(x.T)

        if func_is_vector:
            self.func = func
        else:
            def f(I):
                return [func(i) for i in I]

            self.func = f

        if need_cache:
            raise ValueError("Not implemented")
            pass


        protes_params_all = dict(k=100, k_top=5, k_gd=1)
        if protes_params is not None:
            protes_params_all.update(protes_params)

        self.protes_params = protes_params_all
        self.max_ask = int(max_ask)
        self.debug = debug

    def optimize(self):
        self.x, self.y = protes(self.func, d=self.d, n=self.n[0], m=self.max_ask, log=self.debug, **self.protes_params)
        self.was_opt = True

    def __repr__(self):
        return f"Scheduler: dim={self.d}, range of modes={self.n[0]}, rank={self.rank}, max ask={self.max_ask},{' [debug],' if self.debug else ''} additional params: {self.protes_params}"

    def results(self):
        if self.was_opt:
            return self.x, self.y
        else:
            print("First run `optimize`, then me")
            return None


def loss(workers):
    """
    We shall MINIMIZE this
    """
    return max([i.work_time for i in workers])


def get_f_Scheduler(eviction_mode, pop_task_mode, gpu_memory_size, n_workers, logs_file_name, i_epoch, i_batch, loss=loss, log=False, log_file=False):

    def f(I):

        fn = f"f_{time.time()}"
        if log_file:
            with open(f"{fn}_in.txt",  'w') as f:
                f.write(",".join([str(int(i)) for i in I]))

        task_list, data_list = generate_task(logs_file_name, i_epoch, i_batch) # may be we can make it once, outside the function f


        cpu = CPU()
        workers = [Worker(name=i,
                    memory_size=gpu_memory_size,
                    memory=[],
                    cpu=cpu,
                    eviction_mode=eviction_mode,
                    pop_task_mode=pop_task_mode)
                          for i in range(n_workers)
            ]


        for task, wrk in zip(task_list.values(), I):
            task.best_worker = int(wrk)

        scheduler = Scheduler("random")
        scheduler.do_work(task_list, data_list, workers)

        empty_workers = 0
        for worker in workers:
            if log:
                print(f'{worker.name} : {len(worker.queue)} tasks')
        while empty_workers != len(workers):
            for worker in workers:
                worker.pop_task(workers)
                if len(worker.queue) == 0:
                    empty_workers += 1

        res = loss(workers)
        print(f"loss = {res}")
        if log_file:
            with open(f"{fn}_out.txt",  'w') as f:
                f.write(f"loss: {res}" + "\n")
        return res

    return f


