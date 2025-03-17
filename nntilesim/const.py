TIME_DELIVERY_DATA = 13*1024*1024*1024  # размер/скорость 13Гигабит в  сек 

###

STATUS_DONE = 'Done'
STATUS_PROGRESS = 'In Progress'
STATUS_WAITING = 'Waiting'
STATUS_INIT = 'Init'
STATUS_READY = 'Ready'
EVICTION_LRU = 'LRU'
POP_TASK_GRAPH_TEST = 'graph_test'
POP_TASK_NEW_V2 = 'pop_new_v2'
PUSH_TASK_GRAPH_TEST = 'graph_test'
PUSH_TASK_NEW_V2 = 'push_new_v2'
GPU_MEMORY_SIZE = 85899345920 # 80 gb = 85899345920 byte 60 gb = 64424509440 byte 40 gb = 42949672960 20 gb = 21474836480
N_WORKERS = 1
GRAPH_TEST_DESCENDANTS = 1 # 0 - число потомков у вершины, 1 - глубина вершины в графе StarPU
                           # 2 - глубина вершины в графе наша, 3 - индекс вершины в отсортированном в топологическом порядке графе
N_MINIBATCH_HYPER = 7