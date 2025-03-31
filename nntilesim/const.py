TIME_DELIVERY_DATA = 13*1024*1024*1024  # размер/скорость 13Гигабит в  сек 

###

STATUS_DONE = 'Done'
STATUS_PROGRESS = 'In Progress'
STATUS_WAITING = 'Waiting'
STATUS_INIT = 'Init'
STATUS_READY = 'Ready'
EVICTION_LRU = 'LRU'
POP_TASK_GRAPH_TEST = 'graph_test'
PUSH_TASK_GRAPH_TEST = 'graph_test'
PUSH_TASK_MPHASE_TILE = 'push_new_v2'
GPU_MEMORY_SIZE = 64424509440 # 80 gb = 85899345920 byte 60 gb = 64424509440 byte 40 gb = 42949672960 20 gb = 21474836480
N_WORKERS = 1
GRAPH_TEST_DESCENDANTS = 4 # 0 - число потомков у вершины, 
                           # 1 - глубина вершины в графе StarPU, 2 - глубина вершины в графе наша, 
                           # 3 - прямой порядок топологической сортировки, 4 - обратный порядок 
N_TILE_HYPER = 1
N_TILE = 4