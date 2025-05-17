TIME_DELIVERY_DATA = 13*1024*1024*1024  # размер/скорость 13Гигабит в  сек 
GPU_MEMORY_SIZE = 2684354560  # 80 gb = 85899345920 byte 60 gb = 64424509440 byte 40 gb = 42949672960 
                              # 20 gb = 21474836480 10 gb = 10737418240 5 gb = 5368709120 2.5 gb 2684354560
N_WORKERS = 1

###

STATUS_DONE = 'Done'
STATUS_PROGRESS = 'In Progress'
STATUS_WAITING = 'Waiting'
STATUS_INIT = 'Init'
STATUS_READY = 'Ready'
EVICTION_LRU = 'EVICTION_LRU'
EVICTION_MRU = 'EVICTION_MRU'
EVICTION_THRESHOLD = 'EVICTION_THRESHOLD'
EVICT_BY_UNUSED_TH = 'EVICT_BY_UNUSED_TH'
EVICT_AFTER_TASK = 'EVICT_AFTER_TASK'
POP_TASK_GRAPH_TEST = 'POP_TASK_GRAPH_TEST'
POP_TASK_NEW_V2 = 'POP_TASK_NEW_V2'
PUSH_TASK_GRAPH_TEST = 'PUSH_TASK_GRAPH_TEST'
PUSH_TASK_MPHASE_TILE = 'PUSH_TASK_MPHASE_TILE'

GRAPH_TEST_DESCENDANTS = 0 # 0 - число потомков у вершины, 
                           # 1 - глубина вершины в графе StarPU, 2 - глубина вершины в графе наша, 
                           # 3 - прямой порядок топологической сортировки, 4 - обратный порядок 
N_TILE_HYPER = 1
N_TILE = 1

PRELOAD_N = 1

LRU_THRESHOLD = 15
MEMORY_EVICT_PERSENT = 70