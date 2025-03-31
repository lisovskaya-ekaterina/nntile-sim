from .const import *
import time
import networkx as nx
from sklearn.cluster import SpectralClustering
import warnings
warnings.filterwarnings("ignore")

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

    def split_graph_into_parts(self, graph, num_parts):
        adjacency_matrix = nx.to_numpy_array(graph)
        
        clustering = SpectralClustering(n_clusters=num_parts, affinity='precomputed', random_state=42)
        labels = clustering.fit_predict(adjacency_matrix)
        
        subgraphs = []
        for i in range(num_parts):
            nodes_in_cluster = [node for node, label in zip(graph.nodes, labels) if label == i]
            subgraph = graph.subgraph(nodes_in_cluster).copy()
            subgraphs.append(subgraph)
        
        return subgraphs
    
    def do_work(self, task_list, data_list, cpu, n_tile_hyper, G, n_minibatch):
        start_time = time.time()
        if self.push_task_mode == PUSH_TASK_GRAPH_TEST:
            for task in task_list.values():
                self.push_task(task, cpu, data_list)
            self.prio_gpu = sorted(self.prio_gpu, key = lambda x: x.priority, reverse = True)

        elif self.push_task_mode == PUSH_TASK_MPHASE_TILE:

            G_list = [nx.DiGraph() for _ in range(n_minibatch)]
            for task in (t for t in task_list.values() if t.i_minibatch < n_minibatch):
                G_list[task.i_minibatch].add_node(task.id)

                    
            for G in G_list:
                for node in G.nodes:
                    task = task_list[node]
                    if task.depends_on:
                        for dep in task.depends_on:
                            if dep in G.nodes:
                                G.add_edge(dep, node)

            for G in G_list:
                subgraphs = self.split_graph_into_parts(G, num_parts=N_TILE)
            
                for i in range(0, N_TILE, n_tile_hyper):
                    for j in range(min([len(subgraphs[s]) for s in range(i, i + n_tile_hyper)])):
                        for k in range(min(n_tile_hyper, N_TILE - i)):
                            task_list[list(subgraphs[i + k].nodes)[j]].priority = 0
                            self.push_task(task_list[list(subgraphs[i + k].nodes)[j]], cpu, data_list)
                            task_list[list(subgraphs[i + k].nodes)[j]].status = STATUS_PROGRESS
            
            for task in task_list.values():
                if task.status == STATUS_INIT:
                    task.priority = 0
                    self.push_task(task, cpu, data_list)
                    task.status = STATUS_PROGRESS

        data_task_list= {**task_list, **data_list}

        for item in self.prio_gpu:
            item.depends_on = [data_task_list[d_id] for d_id in item.depends_on if d_id in data_task_list]

        print(f'{len(self.prio_gpu)} tasks in prio_gpu')
        print(f'{time.time() - start_time} seconds\npush task -- done.')
        print('-'*10)