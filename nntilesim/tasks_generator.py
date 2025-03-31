from .Task import Task
from .const import *
import networkx as nx # type: ignore


def remove_spaces_from_list(lst):
    return [value for value in lst if value != '']

def generate_task(logs_file_name, i_batch, graph_test_descendants):

    task_fields = ['Name', 'DependsOn', 'JobId', 'EndTime', 'StartTime', 'Iteration', 'Modes', 'Sizes']
    data_fields = ['JobId']

    task_dict = {}
    data_dict = {}
    with open('examples/'+logs_file_name, 'r') as f:
        lines = f.read()
    
    lines = lines.split('MPIRank: -1')
    n = 0
    i_minibatch_list = set()
    for note in lines:
        note = note.split("\n")
        note = remove_spaces_from_list(note)
        dictinory = dict(subString.split(": ") for subString in note)
        
        if all(element in dictinory for element in task_fields) and dictinory['Name'][0] != '_' and dictinory['Name'] != 'task_build':
            iter = [int(elem) for elem in dictinory['Iteration'].split()]
            if len(iter) == 2 and iter[0] == i_batch:
                n += 1
                mode_letters_list = dictinory['Modes'].split()
                for i in mode_letters_list:
                    if 'W' in i:
                        index_of_w = mode_letters_list.index(i)
                size = dictinory['Sizes'].split()
                task_dict[dictinory['JobId']] = Task(id = dictinory['JobId'],
                                                    name = dictinory['Name'],
                                                    status = STATUS_INIT,
                                                    task_duration = (float(dictinory['EndTime'])-float(dictinory['StartTime'])) / 1000,
                                                    depends_on = dictinory['DependsOn'].split(' '),
                                                    size = int(size[index_of_w]),
                                                    i_batch = i_batch,
                                                    i_minibatch = iter[1]
                                                    )
                i_minibatch_list.add(iter[1])
                                                    
        elif all(element in dictinory for element in data_fields):
            data_dict[dictinory['JobId']] = Task(id = dictinory['JobId'],
                                                 name = 'DATA',
                                                 task_duration = 0,
                                                 depends_on = [],
                                                 size = 65536,
                                                 status = STATUS_INIT)
    G = nx.DiGraph()
    G.add_nodes_from(task_dict)
    
    for job_id, task in task_dict.items():
        G.add_edges_from([(dep, job_id) for dep in task.depends_on if dep in task_dict])
    
    if graph_test_descendants == 0:
        for job_id in task_dict:
            task_dict[job_id].priority = len(nx.descendants(G, job_id))
    elif graph_test_descendants == 1:
        depths = dag_depth_bottom_up(G)
        for job_id in task_dict:
            task_dict[job_id].priority = depths[job_id]
    elif graph_test_descendants == 2:
        depths = depth(G)
        for job_id in task_dict:
            task_dict[job_id].priority = depths[job_id]
    elif graph_test_descendants == 3:
        n = len(G)
        for i, node in enumerate(nx.topological_sort(G)):
            task_dict[node].priority = n - 1 - i
    elif graph_test_descendants == 4:
        for i, node in enumerate(nx.topological_sort(G)):
            task_dict[node].priority = i
            
    print(f'{n} tasks')
    print('generate task -- done. ')
    print('-'*10)
    return task_dict, data_dict, G, len(i_minibatch_list) - 1

def depth(graph, root=None):
    """Вычисляет глубину каждого узла в графе относительно заданного корня."""
    depths = {}
    if root is None:
        # Для несвязных графов нужно перебрать все компоненты связности
        for node in graph.nodes:
            if node not in depths:  #  Проверка, если узел уже обработан в другой компоненте
                _depth_recursive(graph, node, depths, 0)
    else:
         _depth_recursive(graph, root, depths, 0)
    return depths


def _depth_recursive(graph, node, depths, current_depth):
   depths[node] = current_depth
   for neighbor in graph[node]:
       if neighbor not in depths or depths[neighbor] > current_depth +1: # Проверка для корректной работы с циклами
           _depth_recursive(graph, neighbor, depths, current_depth + 1)


def reversed_topological_sort(graph):
    """
    Реализует обратную топологическую сортировку для версий NetworkX < 2.8.
    """
    return reversed(list(nx.topological_sort(graph)))

def dag_depth_bottom_up(graph):
    """
    Вычисляет глубину каждой вершины в DAG (снизу вверх).

    Args:
        graph: Объект графа networkx (должен быть DAG).

    Returns:
        Словарь, где ключи - вершины, а значения - их глубины.
    """

    depths = {}

    # Находим листовые вершины (без исходящих ребер)
    leaf_nodes = [node for node in graph.nodes() if graph.out_degree(node) == 0]

    # Устанавливаем глубину листовых вершин в 0
    for node in leaf_nodes:
        depths[node] = 0

    # Обходим граф снизу вверх
    for node in reversed_topological_sort(graph):
        if node not in depths:
            max_child_depth = 0
            for successor in graph.successors(node):
                if successor in depths:
                    max_child_depth = max(max_child_depth, depths[successor])
            depths[node] = max_child_depth + 1

    return depths