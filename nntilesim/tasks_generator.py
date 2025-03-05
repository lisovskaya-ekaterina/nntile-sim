from .Task import Task
from .const import *
import networkx as nx

def get_graph (logs_file_name):
    # Читаем файл и обрабатываем строки
    records = []
    with open(logs_file_name, 'r', encoding='utf-8') as file:
        record = {}
        for line in file:
            line = line.strip()  # Убираем лишние пробелы и символы перевода строки
            if line:  # Если строка не пустая
                key, value = line.split(': ', 1)  # Разделяем по ": "
                record[key] = value
            else:  # Пустая строка означает конец записи
                if record:  # Если накоплена запись
                    records.append(record)
                    record = {}  # Очищаем для следующей записи
    
    my_records = []
    JobIDs = []
    names_list = set()
    for record in records:
        if 'Name' in record.keys() and record['Name'][0] != '_' and record['Iteration'] == '0 0':
            my_records.append(record)
            JobIDs.append(record['JobId'])
            names_list.add(record['Name'])
    

    if record:  # Добавляем последнюю запись, если файл не заканчивается пустой строкой
        records.append(record)

    G = nx.DiGraph()

    for record in my_records:
        job_id = record["JobId"]
        depends_on = record.get("DependsOn")

        # Добавляем вершину
        G.add_node(job_id, name=record["Name"])

        # Если есть зависимости, добавляем ориентированные рёбра для каждого из DependsOn
        if depends_on:
            # Если DependsOn несколько, разделяем по пробелам или запятой
            dependencies = depends_on.split()  # Разделяем по пробелам, если зависимость в виде списка
            for dep in dependencies:
                if dep in JobIDs:
                    G.add_edge(dep, job_id)  # Добавляем ориентированное ребро от зависимой задачи
    
    print(len(G))
    
    return G
        
def remove_spaces_from_list(lst):
    return [value for value in lst if value != '']

def generate_task(logs_file_name, i_epoch, i_batch):
    G = get_graph(f'examples/{logs_file_name}')
    task_fields = ['Name', 'DependsOn', 'JobId', 'EndTime', 'StartTime', 'Iteration', 'Modes', 'Sizes']
    data_fields = ['JobId']

    task_dict = {}
    data_dict = {}

    with open(f'examples/{logs_file_name}', 'r') as f:
        lines = f.read().split('MPIRank: -1')

    n = 0
    for note in lines:
        note = remove_spaces_from_list(note.split("\n"))
        try:
            dictionary = dict(subString.split(": ") for subString in note)
        except ValueError:
            continue  # Skip lines that don't split correctly

        if all(field in dictionary for field in task_fields) and dictionary['Name'][0] != '_' and dictionary['Name'] != 'task_build':
            iter_values = [int(elem) for elem in dictionary['Iteration'].split()]
            if iter_values[0] == i_epoch and iter_values[1] == i_batch:
                n += 1
                mode_letters_list = dictionary['Modes'].split()
                index_of_w = next((i for i, mode in enumerate(mode_letters_list) if 'W' in mode), None)
                if index_of_w is not None:
                    size = dictionary['Sizes'].split()
                    task_dict[dictionary['JobId']] = Task(
                        id=dictionary['JobId'],
                        name=dictionary['Name'],
                        task_duration=(float(dictionary['EndTime']) - float(dictionary['StartTime'])) / 1000,
                        depends_on=dictionary['DependsOn'].split(' '),
                        size=int(size[index_of_w]), param = len(nx.descendants(G, dictionary['JobId'])))
                    
        elif all(field in dictionary for field in data_fields):
            data_dict[dictionary['JobId']] = Task(
                id=dictionary['JobId'],
                name='DATA',
                task_duration=0,
                depends_on=[],
                size=65536,
                status=STATUS_INIT
            )
            
    
    
    
    print(f'{n} tasks')
    print('generate task -- done.')
    print('-' * 10)
    return task_dict, data_dict