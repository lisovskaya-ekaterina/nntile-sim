from .Task import Task
from .const import *


def remove_spaces_from_list(lst):
    return [value for value in lst if value != '']

def generate_task(logs_file_name, i_epoch, i_batch):

    task_fields = ['Name', 'DependsOn', 'JobId', 'EndTime', 'StartTime', 'Iteration', 'Modes', 'Sizes']
    data_fields = ['JobId']

    task_dict = {}
    data_dict = {}
    with open('examples/'+logs_file_name, 'r') as f:
        lines = f.read()
    
    lines = lines.split('MPIRank: -1')
    n = 0
    for note in lines:
        note = note.split("\n")
        note = remove_spaces_from_list(note)
        dictinory = dict(subString.split(": ") for subString in note)
        
        if all(element in dictinory for element in task_fields) and dictinory['Name'][0] != '_' and dictinory['Name'] != 'task_build':
            iter = [int(elem) for elem in dictinory['Iteration'].split()]
            if iter[0] == i_epoch and iter[1] == i_batch:
                n += 1
                mode_letters_list = dictinory['Modes'].split()
                for i in mode_letters_list:
                    if 'W' in i:
                        index_of_w = mode_letters_list.index(i)
                size = dictinory['Sizes'].split()
                task_dict[dictinory['JobId']] = Task(id = dictinory['JobId'],
                                                    name = dictinory['Name'],
                                                    task_duration = (float(dictinory['EndTime'])-float(dictinory['StartTime'])) / 1000,
                                                    depends_on = dictinory['DependsOn'].split(' '),
                                                    size = int(size[index_of_w]))
                                                    
        elif all(element in dictinory for element in data_fields):
            data_dict[dictinory['JobId']] = Task(id = dictinory['JobId'],
                                                 name = 'DATA',
                                                 task_duration = 0,
                                                 depends_on = [],
                                                 size = 65536,
                                                 status = STATUS_INIT)
    print(f'{n} tasks')
    print('generate task -- done. ')
    print('-'*10)           
    return task_dict, data_dict