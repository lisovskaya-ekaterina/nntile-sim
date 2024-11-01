from .Task import Task
from .const import *


def remove_spaces_from_list(lst):
    return [value for value in lst if value != '']

def generate_task():
    task_dict = {}
    with open('examples/80-tasks.rec', 'r') as f:
        lines = f.read()
    
    lines = lines.split('MPIRank: -1')
    for note in lines: 
        note = note.split("\n")
        note = remove_spaces_from_list(note)
        dictinory = dict(subString.split(": ") for subString in note)
        
        if 'JobId' in dictinory.keys():
            if 'DependsOn' in dictinory.keys() and dictinory['Name'][0] != '_':
                mode_letters_list = dictinory['Modes'].split()
                for i in mode_letters_list:
                    if 'W' in i:
                        index_of_w = mode_letters_list.index(i)
                size = dictinory['Sizes'].split()
                task_dict[dictinory['JobId']] = Task(id = dictinory['JobId'],
                                                    name = 'NAME',
                                                    task_duration=(float(dictinory['EndTime'])-float(dictinory['StartTime'])) / 1000,
                                                    depends_on=dictinory['DependsOn'].split(' '),
                                                    size=int(size[index_of_w]))
            else:
                task_dict[dictinory['JobId']] = Task(id = dictinory['JobId'],
                                                    name = 'NAME',
                                                    task_duration = 0,
                                                    depends_on = [],
                                                    size = 67108864)
                task_dict[dictinory['JobId']].status = STATUS_DONE
                
    return task_dict