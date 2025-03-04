from .Task import Task
from .const import *

def remove_spaces_from_list(lst):
    return [value for value in lst if value != '']

def generate_task(logs_file_name, i_epoch, i_batch):
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
                        size=int(size[index_of_w])
                    )
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