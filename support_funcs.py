import msgpack
import pickle
import json
from pprint import pprint

def dump_json(object, filename):
    with open(filename, mode='w') as f:
        json.dump(object, f, ensure_ascii=False)

def read_msgpack(filename):
    with open(filename, mode='rb') as f:
        return msgpack.load(f)
    
def read_pickle(filename):
    with open(filename, mode='rb') as f:
        return pickle.load(f)
    
def read_text(filename):
    with open(filename, mode='r', encoding='UTF-8') as f:
        data_rows = f.read().strip('=====\n').split('=====\n')
    to_int = ['salary', 'id', 'year', 'age']
    dataset = list()
    for data_row in data_rows:
        item = dict()
        for param_row in data_row.strip().split('\n'):
            [key, value] = param_row.split('::')
            if key in to_int:
                item[key] = value
            elif key:
                item[key] = value
        dataset.append(item)
    return dataset

# pprint(read_msgpack('./task1/task_1_item.msgpack'))
# pprint(read_text('./task2/task_2_item.text'))
# pprint(read_pkl('./task3/task_3_item.pkl'))