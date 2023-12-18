import msgpack
import pickle
import json
from pymongo import MongoClient
from pprint import pprint

#DUMPERS
def dump_json(object, filename):
    with open(filename, mode='w', encoding='UTF-8') as f:
        json.dump(object, f, ensure_ascii=False, default=str)

# READERS
def read_msgpack(filename):
    with open(filename, mode='rb') as f:
        return msgpack.load(f)
    
def read_pickle(filename):
    with open(filename, mode='rb') as f:
        return pickle.load(f)
    
def read_json(filename):
    with open(filename, mode='r', encoding='UTF-8') as f:
        return json.load(f)

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
                item[key] = int(value)
            else:
                item[key] = value
        dataset.append(item)
    return dataset

# MONGO
def connect_to_mongodb(db_name):
    client = MongoClient()
    db = client[db_name]
    return db

def insert_data_to_mongodb(collection, data):
    res = collection.insert_many(data)
