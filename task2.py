from support_funcs import *
from pymongo import MongoClient

TASK_NUM = 2
TASK_PATH = f'./task{TASK_NUM}/'
FILE_NAME = f'task_{TASK_NUM}_item.text'

def main():
    dataset = read_text(f'{TASK_PATH}{FILE_NAME}')

if __name__ == '__main__':
    main()
