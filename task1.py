from support_funcs import *

TASK_NUM = 1
VARIANT = 37
TASK_PATH = f'./task{TASK_NUM}/'
FILE_NAME = f'task_{TASK_NUM}_item.msgpack'
DB_NAME = f'var{VARIANT}_db_task{TASK_NUM}'

def get_limit_sort_by_salary(collection):
    result = collection.find({}, limit=10).sort({'salary': -1})
    return result

def get_limit_filter_age_sort_salary(collection):
    result = collection.find({'age': {'$lt': 30}}, limit=15).sort({'salary': -1})
    return result

def get_limit_filter_any_sort_age(collection, city, jobs):
    result = collection.find({'city': city, 'job': {'$in': jobs}}, limit=10).sort({'age': -1})
    return result

def get_count_filter(collection):
    query = {
            'age': {'$gte': 30, '$lte': 45}, 
            'year': {'$in': list(range(2019, 2023, 1))},
            '$or': [
                {'salary': {'$gt': 50000, '$lte': 75000}}, 
                { 'salary': {'$gt': 125000, '$lt': 150000}}
                ]
            }
    count = collection.count_documents(query)
    result = collection.find(query)
    return {'count': count, 'result': [*result]}


def main():
    db = connect_to_mongodb(DB_NAME)
    collection = db.person
    # получение данных из файла
    data = read_msgpack(f'{TASK_PATH}{FILE_NAME}')
    # проверка на наличие коллекции
    collection_names = db.list_collection_names()
    if collection.name not in collection_names:
        insert_data_to_mongodb(collection, data)
    
    dump_json([*get_limit_sort_by_salary(collection)], 
              f'{TASK_PATH}/result/res1_sort_by_salary.json')
    dump_json([*get_limit_filter_age_sort_salary(collection)], 
              f'{TASK_PATH}/result/res2_filter_age_sort_salary.json')
    dump_json([*get_limit_filter_any_sort_age(collection, 
              city='Москва', 
              jobs=['Оператор call-центра', 'Бухгалтер', 'IT-специалист'])], 
              f'{TASK_PATH}/result/res3_filter_any_sort_age.json')
    dump_json(get_count_filter(collection), 
              f'{TASK_PATH}/result/res4_filter_and_count.json')

if __name__ == '__main__':
    main()
