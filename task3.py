from support_funcs import *

TASK_NUM = 3
VARIANT = 37
TASK_PATH = f'./task{TASK_NUM}/'
FILE_NAME = f'task_{TASK_NUM}_item.pkl'
DB_NAME = f'var{VARIANT}_db_task{TASK_NUM}'
RESULT_PATH = f'{TASK_PATH}result/'

def drop_by_salary(collection):
    query = {
        '$or': [
            {'salary': {'$lt': 25_000}},
            {'salary': {'$gt': 175_000}}
        ]
    }
    result = collection.delete_many(query)
    print(result)

def inc_age_by_one(collection):
    upd = { 
        '$inc': {
            'age': 1
            }
        }
    result = collection.update_many({}, upd)
    print(result)

def inc_salary_by_job(collection, percent, jobs):
    filter = {
        'job': {'$in': jobs},
    }
    upd = { 
        '$mul': {
            'salary': (1 + percent/100)
            }
        }
    result = collection.update_many(filter, upd)
    print(result)

def inc_salary_by_city(collection, percent, cities):
    filter = {
        'city': {'$in': cities},
    }
    upd = { 
        '$mul': {
            'salary': (1 + percent/100)
            }
        }
    result = collection.update_many(filter, upd)
    print(result)

def inc_salary_by(collection, percent, cities, jobs, age):
    filter = {
        'city': {'$in': cities},
        'job': {'$in': jobs},
        'age': {'$in': list(range(age[0], age[1]+1, 1))}
    }
    upd = { 
        '$mul': {
            'salary': (1 + percent/100)
            }
        }
    result = collection.update_many(filter, upd)
    print(result)

def drop_by(collection, **kwargs):
    query = {
        '$and': [
            {'job': {'$in': kwargs['job']}},
            {'age': {'$in': list(range(*kwargs['age']))}}
        ]
    }
    result = collection.delete_many(query)
    print(result)

def main():
    db = connect_to_mongodb(DB_NAME)
    collection = db.person
    # получение данных из файла
    data = read_pickle(f'{TASK_PATH}{FILE_NAME}')
    # проверка на наличие коллекции
    collection_names = db.list_collection_names()
    if collection.name not in collection_names:
        insert_data_to_mongodb(collection, data)
    
    # удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
    drop_by_salary(collection)
    
    # увеличить возраст (age) всех документов на 1
    inc_age_by_one(collection)
    
    # поднять заработную плату на 5% для произвольно выбранных профессий
    inc_salary_by_job(collection, 5, ['Врач', 'Учитель', 'Медсестра'])
    
    # поднять заработную плату на 7% для произвольно выбранных городов
    inc_salary_by_city(collection, 7, ['Мурсия', 'Загреб', 'Санхенхо'])
    
    # поднять заработную плату на 10% для выборки по сложному предикату 
    # (произвольный город, произвольный набор профессий, произвольный диапазон возраста)
    inc_salary_by(collection, 10, ['Таллин'], ['Психолог', 'Водитель', 'Повар'], (40, 65))
    
    # удалить из коллекции записи по произвольному предикату
    drop_by(collection, job=['Архитектор', 'Строитель'], age=(40, 65))

if __name__ == '__main__':
    main()
