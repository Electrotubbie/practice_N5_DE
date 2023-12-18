from support_funcs import *

TASK_NUM = 2
VARIANT = 37
TASK_PATH = f'./task{TASK_NUM}/'
FILE_NAME = f'task_{TASK_NUM}_item.text'
DB_NAME = f'var{VARIANT}_db_task{TASK_NUM}'
RESULT_PATH = f'{TASK_PATH}result/'

def get_salary_stats(collection):
    query = [
        {
            '$group': {
                '_id': 'salary_stats',
                'max': {'$max': '$salary'},
                'min': {'$min': '$salary'},
                'avg': {'$avg': '$salary'}
            }
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_jobs_stats(collection):
    query = [
        {
            '$group': {
                '_id': '$job',
                'count': {'$sum': 1}
            }
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_stats_by_groups(collection, stats_col, group_col):
    query = [
        {
            '$group': {
                '_id': f'${group_col}',
                'max': {'$max': f'${stats_col}'},
                'min': {'$min': f'${stats_col}'},
                'avg': {'$avg': f'${stats_col}'}
            }
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_min_age_max_salary(collection):
    query = [
        {
            '$sort': {
                'age': 1,
                'salary': -1
            }
        },
        {
                '$limit': 1
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_max_age_min_salary(collection):
        query = [
            {
                '$sort': {
                    'age': -1,
                    'salary': 1
                }
            },
            {
                  '$limit': 1
            }
        ]
        result = collection.aggregate(query)
        return [*result]

def get_age_by_city_if_salary(collection):
    query = [
        {
            '$match': {
                'salary': {'$gt': 50_000}
            }
        },
        {
            '$group': {
                '_id': f'$city',
                'max': {'$max': f'$age'},
                'min': {'$min': f'$age'},
                'avg': {'$avg': f'$age'}
            }
        },
        {
            '$sort': {
                'max': -1
            }
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_salary_by_some(collection):
    city = ['Будапешт', 'Сеговия', 'Тирана']
    jobs = ['Водитель', 'Инженер', 'Врач']
    query = [
        {
            '$match': {
                'city': {'$in': city},
                'job': {'$in': jobs},
                '$or': [
                    {'age': {'$gt': 18, '$lt': 25}},
                    {'age': {'$gt': 50, '$lt': 65}}
                ]
            }
        },
        {
            '$group': {
                '_id': 'result',
                'max': {'$max': f'$salary'},
                'min': {'$min': f'$salary'},
                'avg': {'$avg': f'$salary'}
            }
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_all_in_one(collection):
    # подсчёт Операторов call-центра возрастом от 18 до 30 лет во всех городах
    query = [
        {
            '$match': {
                'job': 'Оператор call-центра',
                'age': {'$gt': 18, '$lt': 30}
            }
        },
        {
            '$group': {
                '_id': '$city',
                'count': {'$sum': 1},
            }
        },
        {
            '$sort': {
                'count': -1
            }
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def main():
    db = connect_to_mongodb(DB_NAME)
    collection = db.person
    # получение данных из файла
    data = read_text(f'{TASK_PATH}{FILE_NAME}')
    # проверка на наличие коллекции
    collection_names = db.list_collection_names()
    if collection.name not in collection_names:
        insert_data_to_mongodb(collection, data)

    # вывод минимальной, средней, максимальной salary
    salary_stats = get_salary_stats(collection)
    dump_json(salary_stats, f'{RESULT_PATH}res1_salary_stats.json')

    # вывод количества данных по представленным профессиям
    jobs_stats = get_jobs_stats(collection)
    dump_json(jobs_stats, f'{RESULT_PATH}res2_jobs_stats.json')

    # вывод минимальной, средней, максимальной salary по городу
    salary_by_city = get_stats_by_groups(collection, 'salary', 'city')
    dump_json(salary_by_city, f'{RESULT_PATH}res3_salary_by_city.json')

    # вывод минимальной, средней, максимальной salary по профессии
    salary_by_job = get_stats_by_groups(collection, 'salary', 'job')
    dump_json(salary_by_job, f'{RESULT_PATH}res4_salary_by_job.json')

    # вывод минимального, среднего, максимального возраста по городу
    age_by_city = get_stats_by_groups(collection, 'age', 'city')
    dump_json(age_by_city, f'{RESULT_PATH}res5_age_by_city.json')

    # вывод минимального, среднего, максимального возраста по профессии 
    age_by_job = get_stats_by_groups(collection, 'age', 'job')
    dump_json(age_by_job, f'{RESULT_PATH}res6_age_by_job.json')

    # вывод максимальной заработной платы при минимальном возрасте
    min_age_max_salary = get_min_age_max_salary(collection)
    dump_json(min_age_max_salary, f'{RESULT_PATH}res7_min_age_max_salary.json')

    # вывод минимальной заработной платы при максимальной возрасте
    max_age_min_salary = get_max_age_min_salary(collection)
    dump_json(max_age_min_salary, f'{RESULT_PATH}res8_max_age_min_salary.json')

    # вывод минимального, среднего, максимального возраста по городу, 
    # при условии, что заработная плата больше 50 000, отсортировать вывод по любому полю.
    age_by_city_if_salary = get_age_by_city_if_salary(collection)
    dump_json(age_by_city_if_salary, f'{RESULT_PATH}res9_age_by_city_if_salary.json')

    # вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах 
    # по городу, профессии, и возрасту: 18<age<25 & 50<age<65
    salary_by_some = get_salary_by_some(collection)
    dump_json(salary_by_some, f'{RESULT_PATH}res10_salary_by_some.json')

    # произвольный запрос с $match, $group, $sort
    # подсчёт Операторов call-центра возрастом от 18 до 30 лет во всех городах
    all_in_one = get_all_in_one(collection)
    dump_json(all_in_one, f'{RESULT_PATH}res11_all_in_one.json')

if __name__ == '__main__':
    main()
