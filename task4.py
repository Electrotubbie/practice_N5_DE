from support_funcs import *
from pprint import pprint

TASK_NUM = 4
VARIANT = 37
TASK_PATH = f'./task{TASK_NUM}/'
FILE_NAME_1 = f'task_{TASK_NUM}_item_1.json'
FILE_NAME_2 = f'task_{TASK_NUM}_item_2.pkl'
DB_NAME = f'var{VARIANT}_db_task{TASK_NUM}'
RESULT_PATH = f'{TASK_PATH}result/'

def find_student_by_snils(collection, snils):
    query = {
        'snils': snils
    }
    result = collection.find(query)
    return [*result]

def find_students_by_speciality(collection):
    query = {
        'applications.speciality': {'$regex': '13.03.02'},
        'applications.compensation': 'бюджетная основа',
        'applications.priority': 1,
        'applications.total_mark': {'$gte': 250}
    }
    sort = {
        'applications.total_mark': -1
    }
    result = collection.find(query, limit=20).sort(sort)
    return [*result]

def find_first_students(collection):
    result = collection.find({}, limit=20).sort({'applications.total_mark': -1})
    return [*result]

def find_top_students_on_programs(collection):
    programs = ['Цифровое управление электроэнергетическими системами',
                'Инженерия искусственного интеллекта',
                'Прикладной анализ данных']
    query = {
        'applications.program': {'$in': programs},
    }
    sort = {
        'applications.total_mark': -1
    }
    result = collection.find(query, limit=20).sort(sort)
    return [*result]

def find_all_RTF(collection):
    query = {
        'applications.institute': 'ИРИТ-РТФ',
        'applications.familirization': 'Очная',
        'applications.competition': 'Основные места',
    }
    result = collection.find(query, limit=20)
    return [*result]

def get_count_by_university(collection):
    query = [
        {
            '$unwind': '$applications'
        },
        {
            '$group': {
                '_id': {
                    'institute': '$applications.institute',
                    'regnum': '$regnum'
                },
                'total_applications': {'$sum': 1}
            }
        },
        {
        '$group': {
            '_id': '$_id.institute',
            'total_unique_applicants': {'$sum': 1},
            'total_applications': {'$sum': '$total_applications'}
        }
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_analyse_marks_by_university(collection):
    query = [
        {
            '$unwind': '$applications'
        },
        {
            '$group': {
                '_id': '$applications.institute',
                'min': {'$min': '$applications.total_mark'},
                'max': {'$max': '$applications.total_mark'},
                'avg': {'$avg': '$applications.total_mark'},
            }
        },
        {
            '$sort': {'avg': 1}
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_count_by_compensation_in_university(collection):
    query = [
        {
            '$unwind': '$applications'
        },
        {
            '$match': {
                'applications.familirization': 'Очная'
            }
        },
        {
            '$group': {
                '_id': {
                    'institute': '$applications.institute',
                    'regnum': '$regnum'
                },
                'total_applications': {'$sum': 1}
            }
        },
        {
        '$group': {
            '_id': '$_id.institute',
            'total_unique_applicants': {'$sum': 1},
            'total_applications': {'$sum': '$total_applications'}
        }
        },
        {
            '$sort': {'total_unique_applicants': -1}
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_analyse_marks_by_speciality(collection):
    query = [
        {
            '$unwind': '$applications'
        },
        {
            '$group': {
                '_id': '$applications.speciality',
                'min': {'$min': '$applications.total_mark'},
                'max': {'$max': '$applications.total_mark'},
                'avg': {'$avg': '$applications.total_mark'},
            }
        },
        {
            '$sort': {'avg': -1}
        }
    ]
    result = collection.aggregate(query)
    return [*result]

def get_count_by_marks(collection):
    query = [
        {
            '$unwind': '$applications'
        },
        {
            '$match': {
                'applications.total_mark': {'$gte': 200, '$lte': 250}
            }
        },
        {
            '$group': {
                '_id': '$regnum',
            }
        },
        {
            '$group': {
                '_id': 'result',
                'count_applications_200_250': {'$sum': 1}
            }
        },
    ]
    result = collection.aggregate(query)
    return [*result]

def inc_marks_by_critery(collection):
    flt = {
        'applications.institute': 'ИНМТ',
        'applications.competition': 'Основные места',
        'applications.status': 'К зачислению'
    }
    update = {
        '$inc': {
            'applications.$[].total_mark': 15
        }
    }
    result = collection.update_many(flt, update)
    print(result)

def decline_by_total_mark(collection):
    flt = {
        'applications.total_mark': {'$lt': 30}
    }
    update = {
        '$set': {
            'applications.$[].status': 'Отклонено'
        }
    }
    result = collection.update_many(flt, update)
    print(result)

def apply_application(collection):
    flt = {
        'applications.priority': 1,
        'applications.edu_doc_original': True,
        'applications.total_mark': {'$gte': 300}
    }
    update = {
        '$set': {
            'applications.$[].status': 'Зачислен'
        }
    }
    result = collection.update_many(flt, update)
    print(result)

def drop_by(collection):
    flt = {
        'applications.total_mark': 0,
        'applications.competition': 'Основные места',
        'applications.compensation': 'бюджетная основа'
    }
    update = {
        '$pull': {
            'applications': {
                'total_mark': 0,
                'competition': 'Основные места',
                'compensation': 'бюджетная основа'
            }
        }
    }
    result = collection.update_many(flt, update)
    print(result)

def drop_student(collection, regnum):
    query = {
        'regnum': regnum
    }
    result = collection.delete_many(query)
    print(result)

def main():
    db = connect_to_mongodb(DB_NAME)
    collection = db.person
    # проверка на наличие коллекции
    collection_names = db.list_collection_names()
    if collection.name not in collection_names:
        # получение данных из файла
        data_1 = read_json(f'{TASK_PATH}{FILE_NAME_1}')
        insert_data_to_mongodb(collection, data_1)
        data_2 = read_pickle(f'{TASK_PATH}{FILE_NAME_2}')
        insert_data_to_mongodb(collection, data_2)
    
    ## 1
    # 1.1 поиск студента по СНИЛС
    student_by_snils = find_student_by_snils(collection, '15874551810')
    dump_json(student_by_snils, f'{RESULT_PATH}/res1_1_student_by_snils.json')
    # pprint(student_by_snils)
    
    # 1.2 выборка первых 20 студентов по специальности 13.03.02 среди студетнов бюджетной основы с приоритетом 1
    # а также суммарной оценкой >= 250, отсортированных по полученным баллам
    students_by_speciality = find_students_by_speciality(collection)
    dump_json(student_by_snils, f'{RESULT_PATH}/res1_2_students_by_speciality.json')
    # pprint(students_by_speciality)
    
    # 1.3 получение первых 20 студентов с самым большим количеством баллов среди всех, подавших документы
    top_students = find_first_students(collection)
    dump_json(top_students, f'{RESULT_PATH}/res1_3_top_students.json')
    # pprint(top_students)
    
    # 1.4 получение 20 студентов с самым большим количеством баллов среди всех, 
    # подавших документы по определённым программам
    top_students_on_programs = find_top_students_on_programs(collection)
    dump_json(top_students_on_programs, f'{RESULT_PATH}/res1_4_top_students_on_programs.json')
    # pprint(top_students_on_programs)
    
    # 1.5 получение всех студентов, поступающих очно в ИРИТ-РТФ на основные места
    all_RTF = find_all_RTF(collection)
    dump_json(all_RTF, f'{RESULT_PATH}/res1_5_all_rtf.json')
    # pprint(all_RTF)
    
    ## 2
    # 2.1 получение количества поступающих по университетам
    count_by_university = get_count_by_university(collection)
    dump_json(count_by_university, f'{RESULT_PATH}/res2_1_count_by_university.json')
    
    # 2.2 аналитика по баллам по институтам
    analyse_marks_by_university = get_analyse_marks_by_university(collection)
    dump_json(analyse_marks_by_university, f'{RESULT_PATH}/res2_2_analyse_marks_by_university.json')
    
    # 2.3 количество бюджетных и контрактных заявлений по университетам
    count_by_compensation_in_university = get_count_by_compensation_in_university(collection)
    dump_json(count_by_compensation_in_university, f'{RESULT_PATH}/res2_3_count_by_compensation_in_university.json')
    
    # 2.4 аналитика по баллам по специальностям
    analyse_marks_by_speciality = get_analyse_marks_by_speciality(collection)
    dump_json(analyse_marks_by_speciality, f'{RESULT_PATH}/res2_4_analyse_marks_by_speciality.json')
    
    # 2.5 количество студентов, набравших от [200 до 250] баллов
    count_by_marks = get_count_by_marks(collection)
    dump_json(count_by_marks, f'{RESULT_PATH}/res2_5_count_by_marks.json')
    
    ## 3
    # 3.1 прибавить 15 баллов студентам ИНМТ, находящихся 'К зачислению' 
    # среди Основных мест 
    inc_marks_by_critery(collection)
    
    # # 3.2 задание статуса Отклонено студентам, баллы которых ниже 30
    decline_by_total_mark(collection)
    
    # # 3.3 зачислить студентов, суммарные баллы которых >= 300
    apply_application(collection)
    
    # 3.4 удаление заявлений студентов, баллы которых = 0, среди Основных мест,
    # на бюджетной основе
    drop_by(collection)  
    
    # 3.5 удаление студента по regnum
    drop_student(collection, 261425) 
        
if __name__ == '__main__':
    main()