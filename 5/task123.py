import json
import re
from pymongo import MongoClient
import csv
from bson.json_util import dumps
from bson import SON

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Выбор базы данных
db = client['Jobs']

# Выбор коллекции
collection = db['JobsList']

# Загрузка данных из text файла
with open('task_1_item.text', encoding='utf-8') as file:
    data = []
    lines = file.read().split("=====")
    for line in lines:
        if line.strip():
            match = re.search(r'job::(.+?)\nsalary::(\d+)\nid::(\d+)\ncity::(.+?)\nyear::(\d+)\nage::(\d+)', line, re.DOTALL)
            if match:
                job, salary, job_id, city, year, age = match.groups()
                data.append({"job": job, "salary": int(salary), "id": int(job_id),
                             "city": city, "year": int(year), "age": int(age)})
            else:
                print(f"Skipping invalid line: {line}")

collection.insert_many(data)

# Определение минимального и максимального возраста
min_age = 25
max_age = 35

# Выполнение запросов
# Вывод первых 10 записей, отсортированных по убыванию по полю salary
result1 = list(collection.find().sort("salary", -1).limit(10))

# Вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортировать по убыванию по полю salary
result2 = list(collection.find({"age": {"$lt": 30}}).sort("salary", -1).limit(15))

# Вывод первых 10 записей, отфильтрованных по сложному предикату и отсортированных по возрастанию по полю age
result3 = list(collection.find({"city": "Лас-Росас", "job": {"$in": ["Водитель", "Продавец", "Косметолог"]}}).sort("age", 1).limit(10))

# Вывод количества записей, получаемых в результате фильтрации
result4 = collection.count_documents({"age": {"$gte": min_age, "$lte": max_age},
                                      "year": {"$in": [2019, 2022]},
                                      "$or": [{"salary": {"$gt": 50000, "$lte": 75000}},
                                              {"salary": {"$gt": 125000, "$lt": 150000}}]})


# Сохранение результатов в JSON-файлы
with open('task1_salary_10_desc.json', 'w', encoding='utf-8') as file:
    result1_filtered = [{"job": item["job"], "salary": item["salary"], "id": item["id"],
                         "city": item["city"], "year": item["year"], "age": item["age"]}
                        for item in result1]
    json.dump(result1_filtered, file, ensure_ascii=False)

with open('task1_first_15_age_less_than_30_desc_by_salary.json', 'w', encoding='utf-8') as file:
    result2_filtered = [{"job": item["job"], "salary": item["salary"], "id": item["id"],
                         "city": item["city"], "year": item["year"], "age": item["age"]}
                        for item in result2]
    json.dump(result2_filtered, file, ensure_ascii=False)

with open('task1_hard_predicat.json', 'w', encoding='utf-8') as file:
    result3_filtered = [{"job": item["job"], "salary": item["salary"], "id": item["id"],
                         "city": item["city"], "year": item["year"], "age": item["age"]}
                        for item in result3]
    json.dump(result3_filtered, file, ensure_ascii=False)

with open('task1_filter.json', 'w', encoding='utf-8') as file:
    json.dump({"Количество записей 25 < age < 35, year [2019,2022], 50000 < salary <= 75000 || 125000 < salary < 150000": result4}, file, ensure_ascii=False)

# Импорт данных из файла CSV
with open('task_2_item.csv', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file, delimiter=';')
    csv_data = [dict(line) for line in csv_reader]
    collection.insert_many(csv_data)

def save_to_json(filename, data):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False)

# Вывод минимальной, средней, максимальной salary
result_salary_stats = collection.aggregate([
    {"$group": {"_id": None, "min_salary": {"$min": "$salary"}, "avg_salary": {"$avg": "$salary"}, "max_salary": {"$max": "$salary"}}}
])
save_to_json('task2_salary_stats.json', json.loads(dumps(list(result_salary_stats))))

# Вывод количества данных по представленным профессиям
result_profession_count = collection.aggregate([
    {"$group": {"_id": "$job", "count": {"$sum": 1}}}
])
save_to_json('task2_profession_count.json', json.loads(dumps(list(result_profession_count))))

# Вывод минимальной, средней, максимальной salary по городу
result_city_salary_stats = collection.aggregate([
    {"$group": {"_id": "$city", "min_salary": {"$min": "$salary"}, "avg_salary": {"$avg": "$salary"}, "max_salary": {"$max": "$salary"}}}
])
save_to_json('task2_city_salary_stats.json', json.loads(dumps(list(result_city_salary_stats))))

# Вывод минимальной, средней, максимальной salary по профессии
result_profession_salary_stats = collection.aggregate([
    {"$group": {"_id": "$job", "min_salary": {"$min": "$salary"}, "avg_salary": {"$avg": "$salary"}, "max_salary": {"$max": "$salary"}}}
])
save_to_json('task2_profession_salary_stats.json', json.loads(dumps(list(result_profession_salary_stats))))

# Вывод минимального, среднего, максимального возраста по городу
result_city_age_stats = collection.aggregate([
    {"$group": {"_id": "$city", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}}
])
save_to_json('task2_city_age_stats.json', json.loads(dumps(list(result_city_age_stats))))

# Вывод минимального, среднего, максимального возраста по профессии
result_profession_age_stats = collection.aggregate([
    {"$group": {"_id": "$job", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}}
])
save_to_json('task2_profession_age_stats.json', json.loads(dumps(list(result_profession_age_stats))))

# Вывод максимальной заработной платы при минимальном возрасте
result_max_salary_min_age = collection.find({"age": collection.find_one({}, sort=[("age", 1)])["age"]}).sort("salary", -1).limit(1)
save_to_json('task2_max_salary_min_age.json', json.loads(dumps(list(result_max_salary_min_age))))

# Вывод минимальной заработной платы при максимальном возрасте
result_min_salary_max_age = collection.find({"age": collection.find_one({}, sort=[("age", -1)])["age"]}).sort("salary", 1).limit(1)
save_to_json('task2_min_salary_max_age.json', json.loads(dumps(list(result_min_salary_max_age))))

# Вывод минимального, среднего, максимального возраста по городу, при условии, что заработная плата больше 50 000, отсортировать вывод по любому полю
result_city_age_stats_filtered_salary = collection.aggregate([
    {"$match": {"salary": {"$gt": 50000}}},
    {"$group": {"_id": "$city", "min_age": {"$min": "$age"}, "avg_age": {"$avg": "$age"}, "max_age": {"$max": "$age"}}},
    {"$sort": SON([("_id", 1)])}
])
save_to_json('task2_city_age_stats_filtered_salary.json', json.loads(dumps(list(result_city_age_stats_filtered_salary))))

# Вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах по городу, профессии, и возрасту: 18<age<25 & 50<age<65
result_salary_stats_custom_ranges = collection.aggregate([
    {"$match": {"age": {"$in": list(range(18, 26)) + list(range(50, 66))}}},
    {"$group": {"_id": None, "min_salary": {"$min": "$salary"}, "avg_salary": {"$avg": "$salary"}, "max_salary": {"$max": "$salary"}}}
])
save_to_json('task2_salary_stats_custom_ranges.json', json.loads(dumps(list(result_salary_stats_custom_ranges))))

# Произвольный запрос с $match, $group, $sort
result_custom_aggregation = collection.aggregate([
    {"$match": {"age": {"$gte": 30}}},
    {"$group": {"_id": "$job", "avg_salary": {"$avg": "$salary"}}},
    {"$sort": SON([("avg_salary", -1)])}
])
save_to_json('task2_custom_aggregation.json', json.loads(dumps(list(result_custom_aggregation))))

# Загрузка данных из JSON-файла
with open('task_3_item.json', encoding='utf-8') as file:
    data = eval(file.read())
    collection.insert_many(data)

# Удаление документов по предикату: salary < 25 000 || salary > 175000
collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})

# Преобразование age в числовой формат
collection.update_many({"age": {"$type": "string"}}, [{"$addFields": {"age": {"$toInt": "$age"}}}])
# Увеличение возраста (age) всех документов на 1
collection.update_many({}, {"$inc": {"age": 1}})

# Преобразование salary в числовой формат
collection.update_many({}, [{"$set": {"salary": {"$toDouble": "$salary"}}}])

# Поднятие заработной платы на 5% для произвольно выбранных профессий
collection.update_many({"job": {"$in": ["Программист", "Менеджер"]}}, {"$mul": {"salary": 1.05}})

# Поднятие заработной платы на 7% для произвольно выбранных городов
collection.update_many({"city": {"$in": ["Москва", "Санкт-Петербург"]}}, {"$mul": {"salary": 1.07}})

# Поднятие заработной платы на 10% для выборки по сложному предикату
# (произвольный город, произвольный набор профессий, произвольный диапазон возраста)
collection.update_many({"city": "Москва", "job": {"$in": ["Врач", "Учитель"]}, "age": {"$gte": 30, "$lte": 40}},
                       {"$mul": {"salary": 1.1}})

# Удаление из коллекции записей по произвольному предикату
collection.delete_many({"year": 2010, "age": {"$gte": 25, "$lte": 35}})

# Закрытие подключения
client.close()
