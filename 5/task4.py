import csv
import pickle
import msgpack
from pymongo import MongoClient
import json
from bson import ObjectId

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Выбор базы данных
db = client['Books']

# Выбор коллекции
collection = db['BooksList']

# Загрузка данных из CSV
with open('task_4_csv_books.csv', 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    csv_data = [dict(row) for row in csv_reader]
    collection.insert_many(csv_data)

# Загрузка данных из Pickle
with open('task_4_pkl_books.pkl', 'rb') as pkl_file:
    pkl_data = pickle.load(pkl_file)
    collection.insert_many(pkl_data)

# Загрузка данных из MessagePack
with open('task_4_msgpack_books.msgpack', 'rb') as msgpack_file:
    packed_data = msgpack_file.read()
    msgpack_data = msgpack.unpackb(packed_data, raw=False)
    collection.insert_many(msgpack_data)

# Convert ObjectId to string in documents
for document in collection.find():
    document['_id'] = str(document['_id'])
    collection.replace_one({'_id': document['_id']}, document)

# Convert ObjectId to string in results
def convert_object_ids(item):
    if isinstance(item, ObjectId):
        return str(item)
    elif isinstance(item, dict):
        return {key: convert_object_ids(value) for key, value in item.items()}
    elif isinstance(item, list):
        return [convert_object_ids(element) for element in item]
    return item

# Выборка (задание 1):
result1 = convert_object_ids(list(collection.find().sort("publication_year", 1).limit(4)))

result2 = convert_object_ids(list(collection.find({"title": "The Hitchhiker's Guide to the Galaxy"})))

result3 = convert_object_ids(list(collection.find({"publication_year": {"$gt": 2006}})))

result4 = convert_object_ids(list(collection.find({"author": "Fyodor Dostoevsky"})))

result5 = convert_object_ids(list(collection.find({"year": {"$gt": 1950}})))

# Выборка с агрегацией (задание 2):
agg_result1 = collection.count_documents({})

agg_result2 = convert_object_ids(list(collection.aggregate([{"$group": {"_id": None, "avg_year": {"$avg": "$publication_year"}}}])))

agg_result3 = convert_object_ids(list(collection.aggregate([
    {"$group": {"_id": "$publication_year", "count": {"$sum": 1}}}
])))

agg_result4 = convert_object_ids(list(collection.aggregate([
    {"$group": {"_id": "$category", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
])))

agg_result5 = convert_object_ids(list(collection.aggregate([
    {"$group": {"_id": "$author", "total_books": {"$sum": 1}}},
    {"$sort": {"total_books": -1}},
    {"$limit": 1}
])))

# Обновление/удаление данных (задание 3):

for document in collection.find():
    if 'year' in document and isinstance(document['year'], str):
        document['year'] = int(document['year'])
        collection.replace_one({'_id': document['_id']}, document)

collection.update_one({"_id": ObjectId('65afce32b702d5b4fc1b63bd')}, {"$set": {"rating": 4.8}})

collection.update_many({"category": "Gothic"}, {"$set": {"rating": 5}})

collection.update_many({"title": "Sapiens: A Brief History of Humankind"}, {"$set": {"publication_year": 2012}})

# Сохранение результатов в JSON-файл
results = {
    "Выборка": {
        "result1": result1,
        "result2": result2,
        "result3": result3,
        "result4": result4,
        "result5": result5
    },
    "Выборка с агрегацией": {
        "agg_result1": agg_result1,
        "agg_result2": agg_result2,
        "agg_result3": agg_result3,
        "agg_result4": agg_result4,
        "agg_result5": agg_result5
    },
    "Обновление/удаление данных": {
        "status_update_one": "Success" if "nModified" in collection.update_one({"_id": ObjectId('65afce32b702d5b4fc1b63bc')}, {"$set": {"rating": 4.8}}).raw_result else "Failure",
        "status_update_many": "Success" if "nModified" in collection.update_many({"author": "George Orwell"}, {"$inc": {"publication_year": 5}}).raw_result else "Failure",
        "status_update_year": "Success" if "nModified" in collection.update_many({"category": "Gothic"}, {"$set": {"rating": 5}}).raw_result else "Failure",
        "status_update_category": "Success" if "nModified" in collection.update_many({"author": "J.K. Rowling"}, {"$set": {"category": "Fantasy"}}).raw_result else "Failure",
    }
}

with open('task4_results.json', 'w', encoding='utf-8') as file:
    json.dump(results, file, ensure_ascii=False)

# Закрытие подключения
client.close()
