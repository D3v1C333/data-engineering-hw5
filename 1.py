import json
import pickle
from pymongo import MongoClient


def connect():
    client = MongoClient()
    db = client["test-database"]
    return db.person


def get_from_pickle(filename):
    with open(filename, 'rb') as file:
        data = pickle.load(file)
    return data


def insert_many(collection, data):
    collection.insert_many(data)


def json_output(filename, data):
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2, default=str)


def sort_by_salary(collection):
    items = []
    for person in collection.find({}, limit=10).sort({'salary': -1}):
        items.append(person)
    json_output("./output/1_sort_by_salary.json", items)


def filtered_by_age(collection):
    items = []
    for person in (collection
            .find({"age": {"$lt": 30}}, limit=15)
            .sort({"salary": -1})):
        items.append(person)
    json_output("./output/1_filtered_by_age.json", items)


def filtered_by_city_and_job(collection):
    items = []
    for person in (collection
            .find({"city": "Барселона", "job": {"$in": ["Повар", "Учитель", "Программист"]}}, limit=10).sort(
        {"age": 1})):
        items.append(person)
    json_output("./output/1_filtered_by_city_and_job.json", items)


def count_obj(collection):
    result = collection.count_documents({
        "age": {"$gt": 25, "$lt": 35},
        "year": {"$gte": 2019, "$lte": 2022},
        "$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]
    })
    data = {"count_obj_sort_by_year_age_salary": result}
    json_output("./output/1_count_obj.json", data)


# data = get_from_pickle("./input/task_1_item.pkl")
# insert_many(connect(), data)
sort_by_salary(connect())
filtered_by_age(connect())
filtered_by_city_and_job(connect())
count_obj(connect())
