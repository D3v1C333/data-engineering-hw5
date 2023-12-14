import json
import pickle
import re

from pymongo import MongoClient


def connect():
    client = MongoClient()
    db = client["test-database"]
    return db.person


def insert_many(collection, filename):
    items = []
    with open(filename, "r", encoding="utf-8") as file:
        file_content = file.read()

    # Разделение данных на записи по "====="
    records = re.split(r"=====\s*", file_content.strip())

    # Обработка каждой записи и добавление в items
    for record in records:
        if record:
            data = {}
            lines = record.strip().split('\n')
            for line in lines:
                key, value = line.split("::")
                data[key.lower()] = int(value.strip()) if key.lower() in ["salary", "id", "year",
                                                                          "age"] else value.strip()
            items.append(data)
    # Добавление записей в коллекцию MongoDB
    collection.insert_many(items)


def json_output(filename, data):
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2, default=str)


def get_stat_by_salary(collection):
    items = []
    q = [{
        "$group": {
            "_id": "stat_by_salary",
            "max": {"$max": "$salary"},
            "min": {"$min": "$salary"},
            "avg": {"$avg": "$salary"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_stat_by_salary.json", items)


def get_freq_by_job(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$job",
            "count": {"$sum": 1}}}, {
        "$sort": {"count": -1}}
    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_freq_by_job.json", items)


def get_stat_salary_by_city(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$city",
            "max": {"$max": "$salary"},
            "min": {"$min": "$salary"},
            "avg": {"$avg": "$salary"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_stat_salary_by_city.json", items)


def get_stat_salary_by_job(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$job",
            "max": {"$max": "$salary"},
            "min": {"$min": "$salary"},
            "avg": {"$avg": "$salary"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_stat_salary_by_job.json", items)


def get_stat_age_by_city(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$city",
            "max": {"$max": "$age"},
            "min": {"$min": "$age"},
            "avg": {"$avg": "$age"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_stat_age_by_city.json", items)


def get_stat_age_by_job(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$job",
            "max": {"$max": "$age"},
            "min": {"$min": "$age"},
            "avg": {"$avg": "$age"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_stat_age_by_job.json", items)


def get_max_salary_by_min_age(collection):
    items = []
    q = [{
        "$group": {"_id": "age",
                   "age": {"$min": "$age"},
                   "max_salary": {"$max": "$salary"}}}, {
        "$match": {"age": 18}}
    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_max_salary_by_min_age.json", items)


def get_min_salary_by_max_age(collection):
    items = []
    q = [{
        "$group": {"_id": "age",
                   "age": {"$max": "$age"},
                   "min_salary": {"$min": "$salary"}}}, {
        "$match": {"age": 65}}
    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_get_min_salary_by_max_age.json", items)


def big_query_1(collection):
    items = []
    q = [{
        "$match": {"salary": {"$gt": 50_000}}}, {
        "$group": {"_id": "$city",
                   "max_age": {"$max": "$age"},
                   "min_age": {"$min": "$age"},
                   "avg_age": {"$avg": "$age"}}
    }, {
        "$sort": {"avg_age": 1}}
    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_big_query_1.json", items)


def big_query_2(collection):
    items = []
    q = [{
        "$match": {
            "city": {"$in": ["Барселона", "Малага", "Астана", "Тбилиси"]},
            "job": {"$in": ["Учитель", "Психолог", "Врач", "Повар"]},
            "$or": [{"age": {"$gt": 18, "$lt": 25}},
                    {"age": {"$gt": 50, "$lt": 65}}]}}, {
        "$group": {"_id": "res",
                   "max_salary": {"$max": "$salary"},
                   "min_salary": {"$min": "$salary"},
                   "avg_salary": {"$avg": "$salary"}}},
    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_big_query_2.json", items)


def custom_query(collection):
    items = []
    q = [{
        "$match": {
            "city": {"$in": ["Москва"]},}}, {
        "$group": {"_id": "$job",
                   "max_salary": {"$max": "$salary"},
                   "min_salary": {"$min": "$salary"},
                   "avg_salary": {"$avg": "$salary"}}},{
        "$sort": {"max_salary": -1}
    }

    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/2_custom_query.json", items)


# insert_many(connect(), "./input/task_2_item.text")
get_stat_by_salary(connect())
get_freq_by_job(connect())
get_stat_salary_by_city(connect())
get_stat_salary_by_job(connect())
get_stat_age_by_city(connect())
get_stat_age_by_job(connect())
get_max_salary_by_min_age(connect())
get_min_salary_by_max_age(connect())
big_query_1(connect())
big_query_2(connect())
custom_query(connect())
