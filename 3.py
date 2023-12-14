import json
from pymongo import MongoClient


def connect():
    client = MongoClient()
    db = client["test-database"]
    return db.person


def get_from_json(filename):
    with open(filename, 'r', encoding="utf-8") as file:
        items = json.load(file)
    return items


def insert_many(collection, data):
    collection.insert_many(data)


def delete_by_salary(collection):
    res = collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25_000}},
            {"salary": {"$gt": 175_000}},
        ]
    })
    print(res)


def update_age(collection):
    res = collection.update_many({}, {"$inc": {"age": 1}})
    print(res)


def increase_salary_by_job(collection):
    res = collection.update_many({
        "job": {"$in": ["Программист", "Водитель", "Строитель", "Архитектор"]}}, {
        "$mul": {"salary": 1.05}})
    print(res)


def increase_salary_by_city(collection):
    res = collection.update_many({
        "city": {"$in": ["Барселона", "Москва", "Астана", "Бланес"]}}, {
        "$mul": {"salary": 1.07}})
    print(res)


# Поднять зарплату на 10% тем, кому больше 50 лет || живет в городах Луго, Вроцлав, Бленес, Льйеда || работает программистом, IT-специалистом, косметологом или учителем
def increase_salary(collection):
    res = collection.update_many({
        "$or": [{"city": {"$in": ["Луго", "Вроцлав", "Бланес", "Льейда"]}},
                {"job": {"$in": ["Программист", "IT-специалист", "Косметолог", "Учитель"]}},
                {"age": {"$gt": 50}}]}, {
        "$mul": {"salary": 1.1}})
    print(res)


# Удалить из коллекции программистов и водителей, проживающих в Скопье
def delete_by_city_and_salary(collection):
    res = collection.delete_many({
        "$and": [
            {"city": {"$in": ["Скопье"]}},
            {"job": {"$in": ["Программист", "Водитель"]}},
        ]
    })
    print(res)

# data = get_from_json("./input/task_3_item.json")
# insert_many(connect(), data)

# delete_by_salary(connect())
# update_age(connect())
# increase_salary_by_job(connect())
# increase_salary_by_city(connect())
# increase_salary(connect())
# delete_by_city_and_salary(connect())
