import csv
import json
from pymongo import MongoClient


def connect():
    client = MongoClient()
    db = client["test-database"]
    return db.titanic_person


def get_data():
    # Получаем данные из JSON
    with open('./input/titanic.json', 'r', encoding="utf-8") as json_file:
        data = json.load(json_file)

    # Получаем данные из CSV
    with open('./input/titanic.csv', 'r', encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for row in csv_reader:
            row['Age'] = float(row['Age'])
            row['Survived'] = int(row['Survived'])
            row['Pclass'] = int(row['Pclass'])
            row['Siblings/Spouses Aboard'] = int(row['Siblings/Spouses Aboard'])
            row['Parents/Children Aboard'] = int(row['Parents/Children Aboard'])
            row['Fare'] = float(row['Fare'])
            data.append(dict(row))
    return data


def insert_many(collection, data):
    collection.insert_many(data)


def json_output(filename, data):
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2, default=str)


# Выборка всех, у кого был билет первого класса
def filtered_by_pclass(collection):
    items = []
    for person in (collection
            .find({"Pclass": 1})):
        items.append(person)
    json_output("./output/4_filtered_by_pclass.json", items)


# Выборка несовершенолетних детей с двумя родителями на борту
def filtered_by_parents(collection):
    items = []
    for person in (collection
            .find({"Parents/Children Aboard": 2, "Age": {"$lt": 18}})):
        items.append(person)
    json_output("./output/4_filtered_by_parents.json", items)


# Выборка выживших, отсортированная по увеличению возраста
def filtered_by_survive(collection):
    items = []
    for person in (collection
            .find({"Survived": 1}).sort({"Age": 1})):
        items.append(person)
    json_output("./output/4_filtered_by_survive.json", items)


# Выборка кому меньше 25 и сортировка по уменьшению стоимости проезда на корабле
def filtered_by_age(collection):
    items = []
    for person in (collection
            .find({"Age": {"$lt": 25}})
            .sort({"Fare": -1})):
        items.append(person)
    json_output("./output/4_filtered_by_age.json", items)


# Выборка 15 мужчин, которые выжили после кораблекрушения
def filtered_by_sex(collection):
    items = []
    for person in (collection
            .find({"Sex": "male"}, limit=15)
            .sort({"Survived": -1})):
        items.append(person)
    json_output("./output/4_filtered_by_sex.json", items)


# Вывод минимального, среднего, максимального возраста
def get_stat_by_age(collection):
    items = []
    q = [{
        "$group": {
            "_id": "stat_by_age",
            "max": {"$max": "$Age"},
            "min": {"$min": "$Age"},
            "avg": {"$avg": "$Age"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/4_get_stat_by_age.json", items)


# Вывод количества людей в каждом классе с сортировкой по убыванию количества людей
def get_freq_by_pclass(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$Pclass",
            "count": {"$sum": 1}}}, {
        "$sort": {"count": -1}}
    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/4_get_freq_by_pclass.json", items)


# Статистика по возрасту для каждого класса
def get_stat_age_by_pclass(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$Pclass",
            "max": {"$max": "$Age"},
            "min": {"$min": "$Age"},
            "avg": {"$avg": "$Age"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/4_get_stat_age_by_pclass.json", items)


# Cтастика возраста для выживших и умерших
def get_stat_age_by_survive(collection):
    items = []
    q = [{
        "$group": {
            "_id": "$Survived",
            "max": {"$max": "$Age"},
            "min": {"$min": "$Age"},
            "avg": {"$avg": "$Age"}
        }
    }]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/4_get_stat_age_by_survive.json", items)


# Статистика стоимости билета для людей, кому больше 30 лет по классу билета
def get_fare_stat_by_pclass(collection):
    items = []
    q = [{
        "$match": {"Age": {"$gt": 30}}}, {
        "$group": {"_id": "$Pclass",
                   "max_fare": {"$max": "$Fare"},
                   "min_fare": {"$min": "$Fare"},
                   "avg_fare": {"$avg": "$Fare"}}},
    ]
    for row in collection.aggregate(q):
        items.append(row)
    json_output("./output/4_get_max_fare_by_min_age.json", items)


# Увеличить возраст всех людей на 1
def update_age(collection):
    res = collection.update_many({}, {"$inc": {"Age": 1}})
    print(res)


# Увеличить стоимость проезда на 15% для людей, купивших билет первого класса
def increase_fare(collection):
    res = collection.update_many({
        "$or": [{"Pclass": 1}]}, {
        "$mul": {"Fare": 1.15}})
    print(res)


# Уменьшение стоимости билета на 10% для людей с билетом второго и третьего класса, а также кому больше 50 лет
def decrease_fare(collection):
    res = collection.update_many({
        "$or": [{"Pclass": {"$gt": 1}},
                {"Age": {"$gt": 50}}]}, {
        "$mul": {"Fare": 0.9}})
    print(res)

#Удалить из коллекции всех несовершеннолетних
def delete_by_age(collection):
    res = collection.delete_many({
        "Age": {"$lt": 18}
    })
    print(res)

#Обновить для всех пассажиров 3 класса статус выживания на "выжил"
def update_survive_stat(collection):
    res = collection.update_many({"Pclass": 3}, {"$set": {"Survived": 1}})
    print(res)


# data = get_data()
# insert_many(connect(), data)
# filtered_by_pclass(connect())
# filtered_by_parents(connect())
# filtered_by_survive(connect())
# filtered_by_age(connect())
# filtered_by_sex(connect())
# get_stat_by_age(connect())
# get_freq_by_pclass(connect())
# get_stat_age_by_pclass(connect())
# get_stat_age_by_survive(connect())
# get_fare_stat_by_pclass(connect())
# update_age(connect())
# increase_fare(connect())
# decrease_fare(connect())
# delete_by_age(connect())
# update_survive_stat(connect())
