import datetime
import json
from urllib.request import urlopen
import pandas as pd
import ijson
import pymongo.collection
from pymongo import MongoClient
from pymongo import UpdateOne

client = MongoClient("localhost", 27017)


def monthly_stats():
    print("==========infected monthly==========")
    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/nakazeni-vyleceni-umrti-testy.json"
    collection: pymongo.collection.Collection = client.upa.monthlyStats
    collection.delete_many({})

    f = urlopen(url)
    people = ijson.items(f, 'data.item')
    for person in people:
        fix_date_item(person)
        collection.insert_one(person)

    print("==========completed==========")


def people_region_infected_stats():
    print("==========infected in regions==========")
    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.min.json"
    collection: pymongo.collection.Collection = client.upa.peopleRegionInfected
    collection.delete_many({})

    f = urlopen(url)
    people = ijson.items(f, 'data.item')
    insert_to_db(people, collection)
    print("==========completed==========")


def people_vaccinated_region_stats():
    print("==========vaccinated people in regions==========")
    collection: pymongo.collection.Collection = client.upa.regionVaccinated
    collection.delete_many({})

    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.min.json"
    f = urlopen(url)
    people = ijson.items(f, 'data.item')
    insert_to_db(people, collection)
    print("==========completed==========")


def people_vaccinated_all():
    print("==========all vaccinated people==========")
    collection: pymongo.collection.Collection = client.upa.peopleVaccinated
    collection.delete_many({})

    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-profese.min.json"
    f = urlopen(url)
    people = ijson.items(f, 'data.item')
    insert_to_db(people, collection)
    print("==========completed==========")

def total_population():
    print("==========total population==========")
    collection: pymongo.collection.Collection = client.upa.totalPopulation
    collection.delete_many({})

    data = pd.read_csv('https://www.czso.cz/documents/62353418/143522504/130142-21data043021.csv/760fab9c-d079-4d3a-afed-59cbb639e37d?version=1.1')
    csv_insert_to_db(data.to_dict('records'), collection)
    print("==========completed==========")    


def fix_date_item(item):
    date = datetime.date.fromisoformat(item["datum"])
    item["datum"] = datetime.datetime(date.year, date.month, date.day)

def fix_csv_date_item(item):
    date = datetime.date.fromisoformat(item["casref_do"])
    item["casref_do"] = datetime.datetime(date.year, date.month, date.day)


def insert_to_db(iterable, collection, chunk=100000):
    people_chunk = []
    index = 0
    for item in iterable:
        if index % chunk == 1:
            #print(index)
            collection.insert_many(people_chunk)
            people_chunk.clear()
        fix_date_item(item)
        people_chunk.append(item)
        index += 1

    collection.insert_many(people_chunk)

def csv_insert_to_db(iterable, collection, chunk=100000):
    people_chunk = []
    index = 0
    for item in iterable:
        if index % chunk == 1:
            #print(index)
            collection.insert_many(people_chunk)
            people_chunk.clear()
        fix_csv_date_item(item)
        people_chunk.append(item)
        index += 1

    collection.insert_many(people_chunk)


def insert_hospitalized_db(iterable, collection, chunk=100000):
    data_chunk = []
    index = 0
    for item in iterable:
        if index % chunk == 1:
            collection.bulk_write(data_chunk)
            data_chunk.clear()
        item["_id"] = item.pop("id")
        fix_date_item(item)
        data_chunk.append(UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True))
        index += 1

    collection.bulk_write(data_chunk)


def hospitalized():
    print("==========hospitalized statistics==========")
    collection: pymongo.collection.Collection = client.upa.hospitalized

    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/hospitalizace.min.json"
    f = urlopen(url)
    people = ijson.items(f, 'data.item')
    insert_hospitalized_db(people, collection)
    print("==========completed==========")


def age_distribution():
    print("==========age distribution statistics==========")
    age = pd.read_csv("data/ciselnik-intervalu.csv").filter(["CHODNOTA", "ZKRTEXT", "TEXT", "MIN_TUPY", "MAX_TUPY", "MIN_OSTRY", "MAX_OSTRY"])
    df = pd.read_csv("data/rozlozeni-veku-obyvatel.csv").filter(["idhod", "hodnota", "pohlavi_kod", "vek_kod", "vuzemi_kod", "pohlavi_txt", "vek_txt", "vuzemi_txt"])
    df = df.rename(columns={"idhod": "_id", "hodnota" : "pocet"})
    df = df.merge(age, left_on="vek_kod", right_on="CHODNOTA").drop(["CHODNOTA", "vek_kod"], axis=1)

    data = json.loads(df.to_json(orient="records"))
    collection: pymongo.collection.Collection = client.upa.districtAgeDistribution
    collection.delete_many({})
    collection.insert_many(data)
    print("==========completed==========")


if __name__ == '__main__':
    # monthly_stats()
    # people_region_infected_stats()
    # people_vaccinated_region_stats()
    # people_vaccinated_all()
    # total_population()
    # hospitalized()
    age_distribution()

