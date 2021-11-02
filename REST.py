import datetime
import json
from urllib.request import urlopen

import ijson
import pymongo.collection
import requests
from pymongo import MongoClient

client = MongoClient("localhost", 27017)


def monthly_stats():
    print("==========infected monthly==========")
    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/nakazeni-vyleceni-umrti-testy.json"
    json_data = get_data(url)
    print("Saving database")
    collection: pymongo.collection.Collection = client.upa.monthlyStats
    collection.delete_many({})
    collection.insert_many(json_data["data"])
    print("==========completed==========")


def people_region_infected_stats():
    print("==========infected in regions==========")
    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.json"
    json_data = get_data(url)
    print("Saving to database")
    collection: pymongo.collection.Collection = client.upa.peopleRegionInfected
    collection.delete_many({})
    collection.insert_many(json_data["data"])
    print("==========completed==========")


def people_vaccinated_region_stats():
    print("==========vaccinated people in regions==========")
    # url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.min.json"
    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-profese.min.json"
    json_data = get_data(url)
    print("Saving to database")
    collection: pymongo.collection.Collection = client.upa.regionVaccinated
    collection.delete_many({})
    collection.insert_many(json_data["data"])
    print("==========completed==========")


def people_vaccinated_all():
    print("==========vaccinated people in regions==========")
    collection: pymongo.collection.Collection = client.upa.peopleVaccinated
    collection.delete_many({})

    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-profese.min.json"
    f = urlopen(url)
    people = ijson.items(f, 'data.item')
    i = 0
    for person in people:
        i += 1
        if i % 1000 == 0:
            print(i)
        fix_date_item(person)
        collection.insert_one(person)
    print("==========completed==========")


def fix_date(json_data):
    for data in json_data["data"]:
        date = datetime.date.fromisoformat(data["datum"])
        data["datum"] = datetime.datetime(date.year, date.month, date.day)


def fix_date_item(item):
    date = datetime.date.fromisoformat(item["datum"])
    item["datum"] = datetime.datetime(date.year, date.month, date.day)


def get_data(url):
    print("Sending GET")
    response = requests.get(url)
    print("Parsing json")
    json.loads()
    json_data = response.json()

    print("Fixing date")
    fix_date(json_data)

    return json_data


if __name__ == '__main__':
    # monthly_stats()
    # people_region_infected_stats()
    # people_vaccinated_region_stats()
    people_vaccinated_all()
