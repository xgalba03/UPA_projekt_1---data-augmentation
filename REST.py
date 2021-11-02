import datetime
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
    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.min.json"
    json_data = get_data(url)
    print("Saving to database")
    collection: pymongo.collection.Collection = client.upa.regionVaccinated
    collection.delete_many({})
    collection.insert_many(json_data["data"])
    print("==========completed==========")


def fix_date(json_data):
    for data in json_data["data"]:
        date = datetime.date.fromisoformat(data["datum"])
        data["datum"] = datetime.datetime(date.year, date.month, date.day)


def get_data(url):
    print("Sending GET")
    response = requests.get(url)
    print("Parsing json")
    json_data = response.json()

    print("Fixing date")
    fix_date(json_data)

    return json_data


if __name__ == '__main__':
    monthly_stats()
    people_region_infected_stats()
    people_vaccinated_region_stats()
