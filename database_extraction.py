import datetime

import numpy as np
import pandas as pd
import pymongo.collection
from pymongo import MongoClient

client = MongoClient("localhost", 27017)


def read_infected_in_district():
    collection: pymongo.collection.Collection = client.upa.peopleRegionInfected
    aggregation = collection.aggregate(
        [
            {
                "$match": {
                    "datum": {"$gt": datetime.datetime(2021, 1, 1)},
                    "okres_lau_kod": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {
                        "okres_lau_kod": "$okres_lau_kod"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "okres_lau_kod": "$_id.okres_lau_kod",
                    "count": 1
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            }
        ]

    )
    df = pd.DataFrame(list(aggregation))
    df.to_csv("csv/infected_in_district.csv", index=False)


def read_vaccinated_in_district():
    collection: pymongo.collection.Collection = client.upa.peopleVaccinated
    aggregation = collection.aggregate([
        {
            "$match": {
                "datum": {"$gt": datetime.datetime(2021, 1, 1)},
                "ukoncujici_davka": True,
                "orp_bydliste_kod": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": {
                    "orp_bydliste_kod": "$orp_bydliste_kod"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "orp_bydliste_kod": {"$toInt": "$_id.orp_bydliste_kod"},
                "count": {"$toInt": "$count"}
            }
        },
        {
            "$project": {
                "_id": 0
            }
        }
    ])
    district = pd.read_csv("data/orp-lau.csv")
    df = pd.DataFrame(list(aggregation))
    data = pd.merge(df, district, how="left", left_on="orp_bydliste_kod", right_on="ORP")
    data = data.groupby(["LAU1"]).sum()
    data = data.filter(["LAU1", "count"])
    data.to_csv("csv/vaccinated_in_district.csv", index=True)


def vekova_skupina(row: pd.Series):
    if row["MAX_TUPY"] <= 15:
        return "0-14"
    elif row["MIN_OSTRY"] >= 15 and row["MAX_TUPY"] <= 60:
        return "15-59"
    elif row["MIN_OSTRY"] >= 60:
        return "60+"


def read_resident_district_age():
    collection: pymongo.collection.Collection = client.upa.districtAgeDistribution
    data = collection.find({})
    age = pd.DataFrame(list(data))
    age = age.rename(columns={"_id": "vek_kod"})

    orp_map = pd.read_csv("data/orp-lau.csv")
    data = pd.merge(age, orp_map, how="left", left_on="vuzemi_kod", right_on="ORP")
    data = data.groupby(["LAU1", "pohlavi_kod", "vek_kod"]).agg(
        {"pocet": "sum", "pohlavi_txt": "first", "vek_txt": "first", "vuzemi_txt": "first", "ZKRTEXT": "first",
         "TEXT": "first", "MIN_TUPY": "first", "MAX_TUPY": "first", "MIN_OSTRY": "first",
         "MAX_OSTRY": "first"}).reset_index()
    data = data.filter(["LAU1", "pohlavi_kod", "vek_kod", "pocet", "ZKRTEXT", "TEXT", "MAX_TUPY", "MIN_OSTRY"])

    data["vekova_skupina"] = data.apply(vekova_skupina, axis=1)
    data = data.groupby(["vekova_skupina", "LAU1"])["pocet"].sum().reset_index()
    data = pd.pivot_table(data, values="pocet", index="LAU1", columns="vekova_skupina", aggfunc=np.sum)
    data.to_csv("csv/district_age_distribution.csv")


def read_infected_age_and_sex():
    collection: pymongo.collection.Collection = client.upa.peopleRegionInfected
    aggregation = collection.aggregate(
        [
            {
                "$match": {
                    "vek": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {
                        "vek": "$vek",
                        "pohlavi": "$pohlavi"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "vek": "$_id.vek",
                    "pohlavi": "$_id.pohlavi",
                    "count": 1
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            }
        ]

    )
    df = pd.DataFrame(list(aggregation))
    df.to_csv("csv/infected_age_sex.csv", index=False)


def read_used_vaccines_in_regions():
    collection: pymongo.collection.Collection = client.upa.peopleVaccinated
    aggregation = collection.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "kraj_nuts": "$kraj_nuts_kod",
                        "vakcina": "$vakcina"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "kraj_nuts": "$_id.kraj_nuts",
                    "vakcina": "$_id.vakcina",
                    "count": 1
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            }
        ]
    )
    df = pd.DataFrame(list(aggregation))
    df.to_csv("csv/used_vaccines_in_region.csv", index=False)


def read_month_stats():
    collection: pymongo.collection.Collection = client.upa.monthlyStats
    aggregation = collection.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$datum"},
                        "month": {"$month": "$datum"}
                    },
                    "prirustkovy_pocet_ag_testu": {"$sum": "$prirustkovy_pocet_provedenych_ag_testu"},
                    "prirustkovy_pocet_nakazenych": {"$sum": "$prirustkovy_pocet_nakazenych"},
                    "prirustkovy_pocet_provedenych_testu": {"$sum": "$prirustkovy_pocet_provedenych_testu"},
                    "prirustkovy_pocet_umrti": {"$sum": "$prirustkovy_pocet_umrti"},
                    "prirustkovy_pocet_vylecenych": {"$sum": "$prirustkovy_pocet_vylecenych"}
                }
            },
            {
                "$project": {
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "prirustkovy_pocet_ag_testu": 1,
                    "prirustkovy_pocet_nakazenych": 1,
                    "prirustkovy_pocet_provedenych_testu": 1,
                    "prirustkovy_pocet_umrti": 1,
                    "prirustkovy_pocet_vylecenych": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {
                    "year": 1,
                    "month": 1
                }
            }
        ]
    )
    df = pd.DataFrame(list(aggregation))

    collection_hospitalized: pymongo.collection.Collection = client.upa.hospitalized
    hospitalized = collection_hospitalized.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$datum"},
                        "month": {"$month": "$datum"}
                    },
                    "hospitalizace": {"$sum": "$pocet_hosp"}
                }
            },
            {
                "$project": {
                    "month": "$_id.month",
                    "year": "$_id.year",
                    "hospitalizace": 1
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            }
        ])
    hospitalized_df = pd.DataFrame(list(hospitalized))
    df = df.merge(hospitalized_df, left_on=["month", "year"], right_on=["month", "year"])
    df.to_csv("csv/monthly_stats.csv", index=False)


def read_vaccinated_in_region():
    collection: pymongo.collection.Collection = client.upa.peopleVaccinated
    aggregation = collection.aggregate(
        [
            {
                "$match": {
                    "kraj_nuts_kod": {"$ne": None},
                    "ukoncujici_davka": True
                }
            },
            {
                "$group": {
                    "_id": {
                        "kraj_nuts_kod": "$kraj_nuts_kod"
                    },
                    "vaccinated": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "kraj_nuts": "$_id.kraj_nuts_kod",
                    "vaccinated": 1
                }
            },
            {
                "$project": {
                    "_id": 0
                }
            }
        ]
    )
    df = pd.DataFrame(list(aggregation))
    df.to_csv("csv/vaccinated_in_region.csv", index=False)


def read_infected_age_in_regions():
    collection: pymongo.collection.Collection = client.upa.peopleRegionInfected
    aggregation = collection.aggregate(
        [
            {
                "$match": {
                    "kraj_nuts_kod": {"$ne": None},
                    "vek": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {
                        "kraj": "$kraj_nuts_kod",
                        "vek": "$vek"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "kraj": "$_id.kraj",
                    "vek": "$_id.vek",
                    "count": 1,
                    "_id": 0
                }
            },
            {
                "$match": {
                    "vek": {"$exists": "true"},
                    "kraj": {"$exists": "true"}
                }
            },
            {
                "$sort": {
                    "vek": 1
                }
            }
        ]
    )

    obj = pd.DataFrame(list(aggregation))
    obj.to_csv("csv/infected_age_in_region.csv", index=False)


def get_region_count_data():
    # data from: https://www.czso.cz/csu/czso/porovnani-kraju section Demografie
    # Get region count data from csv file
    region_count = pd.read_csv("data/ukazatele_kraje_demogr.csv").replace(
        to_replace={"2020": {",": ""}}
    )
    region_count.set_index("NUTS 3")
    for index in region_count.index:
        region_count["2020"][index] = int(region_count["2020"][index].replace(",", ""))
    return region_count


def read_infected_by_date_region():
    region_count = get_region_count_data()

    collection: pymongo.collection.Collection = client.upa.peopleRegionInfected
    aggregation = collection.aggregate(
        [
            {
                "$match": {
                    "kraj_nuts_kod": {"$ne": None},
                    "vek": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$datum"},
                        "month": {"$month": "$datum"},
                        "kraj": "$kraj_nuts_kod",
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "kraj": "$_id.kraj",
                    "count": 1,
                    "_id": 0
                }
            }
        ]
    )
    data = pd.DataFrame(list(aggregation))

    data["quarter"] = ((data["month"] - 1) // 3 + 1)  # compute year quarter

    # Join region count data and infected data
    merged_data = pd.merge(data, region_count, how="left", left_on="kraj", right_on="NUTS 3")

    merged_data = merged_data[
        ["count", "year", "month", "quarter", "kraj", "Název kraje", "2020"]]  # Keep only needed columns
    merged_data = merged_data.rename(columns={"count": "nakazenych", "2020": "celkovy pocet"})  # Rename columns

    merged_data.to_csv("csv/infected_region.csv", index=False)


if __name__ == '__main__':
    read_resident_district_age()
    read_infected_in_district()
    read_vaccinated_in_district()
    read_infected_age_and_sex()
    read_used_vaccines_in_regions()
    read_vaccinated_in_region()
    read_vaccinated_in_region()
    read_infected_age_in_regions()
    read_infected_by_date_region()
    read_month_stats()
    read_resident_district_age()
