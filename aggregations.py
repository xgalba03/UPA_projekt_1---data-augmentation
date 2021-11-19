import matplotlib.pyplot as plt
import pymongo.collection
import pandas as pd
import matplotlib.dates as mdates

from pymongo import MongoClient

client = MongoClient("localhost", 27017)


def aggregate_months():
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
    csv = df.to_csv()
    f = open("csv/monthly_stats.csv", "w")
    f.write(csv)


def plot_monthly_stats():
    data = pd.read_csv("csv/monthly_stats.csv")
    x = [f"{row['month']}.{row['year']}" for i, row in data[["month", "year"]].iterrows()] #Todo: nastavit popisky x osy
    data.plot(
        x_compat=True,
        figsize=(10, 20),
        grid=True,
        y=["prirustkovy_pocet_ag_testu",
           "prirustkovy_pocet_nakazenych",
           "prirustkovy_pocet_provedenych_testu",
           "prirustkovy_pocet_umrti",
           "prirustkovy_pocet_vylecenych"],
        subplots=True
    )
    plt.tight_layout()
    plt.show()


def plot_line(date, value, label):
    plt.plot(date, value)
    plt.title(label)
    plt.xticks(rotation=45)
    plt.grid()
    plt.show()


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
    csv = obj.to_csv()
    f = open("csv/infected_age_in_region.csv", "w")
    f.write(csv)
    f.close()


if __name__ == '__main__':
    # read_infected_age_in_regions()
    aggregate_months()
    plot_monthly_stats()
