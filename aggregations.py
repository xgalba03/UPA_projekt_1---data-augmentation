import matplotlib.pyplot as plt
import numpy as np
import pymongo.collection
import pandas as pd

from pymongo import MongoClient

client = MongoClient("localhost", 27017)


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


def plot_monthly_stats():
    data = pd.read_csv("csv/monthly_stats.csv")
    x = [f"{row['month']}.{row['year']}" for i, row in
         data[["month", "year"]].iterrows()]

    data.index = x  # set index values as month.date string values
    axs = data.plot(
        figsize=(8, 10),
        grid=True,
        y=["prirustkovy_pocet_ag_testu",
           "prirustkovy_pocet_nakazenych",
           "prirustkovy_pocet_provedenych_testu",
           "prirustkovy_pocet_umrti",
           "prirustkovy_pocet_vylecenych"],
        subplots=True,
    )

    for ax in axs:
        ax.set_xlabel("Datum")
        ax.set_ylabel("Počet")
    plt.suptitle("Měsíční statistiky epidemie COVID-19")
    plt.tight_layout()
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


def plot_infected_in_region_age():
    region_data = get_region_count_data()[["Název kraje", "NUTS 3"]]
    data = pd.read_csv("csv/infected_age_in_region.csv")

    data = pd.merge(data, region_data, how="left", left_on="kraj", right_on="NUTS 3")

    df = data.loc[data.index.repeat(data["count"])]  # Multiply age rows by count column
    df = df.drop(columns="count")  # Drop redundant count column
    ax = df.boxplot(by="Název kraje", )  # Boxplot grouped by kraj column

    ax.set_xlabel("Kraj")
    ax.set_ylabel("Vek")
    plt.xticks(rotation=90)
    plt.title("Rozložení věku infikovaných v krajích")
    plt.suptitle("")
    plt.tight_layout()
    plt.show()


def get_quarter(quarter, year) -> pd.DataFrame:
    """
    Gets regional covid data for quarter
    :param quarter: year quarter. Should be value from 1 to 3
    :param year: year for which to get data
    :return: Data frame with quarter covid data
    """
    data = pd.read_csv("csv/infected_region.csv")
    quarters = data.groupby(["year", "quarter", "kraj"]).agg(
        {"nakazenych": "sum", "Název kraje": "first", "celkovy pocet": "first"}
    )  # group by quarters and sum infected count
    quarters["infected/person"] = np.divide(quarters["nakazenych"], quarters["celkovy pocet"])
    quarters = quarters.reset_index()  # reset multi index from group by

    return quarters.loc[(quarters["quarter"] == quarter) & (quarters["year"] == year)]


def print_quarter_rating(quarter, year):
    """
    Prints sorted regional covid data
    :param quarter: year quarter. Should be value from 1 to 3
    :param year: year for which to print data
    """
    quarter_data = get_quarter(quarter, year)
    sorted_quarter = quarter_data.sort_values(by=["infected/person"], ascending=False)
    data_for_print = sorted_quarter.filter(items=["Název kraje", "infected", "infected/person"]).reset_index(drop=True)
    data_for_print.rename({"infected/person": "infikovaných na jednu osobu"})
    data_for_print.index += 1
    print(f"{'=' * 15} {quarter}. čtvrtletí {year} {'=' * 15}")
    print(data_for_print)


def print_best_in_covid():
    """
    Prints covid region rating for 4 quarters
    """
    print_quarter_rating(3, 2021)
    print()
    print_quarter_rating(2, 2021)
    print()
    print_quarter_rating(1, 2021)
    print()
    print_quarter_rating(3, 2020)


def plot_quarter(quarter, year):
    """
    Plots single quarter covid data into bar chart
    :param quarter: quarter number should be in range from 1 to 3
    :param year: year
    """
    quarter_data = get_quarter(quarter, year)
    quarter_data = quarter_data.set_index("Název kraje", drop=True)
    quarter_data.rename(columns={"infected/person": "Infikovaných na jednu osobu"})

    ax = quarter_data.plot(figsize=(8, 10), kind="bar", logy=True, y=["nakazenych", "celkovy pocet"], )
    ax.set_ylabel("Počet lidí")
    ax1 = ax.twinx()
    ax1.set_ylabel("Počet infikovaných na osobu")
    quarter_data.plot(y=["infected/person"], ax=ax1, color="k")
    plt.xticks(rotation=90)
    plt.title("Čtvrtlení statistika infikovaných v krajích")
    plt.tight_layout()
    plt.show()


def get_region_vaccinate_percentage() -> pd.DataFrame:
    region_count = get_region_count_data()[["NUTS 3", "Název kraje", "2020"]]
    vaccination = pd.read_csv("csv/vaccinated_in_region.csv")
    data = pd.merge(vaccination, region_count, how="left", left_on="kraj_nuts", right_on="NUTS 3")
    data["vaccinated percentage"] = (data["vaccinated"] / data["2020"]) * 100
    data = data.set_index("Název kraje", drop=True)
    return data


def plot_region_vaccinate_percentage():
    data = get_region_vaccinate_percentage()

    ax = data.plot(figsize=(8, 10), kind="bar", y="vaccinated percentage")
    plt.xticks(rotation=90)
    ax.set_ylabel("Procento očkovaných lidí")
    plt.title("Procentualni počet infikovanych v krajích")
    plt.tight_layout()
    plt.show()


def plot_used_vaccines_in_regions():
    region_count = get_region_count_data()[["NUTS 3", "Název kraje"]]
    data = pd.read_csv("csv/used_vaccines_in_region.csv")
    data = pd.merge(data, region_count, how="left", left_on="kraj_nuts", right_on="NUTS 3")
    data = data.set_index("Název kraje")

    data = pd.pivot_table(data, values="count", index="Název kraje", columns="vakcina", aggfunc=np.sum)
    ax = data.plot(figsize=(10, 8), kind="bar", logy=True)
    ax.set_ylabel("Počet očkování")
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    read_used_vaccines_in_regions()
    read_vaccinated_in_region()
    read_vaccinated_in_region()
    read_infected_age_in_regions()
    read_infected_by_date_region()
    read_month_stats()
    plot_monthly_stats()
    plot_infected_in_region_age()
    print_best_in_covid()
    plot_quarter(3, 2020)
    plot_region_vaccinate_percentage()
    plot_used_vaccines_in_regions()
