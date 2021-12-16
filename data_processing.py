import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from database_extraction import get_region_count_data


def combine_district_values():
    vaccination = pd.read_csv("csv/vaccinated_in_district.csv")
    vaccination = vaccination.rename(columns={"count": "vaccination_count"})
    infected = pd.read_csv("csv/infected_in_district.csv")
    infected = infected.rename(columns={"count": "infected_count"})
    age_distribution = pd.read_csv("csv/district_age_distribution.csv")
    district_names = pd.read_csv("data/ciselnik-okresu.csv")
    district_names = district_names.filter(["CHODNOTA", "TEXT"])
    district_names = district_names.rename(columns={"TEXT": "název"})

    data = pd.merge(vaccination, infected, how="left", left_on="LAU1", right_on="okres_lau_kod")
    data = data.merge(age_distribution, how="left", left_on="LAU1", right_on="LAU1")
    data = data.merge(district_names, how="left", left_on="LAU1", right_on="CHODNOTA")
    data = data.drop(["okres_lau_kod", "CHODNOTA"], axis=1)
    data.to_csv("csv/combined_district_data.csv", index=False)


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
           "prirustkovy_pocet_vylecenych",
           "hospitalizace"],
        subplots=True,
    )

    for ax in axs:
        ax.set_xlabel("Datum")
        ax.set_ylabel("Počet")
    plt.suptitle("Měsíční statistiky epidemie COVID-19")
    plt.tight_layout()
    plt.show()


def plot_age_sex():
    data = pd.read_csv("csv/infected_age_sex.csv")
    df = data.loc[data.index.repeat(data["count"])]
    df = df.drop(columns="count")
    ax = df.boxplot(figsize=(8, 10), by="pohlavi")
    ax.set_ylabel("Vek")
    ax.set_xlabel("Pohlaví")

    plt.title("Rozložení věku infikovaných v rámci pohlaví ")
    plt.suptitle("")
    plt.tight_layout()
    plt.show()


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
    print_quarter_rating(4, 2020)
    print()
    print_quarter_rating(3, 2021)
    print()
    print_quarter_rating(2, 2021)
    print()
    print_quarter_rating(1, 2021)



def plot_quarter(quarter, year):
    """
    Plots single quarter covid data into bar chart
    :param quarter: quarter number should be in range from 1 to 3
    :param year: year
    """
    quarter_data = get_quarter(quarter, year)
    quarter_data = quarter_data.set_index("Název kraje", drop=True)
    quarter_data.rename(columns={"infected/person": "Infikovaných na jednu osobu"})
    quarter_data = quarter_data.sort_values('infected/person', ascending = False)

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
    plt.title("Procentualni počet očkovaných v krajích")
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
    combine_district_values()
    plot_monthly_stats()
    plot_infected_in_region_age()
    print_best_in_covid()
    plot_quarter(4, 2020)
    plot_region_vaccinate_percentage()
    plot_used_vaccines_in_regions()
    plot_age_sex()
