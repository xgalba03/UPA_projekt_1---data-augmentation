import pandas as pd
import numpy as np
from scipy import stats

data = pd.read_csv("csv/combined_district_data.csv")


def calculate_percentages():
    data["infected_percentage"] = data["infected_count"] / (data["0-14"] + data["15-59"] + data["60+"])
    data["vaccinated_percentage"] = data["vaccination_count"] / (data["0-14"] + data["15-59"] + data["60+"])
    data["kids_percentage"] = data["0-14"] / (data["0-14"] + data["15-59"] + data["60+"])


def min_max_normalization():
    data["normalized_infected_count"] = (data["infected_count"] - data["infected_count"].min()) / (
            data["infected_count"].max() - data["infected_count"].min())


def infected_percent_discretization():
    data["infection_category"] = pd.qcut(data["infected_percentage"], q=3, labels=["Malý", "Střední", "Velký"])
    data["vaccination_category"] = pd.qcut(data["vaccinated_percentage"], q=3, labels=["Malý", "Střední", "Velký"])


def outliers_detection():
    condition = [
        (stats.zscore(data["kids_percentage"]) > 3),
        (stats.zscore(data["kids_percentage"]) < -3),
    ]
    values = ["outlier+", "outlier-"]
    data["outlier"] = np.select(condition, values)


def outliers_replace():
    upper_quantile = data["kids_percentage"].quantile(.95)
    lower_quantile = data["kids_percentage"].quantile(.05)
    data["kids_percentage"] = np.where(data["outlier"] == "outlier+", upper_quantile, data["kids_percentage"])
    data["kids_percentage"] = np.where(data["outlier"] == "outlier-", lower_quantile, data["kids_percentage"])
    pass


def csv_export():
    data.to_csv("csv/district_data_final.csv", index=False)


if __name__ == '__main__':
    calculate_percentages()
    outliers_detection()
    outliers_replace()
    infected_percent_discretization()
    min_max_normalization()
