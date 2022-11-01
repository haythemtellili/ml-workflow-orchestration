from datetime import timedelta

import numpy as np
import pandas as pd
from prefect.tasks import task_input_hash
from sklearn import preprocessing
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine

import config
from prefect import flow, get_run_logger, task


def dummify_dataset(df: pd.DataFrame, column: str):
    df = pd.concat([df, pd.get_dummies(df[column], prefix=column)], axis=1)
    df = df.drop([column], axis=1)
    return df


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def pre_process_data(bikes: pd.DataFrame):
    date = pd.DatetimeIndex(bikes["dteday"])

    bikes["year"] = date.year

    bikes["dayofweek"] = date.dayofweek

    bikes["year_season"] = bikes["year"] + bikes["season"] / 10

    bikes["hour_workingday_casual"] = bikes[["hr", "workingday"]].apply(
        lambda x: int(10 <= x["hr"] <= 19), axis=1
    )

    bikes["hour_workingday_registered"] = bikes[["hr", "workingday"]].apply(
        lambda x: int(
            (x["workingday"] == 1 and (x["hr"] == 8 or 17 <= x["hr"] <= 18))
            or (x["workingday"] == 0 and 10 <= x["hr"] <= 19)
        ),
        axis=1,
    )

    by_season = bikes.groupby("year_season")[["cnt"]].median()
    by_season.columns = ["count_season"]

    bikes = bikes.join(by_season, on="year_season")

    # One-Hot-Encoding
    columns_to_dummify = ["season", "weathersit", "mnth"]
    for column in columns_to_dummify:
        bikes = dummify_dataset(bikes, column)

    # Normalize features - scale
    numerical_features = ["temp", "atemp", "hum", "windspeed", "hr"]
    bikes.loc[:, numerical_features] = preprocessing.scale(
        bikes.loc[:, numerical_features]
    )

    # remove unused columns
    bikes.drop(columns=["instant", "dteday", "registered", "casual"], inplace=True)

    # use better column names
    bikes.rename(
        columns={
            "yr": "year",
            "mnth": "month",
            "hr": "hour_of_day",
            "holiday": "is_holiday",
            "workingday": "is_workingday",
            "weathersit": "weather_situation",
            "temp": "temperature",
            "atemp": "feels_like_temperature",
            "hum": "humidity",
            "cnt": "rented_bikes",
        },
        inplace=True,
    )
    return bikes


@task(retries=3, retry_delay_seconds=5)
def load_data(config):
    engine = create_engine(
        f"postgresql://{config.user}:{config.password}@{config.host}/{config.database}",
    )
    query = """SELECT * FROM bike_data"""
    data = pd.read_sql(query, con=engine)
    return data


@task
def split_data(data: pd.DataFrame):
    # Split the dataset randomly into 70% for training and 30% for testing.
    X = data.drop("rented_bikes", axis=1)
    y = data.rented_bikes
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, train_size=0.7, test_size=0.3, random_state=42
    )
    return X_train, X_test, y_train, y_test


@task
def train_model(X_train: pd.DataFrame, y_train: pd.DataFrame):
    # create model instance: GBRT (Gradient Boosted Regression Tree)
    model = GradientBoostingRegressor()
    # Model Training
    model.fit(X_train, y_train)
    return model


# Evaluation Metrics
@task
def rmse_score(
    X_test: pd.DataFrame, y_test: pd.DataFrame, model: GradientBoostingRegressor
):
    y_pred = model.predict(X_test)
    score = np.sqrt(mean_squared_error(y_test, y_pred))
    return score


@flow
def main():
    logger = get_run_logger()
    data = load_data(config)
    data_processed = pre_process_data(data)
    X_train, X_test, y_train, y_test = split_data(data_processed)
    model = train_model(X_train, y_train)
    score = rmse_score(X_test, y_test, model)
    logger.info(f"The RMSE is: {score}")
