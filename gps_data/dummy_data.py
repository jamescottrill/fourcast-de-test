# Imports
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery, storage

#Declare Variables
west_limit = -87.9401
south_limit = 41.6439
east_limit = -87.5240
north_limit = 42.0230
seed = 345
storage_bucket = '%STORAGE_BUCKET%'
project = '%PROJECT%'


def get_taxi_ids():
    """
    Query the taxi trips table and get all the taxi Ids
    :return: Dataframe of taxi Ids.
    """
    bq = bigquery.Client.from_service_account_json('service-account.json', project=project)
    query = bq.query('SELECT DISTINCT taxi_id FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`')
    rows = query.result()
    df = bigquery.table.RowIterator.to_dataframe(rows)
    return df


def get_timestamps(timestamp):
    """
    Create the start and end timestamps for the data period. The period is 30 mins long.
    :param timestamp: Starting timestamp
    :return: Start timestamp in POSIX and end timestamp
    """
    time_start = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    time_end = time_start + timedelta(minutes=30)
    start = (int)(time_start.strftime('%s'))
    end = (int)(time_end.strftime('%s'))
    return start, end


def create_data(start, end, seed=345):
    """
    Create the dummy data with random values for all columns between the minimum and maximum for each field.
    :param seed: Seed to ensure data can be reproduced
    :return: A dataframe of dummy data 100 rows long.
    """
    ids = get_taxi_ids()
    np.random.seed(seed)
    rnd_ind = np.random.choice(ids.shape[0], size=100)
    df = pd.DataFrame({
        'taxi_id': [l.tolist()[0] for l in ids.iloc[rnd_ind].values],
        'timestamp': np.random.randint(start, end, size=100).tolist(),
        'latitude': np.random.uniform(low=west_limit, high=east_limit, size=100).tolist(),
        'longitude': np.random.uniform(low=south_limit, high=north_limit, size=100).tolist(),
    },
        columns=["taxi_id", "timestamp", "latitude", "longitude"]
    )
    return df


def upload_file(df, file):
    """
    Upload the dummy data CSV file to cloud storage.
    :param df: The dataframe to be uploaded
    :param file: The target filename
    :return: None
    """
    df.to_csv(file, index=False)
    gs = storage.Client.from_service_account_json('service-account.json')
    bucket = gs.bucket(bucket_name=storage_bucket)
    blob = bucket.blob(file)
    with open(file, 'rb') as source_file:
        blob.upload_from_file(source_file)
        source_file.close()
    return


def main():
    """
    Function to create three sets of dummy data and upload to GCS
    :return:
    """
    time_start_unix, time_end_unix = get_timestamps('2020-06-27 19:00:00')
    df = create_data(time_start_unix, time_end_unix, seed)
    filename = 'chicago_data_{}.csv'.format(time_start_unix)
    upload_file(df, filename)
    time_start_unix, time_end_unix = get_timestamps('2020-06-27 19:30:00')
    filename = 'chicago_data_{}.csv'.format(time_start_unix)
    df = create_data(time_start_unix, time_end_unix, seed)
    upload_file(df, filename)
    time_start_unix, time_end_unix = get_timestamps('2020-06-27 20:00:00')
    filename = 'chicago_data_{}.csv'.format(time_start_unix)
    df = create_data(time_start_unix, time_end_unix, seed)
    upload_file(df, filename)

