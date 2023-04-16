import os
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from utils import (
    create_local_dir,
    download_kaggle_dataset,
    fetch_kaggle_credentials,
    get_kaggle_client,
    send_to_bigquery,
    set_kaggle_credentials,
)

from nyc_bus.transform import fix_scheduled_arrival_time


@flow(log_prints=True, task_runner=SequentialTaskRunner())
def transform_and_upload(file_path: Path, chunksize: int = 100000) -> None:
    """
    takes a file path, applies transformations and uploads
    transformed data to google bigquery.
    file is uploaded in chunks.
    """
    # - some .csv rows are split into 18 columns instead of 17
    # - this is due an occasional comma in the value of the 'NextStopPointName' column
    # - typical pattern : "<stop name> (non-public,for GEO)"
    # - we simply skip these rows
    for _, chunk in enumerate(
        pd.read_csv(
            file_path,
            index_col=False,
            on_bad_lines="skip",
            compression="infer",
            chunksize=chunksize,
        )
    ):
        # apply transformations
        chunk = transform(chunk)
        # send data to BQ
        table_name = (
            f'{os.getenv("GCP_BQ_DATASET_NAME")}.'
            f'{os.getenv("GCP_BQ_BUS_RECORDS_TABLE_NAME")}'
        )
        send_to_bigquery(chunk, table_name, chunksize)


@task()
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
    applies transformations to data:
        1. remove '.' from 'VehicleLocation.*' columns
        2. drop rows with null 'RecordedAtTime' and 'ScheduledArrivalTime' columns
        3. fix 'ScheduledArrivalTime' column, which can have values > '23:59:50'
    """
    data.columns = [c.replace('.', '') for c in data.columns]
    data = data.dropna(subset=['RecordedAtTime', 'ScheduledArrivalTime']).reset_index(
        drop=True
    )
    data = fix_scheduled_arrival_time(data)

    return data


@flow(task_runner=SequentialTaskRunner())
def download_from_kaggle(month: int, output_dir: Path, kaggle_dataset_id: str) -> Path:
    """
    downloads the dataset file for the specified month from Kaggle into local path
    returns path of saved file
    """
    file_path = f"mta_17{int(month):02d}.csv"

    client = get_kaggle_client()
    download_kaggle_dataset(client, kaggle_dataset_id, output_dir / file_path)

    # file is downloaded in .zip format, hence the '.zip' suffix
    # afaik, Kaggle API doesn't allow to get the file name
    return output_dir / f"{file_path}.zip"


@flow(log_prints=True, task_runner=SequentialTaskRunner())
def etl_main_flow(
    months: list[int],
    kaggle_dataset_id: str = "stoney71/new-york-city-transport-statistics",
) -> None:
    """
    given a list of months:
      1. downloads dataset files from Kaggle, one for each month
      2. loads each file to GCP BigQuery
    """
    credentials = fetch_kaggle_credentials()
    set_kaggle_credentials(credentials)

    output_dir = create_local_dir()

    for month in months:
        file_path = download_from_kaggle(month, output_dir, kaggle_dataset_id)
        transform_and_upload(file_path)


if __name__ == "__main__":
    etl_main_flow(months=[6])
