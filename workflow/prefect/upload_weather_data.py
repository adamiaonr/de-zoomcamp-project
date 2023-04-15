import os
from pathlib import Path

import pandas as pd
from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from utils import (
    create_local_dir,
    download_kaggle_dataset,
    fetch_kaggle_credentials,
    get_kaggle_client,
    send_to_bigquery,
    set_kaggle_credentials,
)


@flow(log_prints=True, task_runner=SequentialTaskRunner())
def transform_and_upload(file_path: Path, chunksize: int = 100000) -> None:
    """
    takes a file path and uploads data to google bigquery.
    file is uploaded in chunks.
    """
    for _, chunk in enumerate(
        pd.read_csv(
            file_path,
            index_col=False,
            on_bad_lines="skip",
            compression="infer",
            chunksize=chunksize,
        )
    ):
        # send data to BQ
        table_name = (
            f"{os.getenv('GCP_BQ_DATASET_NAME')}.{file_path.name.split('.')[0]}"
        )
        send_to_bigquery(chunk, table_name, chunksize)

        print(f"loaded {len(chunk)} rows to GCP BQ")


@flow(task_runner=SequentialTaskRunner())
def download_from_kaggle(
    variable: str, output_dir: Path, kaggle_dataset_id: str
) -> Path:
    """
    downloads the dataset file for the specified month from Kaggle into local path
    returns path of saved file
    """
    file_path = f"{variable}.csv"

    client = get_kaggle_client()
    download_kaggle_dataset(client, kaggle_dataset_id, output_dir / file_path)

    # file is downloaded in .zip format, hence the '.zip' suffix
    # afaik, Kaggle API doesn't allow to get the file name
    return output_dir / f"{file_path}.zip"


@flow(log_prints=True, task_runner=SequentialTaskRunner())
def elt_main_flow(
    variables: list[str],
    kaggle_dataset_id: str = "selfishgene/historical-hourly-weather-data",
) -> None:
    """
    given a list of months:
      1. downloads dataset files from Kaggle, one for each variable
      2. uploads each file to GCP BigQuery
    """
    credentials = fetch_kaggle_credentials()
    set_kaggle_credentials(credentials)

    output_dir = create_local_dir()

    for variable in variables:
        file_path = download_from_kaggle(variable, output_dir, kaggle_dataset_id)
        transform_and_upload(file_path)


if __name__ == "__main__":
    elt_main_flow(variables=['humidity', 'temperature', 'weather_description'])
