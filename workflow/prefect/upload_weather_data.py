import json
import os
from pathlib import Path

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi  # pylint: disable-msg=E0611
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.task_runners import SequentialTaskRunner
from prefect_gcp import GcpCredentials


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
        table_name = f"{os.getenv('GCP_BQ_TABLE_NAME')}.{file_path.stem}"
        send_to_bq(chunk, table_name, chunksize)

        print(f"loaded {len(chunk)} rows to GCP BQ")


@task(log_prints=True, retries=3)
def send_to_bq(data: pd.DataFrame, table_name: str, chunksize: int) -> None:
    """
    sends data to google bigquery.
    extracts configs from environment.
    """
    gcp_project_id = os.getenv("GCP_PROJECT_ID")
    gcp_credentials_block_name = os.getenv("PREFECT_GCP_CREDENTIALS_BLOCK")
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)

    print(f"{gcp_project_id}")
    print(f"{gcp_credentials_block_name}")
    print(f"{table_name}")

    data.to_gbq(
        destination_table=table_name,
        project_id=gcp_project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=chunksize,
        if_exists="append",
    )


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
    download_dataset(client, kaggle_dataset_id, output_dir / file_path)

    # file is downloaded in .zip format, hence the '.zip' suffix
    # afaik, Kaggle API doesn't allow to get the file name
    return output_dir / f"{file_path}.zip"


@task()
def get_kaggle_client() -> KaggleApi:
    """
    returns an authenticated Kaggle client
    assumes KAGGLE_USERNAME and KAGGLE_KEY env variables are set
    """
    client = KaggleApi()
    client.authenticate()

    return client


@task(retries=3)
def download_dataset(
    client: KaggleApi, dataset_id: str, file_path: Path, force=False
) -> None:
    client.dataset_download_file(
        dataset_id, file_name=file_path.name, path=file_path.parent, force=force
    )


@task()
def create_local_dir(path: Path = Path("data")) -> Path:
    """
    creates local directory to save data
    """
    path.mkdir(parents=True, exist_ok=True)

    return path


@task()
def set_kaggle_credentials(credentials: dict) -> None:
    """
    sets passed username and key as KAGGLE_USERNAME and KAGGLE_KEY env variables
    """
    try:
        os.environ["KAGGLE_USERNAME"] = credentials['KAGGLE_USERNAME']
        os.environ["KAGGLE_KEY"] = credentials['KAGGLE_KEY']
    except KeyError as exc:
        raise KeyError(
            (
                f"could not find Kaggle credential keys {credentials.keys()}:"
                " did you set your Kaggle secret block correctly?"
            )
        ) from exc


@task()
def fetch_kaggle_credentials() -> dict:
    """
    fetches Kaggle API credentials from secret block
    """
    kaggle_credentials_block = os.getenv("PREFECT_KAGGLE_CREDENTIALS_BLOCK")
    secret_block = Secret.load(kaggle_credentials_block)
    kaggle_credentials = json.loads(secret_block.get())

    return kaggle_credentials


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
