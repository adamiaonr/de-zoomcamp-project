import json
import os
from pathlib import Path

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi  # pylint: disable-msg=E0611
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials


@flow(log_prints=True)
def upload_to_bq(file_path: Path, chunksize: int = 100000) -> None:
    """
    takes a file path and uploads the file to GCP BigQuery
    file is uploaded in chunks
    """
    gcp_project_id = os.getenv("GCP_PROJECT_ID")
    gcp_credentials_block_name = os.getenv("PREFECT_GCP_CREDENTIALS_BLOCK")
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    bq_table_name = os.getenv("GCP_BQ_TABLE_NAME")

    # apply a minimal transformation :
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
        # minor transformation : remove '.' from 'VehicleLocation.*' columns
        chunk.columns = [c.replace('.', '') for c in chunk.columns]
        chunk.to_gbq(
            destination_table=bq_table_name,
            project_id=gcp_project_id,
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=chunksize,
            if_exists="append",
        )
        print(f"loaded {len(chunk)} rows to GCP BQ")


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


@flow()
def download_from_kaggle(month: int, output_dir: Path, kaggle_dataset_id: str) -> Path:
    """
    downloads the dataset file for the specified month from Kaggle into local path
    returns path of saved file
    """
    file_path = f"mta_17{int(month):02d}.csv"

    client = get_kaggle_client()
    download_dataset(client, kaggle_dataset_id, output_dir / file_path)

    # file is downloaded in .zip format, hence the '.zip' suffix
    # afaik, Kaggle API doesn't allow to get the file name
    return output_dir / f"{file_path}.zip"


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


@flow(log_prints=True)
def elt_main_flow(
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
        upload_to_bq(file_path)


if __name__ == "__main__":
    elt_main_flow(months=[6])
