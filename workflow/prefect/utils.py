import json
import os
from pathlib import Path

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi  # pylint: disable-msg=E0611
from prefect import task
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials


@task()
def fetch_kaggle_credentials() -> dict:
    """
    fetches Kaggle API credentials from secret block
    """
    kaggle_credentials_block = os.getenv("PREFECT_KAGGLE_CREDENTIALS_BLOCK")
    secret_block = Secret.load(kaggle_credentials_block)
    kaggle_credentials = json.loads(secret_block.get())

    return kaggle_credentials


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
def get_kaggle_client() -> KaggleApi:
    """
    returns an authenticated Kaggle client
    assumes KAGGLE_USERNAME and KAGGLE_KEY env variables are set
    """
    client = KaggleApi()
    client.authenticate()

    return client


@task(retries=3)
def download_kaggle_dataset(
    client: KaggleApi, dataset_id: str, file_path: Path, force=False
) -> None:
    client.dataset_download_file(
        dataset_id, file_name=file_path.name, path=file_path.parent, force=force
    )


@task(log_prints=True, retries=3)
def send_to_bigquery(data: pd.DataFrame, table_name: str, chunksize: int) -> None:
    """
    sends data to google bigquery.
    extracts configs from environment.
    """
    gcp_project_id = os.getenv("GCP_PROJECT_ID")
    gcp_credentials_block_name = os.getenv("PREFECT_GCP_CREDENTIALS_BLOCK")
    gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)

    # ensure column names have no spaces in them
    data.columns = [c.replace(' ', '') for c in data.columns]

    data.to_gbq(
        destination_table=table_name,
        project_id=gcp_project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=chunksize,
        if_exists="append",
    )

    print(f"loaded {len(data)} rows to {table_name} in GCP BQ")


@task()
def create_local_dir(path: Path = Path("data")) -> Path:
    """
    creates local directory to save data
    """
    path.mkdir(parents=True, exist_ok=True)

    return path
