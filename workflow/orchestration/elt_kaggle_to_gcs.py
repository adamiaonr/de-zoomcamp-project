import os
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi  # pylint: disable-msg=E0611
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

def get_kaggle_client() -> KaggleApi:
  """
  Returns an authenticated Kaggle client.
  Assumes ~/.kaggle exists OR KAGGLE_USERNAME and KAGGLE_KEY env variables are set.
  """
  client = KaggleApi()
  client.authenticate()

  return client

@task()
def write_to_gcs(path: Path) -> None:
  """Upload local file to GCS"""
  gcs_block_name = os.getenv('PREFECT_GCS_BUCKET_BLOCK')
  print(gcs_block_name)
  gcs_block = GcsBucket.load(gcs_block_name)
  gcs_block.upload_from_path(from_path=path, to_path=path)

@task()
def create_local_dir() -> Path:
  """Creates local directory to save downloaded files"""
  path = Path(f"data")
  path.mkdir(parents=True, exist_ok=True)

  return path

@task(retries=3)
def download_from_kaggle(month: int, output_dir: Path) -> Path:
  """
  Downloads the dataset file for the specified month from Kaggle into local path.
  Returns path of saved file.
  Data is downloaded in compressed .zip format.
  """
  kaggle_id = os.getenv(
    'KAGGLE_DATASET_ID', 'stoney71/new-york-city-transport-statistics'
  )
  file_path = f"mta_17{int(month):02d}.csv"
  get_kaggle_client().dataset_download_file(
    kaggle_id,
    file_name=file_path,
    path=output_dir,
    force=False
  )

  return output_dir/f"{file_path}.zip"

@flow()
def elt_main_flow(months: list[int] = [6]) -> None:
  """
  Given a list of months: 
    1. Downloads dataset files from Kaggle, one for each month
    2. Loads each file to GCS
  No data transformations are applied at this point.
  """
  output_dir = create_local_dir()
  for month in months:
    file_path = download_from_kaggle(month, output_dir)
    write_to_gcs(file_path)

if __name__ == "__main__":
  months=[6]
  elt_main_flow(months)
