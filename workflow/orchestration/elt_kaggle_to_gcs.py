import os
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi  # pylint: disable-msg=E0611
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.blocks.system import Secret

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
  gcs_block = GcsBucket.load(gcs_block_name)
  gcs_block.upload_from_path(from_path=path, to_path=path)

@task()
def create_local_dir() -> Path:
  """Creates local directory to save downloaded files"""
  path = Path(f"data")
  path.mkdir(parents=True, exist_ok=True)

  return path

@task(retries=3)
def download_from_kaggle(month: int, output_dir: Path, kaggle_dataset_id: str) -> Path:
  """
  Downloads the dataset file for the specified month from Kaggle into local path.
  Returns path of saved file.
  Data is downloaded in compressed .zip format.
  """
  # download dataset file for month
  file_path = f"mta_17{int(month):02d}.csv"
  get_kaggle_client().dataset_download_file(
    kaggle_dataset_id,
    file_name=file_path,
    path=output_dir,
    force=False
  )

  return output_dir/f"{file_path}.zip"

@task()
def fetch_kaggle_credentials() -> tuple[str]:
  """Fetches Kaggle API credentials from secret block"""
  kaggle_credentials_block = os.getenv('PREFECT_KAGGLE_CREDENTIALS_BLOCK')
  secret_block = Secret.load(kaggle_credentials_block)
  kaggle_credentials = secret_block.get()

  return kaggle_credentials.split(',')

@task()
def set_kaggle_credentials(username: str, key: str) -> None:
  """Sets passed Kaggle API username and key as environment variables"""
  os.environ['KAGGLE_USERNAME'] = username
  os.environ['KAGGLE_KEY'] = key

@flow()
def elt_main_flow(
  months: list[int] = [6],
  kaggle_dataset_id: str = 'stoney71/new-york-city-transport-statistics'
) -> None:
  """
  Given a list of months: 
    1. Downloads dataset files from Kaggle, one for each month
    2. Loads each file to GCS
  No data transformations are applied at this point.
  """
  # fetch kaggle API user and key from secret block
  username, key = fetch_kaggle_credentials()
  # set kaggle credentials as env variables
  set_kaggle_credentials(username, key)
  
  output_dir = create_local_dir()
  for month in months:
    file_path = download_from_kaggle(month, output_dir, kaggle_dataset_id)
    write_to_gcs(file_path)

if __name__ == "__main__":
  months=[6]
  elt_main_flow(months)
