# de-zoomcamp-project
Final project for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp), 2023 cohort.

## Motivation

The goal of this project is to process the records of a bus service in New York City (NYC) and analyze the delays of registered at the bus stops.

<img width="384" alt="Screenshot 2023-05-03 at 23 58 26" src="https://user-images.githubusercontent.com/5468601/236068696-e8ae368e-66a6-44cf-a4c8-732ca9018756.png">

I try to answer the following (kind of silly...) questions:

* What is the average delay on any given bus line in NYC?
* How does the average delay differ between different days of the week?
* What is the average delay experienced in different bus stops along the route(s) of any given bus line?
* How does the average bus delay vary with different weather conditions?

## Data sources

I've used two data sources in this project:

| dataset | description | what do we use the data for? | link |
|---|---|---|---|
| New York City Bus Data | Live data recorded from NYC Buses - Location, Time, Schedule & more | Bus records used to calculate bus delay per line & stop | [link](https://www.kaggle.com/datasets/stoney71/new-york-city-transport-statistics) |
| Historical Hourly Weather Data 2012-2017 | Hourly weather data for 30 US & Canadian Cities + 6 Israeli Cities | Weather data for NYC, which we relate with bus delay data | [link](https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data) |

## Overview

### Project structure

```
├── nyc_bus/                        # code used for pre-GBQ data transformations
├── tests/                          # unit tests for nyc_bus/ code
├── workflow/
│   ├── dbt/
│   │   └── nyc_bus/                # dbt models used for data warehouse transformations
│   ├── prefect/
│   │   ├── trigger_dbt.py          # triggers dbt transformations
│   │   ├── upload_bus_data.py      # uploads bus data from Kaggle to GBQ
│   │   ├── upload_weather_data.py  # uploads weather data from Kaggle to GBQ
│   │   └── utils.py                # common functions used by prefect flows
│   └── terraform/                  # sets up infrastructure in GBQ
├── pyproject.toml                  # nyc_bus package configs + python packages
├── poetry.lock                     # .lock file for poetry python package manager
├── .pre-commit-config.yaml         # configs for pre-commit hooks
├── .gitignore
└── LICENSE
```

### Flow

I've used a ETLT pipeline that I partially orchestrate via Prefect.

![Screenshot 2023-05-04 at 18 59 30](https://user-images.githubusercontent.com/5468601/236289636-846e72f2-4ee5-421b-b0d7-9145fd078f8b.png)

* **1:** Use Prefect Cloud to (a) keep necessary blocks & deployments; and (b) trigger flows via the web UI. **Note:** the flows are ran in a local agent.
* **2, 3 and 4:** Two of the Prefect flows are used to fetch data from Kaggle, pre-transform it, and ingest it directly into Google BigQuery.
* **5:** An additional Prefect flow triggers all necessary `dbt` steps, i.e. installation of packages and transformations.
* **6:** A dashboard built in Google Looker / Data Studio presents the data

### Data transformations and delay computation

In the course, we typically talked about an ETL/ELT dichotomy.
As such, you may have noticed an extra 'T' in my pipeline description: E**T**LT.
The need for the extra transformation step is related with the computation of bus delays.

The original dataset is composed by 3 date & time columns:

| Field Name | Description |
|------------|-------------|
| `RecordedAtTime` | Date & time at which the bus status is recorded |
| `ScheduledArrivalTime` | Time at which the bus is scheduled to pass in the bus stop |
| `ExpectedArrivalTime` | Updated estimate of date & time at which the bus is expected at the stop |

We calculate the bus delay via the difference between `RecordedAtTime` and the `ScheduledArrivalTime` columns when the bus is known to be at a stop.
This is done in a transformation step on the data warehouse (see [this](https://github.com/adamiaonr/de-zoomcamp-project/blob/master/workflow/dbt/nyc_bus/models/intermediate/bus/int_bus_delays.sql) `dbt` model).

Unfortunately, the `ScheduledArrivalTime` column sometimes shows time values such as `27:15:00`, without a date.
In order to parse this to a format like `2022-08-01 03:15:00`, we use the function [`fix_scheduled_arrival_time()`](https://github.com/adamiaonr/de-zoomcamp-project/blob/master/nyc_bus/transform.py#L4).
This function uses the date of `RecordedAtTime` as the base date, and assumes that the difference between the `ScheduledArrivalTime` and `RecordDateTime` is within an interval of [-`H`, `H`] hours.
By default, we assume `H` = 12 hours.

I did not find a nice way to handle this with `dbt` and macros.
As such, I apply an initial transformation step, just before the load step to Google BigQuery.

### Dashboard

![Screenshot 2023-05-03 at 23 55 33](https://user-images.githubusercontent.com/5468601/236068518-59225297-d89f-4b91-9edf-dbf14ffca9a2.png)

1. Select {month, bus line(s), bus direction(s), day(s) of week}
2. Sort by average delay per bus line on a table (you can click on a line of interest)
3. Check the average bus delay per weekday on bar chart
4. Check the delay hotspots for the selected lines on the map

Try it out [here](https://lookerstudio.google.com/reporting/f500306b-9ba7-42d6-bfb6-92f570ff240b)!

## Production table

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| RecordId | STRING | Unique identifier for the record |
| RecordDateTime | TIMESTAMP | Date and time when the record was created |
| DayOfWeek | INTEGER | Day of the week (1-7) corresponding to the date of the record |
| BusLineId | STRING | Identifier for the bus line |
| BusLineName | STRING | Name of the bus line |
| BusLineDirection | INTEGER | Direction of the bus line (0 or 1) |
| BusLineOrigin | STRING | Origin of the bus line |
| BusLineDestination | STRING | Destination of the bus line |
| BusStopId | STRING | Identifier for the bus stop |
| BusStopName | STRING | Name of the bus stop |
| BusStopLocation | GEOGRAPHY | Location (latitude and longitude) of the bus stop |
| DelaySeconds | INTEGER | Delay time in seconds for the bus record, registered at time of arrival at the stop |
| Weather | STRING | Description of the weather at the time of the record |
| Humidity | FLOAT | Humidity percentage at the time of the record |
| Temperature | FLOAT | Temperature in Celsius at the time of the record |

## Model lineage

![Screenshot 2023-05-04 at 19 19 34](https://user-images.githubusercontent.com/5468601/236296520-7e64fb9b-fbd9-4109-a118-328b3a74f418.png)

* 3 layer approach, with `staging`, `intermediate` and `mart` layers
* In `staging/` we have the following transformations:
    1. **NYC Bus data:** (i) renaming of columns; (ii) combination of vehicle latitude and longitude into a single-point `GEOGRAPHY`; and (iii) creation of record, bus stop and bus line IDs
    2. **Weather data:** (i) tables are pivoted so that we have a `City` column (instead of 1 column per city); and (ii) temperature is converted from Kelvin to Celsius.
* In the `intermediate` layer I've created a `int_bus_stops` dimension table, which lists all bus stops, together with their estimated location. The location of the bus stop is estimated by taking the average of the bus vehicle's coordinates when the bus is recorded to be 'at the stop'.
* The `int_bus_delays` model is of type `incremental`. The idea is to incrementally calculate delays as new records become available in `stg_bus_records`.
* The `int_bus_delays` and `bus_delays` tables are partitioned by the `RecordDateTime` field, with a month granularity. This is because the dashboard shows data aggregated over 1-month periods.
* The `int_bus_delays` and `bus_delays` tables are clustered by `BusLineId`, `BusStopId` and `DayOfWeek`, following from the queries that are done in the dashboard.
* In addition to the `mart` layer, I've used the concept of `dbt` [metrics](https://docs.getdbt.com/docs/build/metrics) to calculate aggregations of the bus delay over a month

## Usage

1. Setup your environment for GCP development ([instructions](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp))

2. Setup `terraform` and initialize the infrastructure in GCP ([instructions](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp))

```
$ cd <repo-base>
$ cd workflow/terraform/
$ terraform init
$ terraform plan -var="project=<your-gcp-project-id>"
$ terraform apply -var="project=<your-gcp-project-id>"
```

3. Install `poetry` on your system, install project dependencies and initialize the virtual environment

```
$ pip install poetry
$ cd <repo-base>
$ poetry install
$ poetry shell
```

4. Setup Prefect orchestration. You'll need to setup a [Prefect Cloud](https://app.prefect.cloud/). Then create a workspace and the following blocks :

| Block type | Description |
|------------|-------------|
| GitHub | Used to get the Prefect flow code via a Github repository |
| GCP credentials | Keep a reference to your GCP credentials, allowing Prefect to interact with your GCP resources |
| Secret | Used to keep your Kaggle credentials, so that we can download Kaggle datasets |
| dbt CLI profile | Keeps a configuration for Prefect to run `dbt` against your GCP BigQuery data warehouse |
| dbt CLI BigQuery Target Configs | Complement to the above |

5. Create Prefect deployments for each of the flows:

```
$ cd <repo-base>
$ mkdir -p workflow/prefect/deployments/
$ prefect deployment build "workflow/prefect/upload_bus_data.py:etl_main_flow" --name "ETL Kaggle NYC bus data to BQ" --work-queue "main" --storage-block "github/<your-gitub-block-name>" --path "workflow/prefect/" -o "workflow/prefect/deployments/upload-bus-data-github-cloud.yaml"  --override env.PREFECT_GCP_CREDENTIALS_BLOCK="<your-gcp-credentials-block-name>" --override env.PREFECT_KAGGLE_CREDENTIALS_BLOCK="<your-secret-block-name>" --override env.GCP_PROJECT_ID="<your-gcp-project-id>" --override env.GCP_BQ_BUS_RECORDS_TABLE_NAME="bus_records" --override env.KAGGLE_USERNAME="." --override env.KAGGLE_KEY="." --override env.GCP_BQ_DATASET_NAME="nyc_bus_dataset"
```

```
$ prefect deployment build "workflow/prefect/upload_weather_data.py:elt_main_flow" --name "ELT Kaggle weather data to BQ" --work-queue "main" --storage-block "github/<your-gitub-block-name>" --path "workflow/prefect/" -o "workflow/prefect/deployments/upload-weather-data-github-cloud.yaml"  --override env.PREFECT_GCP_CREDENTIALS_BLOCK="<your-gcp-credentials-block-name>" --override env.PREFECT_KAGGLE_CREDENTIALS_BLOCK="<your-secret-block-name>" --override env.GCP_PROJECT_ID="<your-gcp-project-id>" --override env.KAGGLE_USERNAME="." --override env.KAGGLE_KEY="." --override env.GCP_BQ_DATASET_NAME="nyc_bus_dataset"
```

```
$ prefect deployment build "workflow/prefect/trigger_dbt.py:trigger_dbt_flow" --name "Trigger dbt transformations" --work-queue "main" --storage-block "github/<your-gitub-block-name>" --path "workflow/prefect/" -o "workflow/prefect/deployments/trigger-dbt-cloud.yaml"  --override env.DBT_CLI_PROFILE_BLOCK="<your-dbt-cli-profile-block-name>"
```

6. Apply the Prefect deployments:

```
$ prefect deployment apply workflow/prefect/deployments/*.yaml
```

7. Start a Prefect agent on your machine:

```
$ prefect agent start -q "main"
```

8. In the Prefect Cloud UI, select each of the 'upload' deployments in turn, and run a custom build.
Your local agent should start downloading data from Kaggle and upload it to GBQ.

9. Finally, you can also use the Prefect Cloud UI to run the `dbt` trigger Prefect flow, in order to perform the final transformations on GBQ.

At this point, you can take a look at the data in GBQ.
Alternatively, you can head up to [Google Looker Studio](https://lookerstudio.google.com/), connect data sources to your GBQ tables and create a dashboard.

## TODOs

1. Is there something to be gained by partitioning/clustering the table with 'raw' bus records? We can do this via `terraform`, which allows us to specify the schema of the table, along with other configs.
2. Join the different Prefect flows into a single script (or high-level flow).
3. Script to create blocks.
4. We have a lot of environment variables to keep track of. We need a smarter way to do this, and it doesn't need to be very complicated. E.g., a `.env` file + `python-dotenv`?.
5. Make the orchestration step even more 'one-click' by running it using [serverless Prefect flows](https://medium.com/the-prefect-blog/serverless-prefect-flows-with-google-cloud-run-jobs-23edbf371175).
