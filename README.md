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
As of now, the final transformations are manually performed by running `dbt` locally.

![Screenshot 2023-05-04 at 18 59 30](https://user-images.githubusercontent.com/5468601/236289636-846e72f2-4ee5-421b-b0d7-9145fd078f8b.png)

### Why the two 'Ts'? The issue with the `ScheduledArrivalTimeColumn` column...

The `ScheduledArrivalTime` column sometimes shows time values such as `24:05:00`, without a date, which is quite annoying...
In order to pass it to a format like `2022-08-01 00:05:00` we apply the function [`fix_scheduled_arrival_time()`](https://github.com/adamiaonr/de-zoomcamp-project/blob/master/nyc_bus/transform.py#L4).

Unfortunately, I did not find a nice way to handle this with `dbt` and macros, so I ended up just transforming the data before the **L**oad step.

### Dashboard

![Screenshot 2023-05-03 at 23 55 33](https://user-images.githubusercontent.com/5468601/236068518-59225297-d89f-4b91-9edf-dbf14ffca9a2.png)

1. Select {month, bus line(s), bus direction(s), day(s) of week}
2. Sort by average delay per bus line on a table (you can click on a line of interest)
3. Check the average bus delay per weekday on bar chart
4. Check the delay hotspots for the selected lines on the map

Try it out [here](https://lookerstudio.google.com/reporting/f500306b-9ba7-42d6-bfb6-92f570ff240b)!

## Final production tables

## Usage

## Details

### Why do I use E(T)LT? The issue with the `ScheduledArrivalTimeColumn` column

### Model lineage

## Open questions & future work

## Self-evaluation
