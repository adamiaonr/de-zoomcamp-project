import pandas as pd


def fix_scheduled_arrival_time(
    data: pd.DataFrame, tolerance: float = 12
) -> pd.DataFrame:
    """
    - fixes scheduled arrival times which are above '23:59:59', e.g. '25:12:14'
    - adds the correct date to 'ScheduledArrivalTime', converting it into a datetime
    """
    # ensure 'RecordedAtTime' is in pd.datetime format
    convert_to_datetime(data, ['RecordedAtTime'])

    # add 'ScheduledArrivalTime' to start of day of 'RecordedAtTime'
    data['ScheduledArrivalTime'] = data[
        'RecordedAtTime'
    ].dt.normalize() + pd.to_timedelta(data['ScheduledArrivalTime'].astype(str))

    # calculate time delta, in hours, between 'scheduled' and 'recorded' time
    time_delta = (
        data['RecordedAtTime'] - data['ScheduledArrivalTime']
    ).dt.total_seconds() / 3600.0

    # case 1 :
    #   - time delta > 12 hr
    #   - e.g., { recorded : 2023-04-01 23:00:00, scheduled : 2023-04-01 01:00:00 }
    #   - action : add 1 day to scheduled
    mask = time_delta > tolerance
    data.loc[mask, 'ScheduledArrivalTime'] = data[mask][
        'ScheduledArrivalTime'
    ] + pd.to_timedelta(1, unit='d')

    # case 2 :
    #   - time delta < -12 hr
    #   - e.g., { recorded : 2023-04-01 01:00:00, scheduled : 2023-04-01 23:00:00 }
    #   - action : subtract 1 day from scheduled
    mask = time_delta < -tolerance
    data.loc[mask, 'ScheduledArrivalTime'] = data[mask][
        'ScheduledArrivalTime'
    ] - pd.to_timedelta(1, unit='d')

    # other cases, time delta within tolerance : leave as it is

    return data


def convert_to_datetime(data: pd.DataFrame, columns: list[str]) -> None:
    """
    converts columns in passed list to datetime format
    """
    data[columns] = data[columns].apply(
        pd.to_datetime, errors='coerce', infer_datetime_format=True
    )
