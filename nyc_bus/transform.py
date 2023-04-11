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

    # extract hour component from 'RecordedAtTime' and 'ScheduledArrivalTime'
    data['RecordedAtTimeHour'] = (data['RecordedAtTime'].dt.hour).astype(int)
    data['ScheduledArrivalTimeHour'] = (
        data['ScheduledArrivalTime'].apply(lambda x: x.split(':')[0]).astype(int)
    )

    # calculate 2 diff types
    data['DiffOriginal'] = (
        data['RecordedAtTimeHour'] - data['ScheduledArrivalTimeHour']
    ).abs()
    data['DiffMod'] = (
        data['RecordedAtTimeHour'] - (data['ScheduledArrivalTimeHour'] % 24)
    ).abs()

    # add 'RecordedAtTime' date to 'ScheduledArrivalTime'
    data['ScheduledArrivalTime'] = data[
        'RecordedAtTime'
    ].dt.normalize() + pd.to_timedelta(data['ScheduledArrivalTime'])

    # special case 1 :
    #   - scheduled arrival time hour < 24, diff. original >= tolerance
    #   - e.g., '2017-06-15 19:15:00' vs. '23:01:01'
    #   - action : subtract 1 day from 'ScheduledArrivalTime'
    mask = (data['ScheduledArrivalTimeHour'] < 24) & (data['DiffOriginal'] >= tolerance)
    data.loc[mask, 'ScheduledArrivalTime'] = data[mask][
        'ScheduledArrivalTime'
    ] - pd.to_timedelta(1, unit='d')

    # special case 2 :
    #   - scheduled arrival time hour >= 24, diff. mod < tolerance
    #   - e.g., '2017-06-16 01:15:00' vs. '25:01:01'
    #   - action :
    #       - since 'ScheduledArrivalTimeHour' >= 24, it exceeds 'RecordedAtTime' by 1 day
    #       - as such, subtract 1 day from 'ScheduledArrivalTime'
    mask = (data['ScheduledArrivalTimeHour'] >= 24) & (data['DiffMod'] < tolerance)
    data.loc[mask, 'ScheduledArrivalTime'] = data[mask][
        'ScheduledArrivalTime'
    ] - pd.to_timedelta(1, unit='d')

    # convert 'ScheduledArrivalTime' to datetime
    data['ScheduledArrivalTime'] = pd.to_datetime(data['ScheduledArrivalTime'])

    # remove extra columns
    data = data.drop(
        ['RecordedAtTimeHour', 'ScheduledArrivalTimeHour', 'DiffOriginal', 'DiffMod'],
        axis=1,
    )

    return data


def convert_to_datetime(data: pd.DataFrame, columns: list[str]) -> None:
    """
    converts columns in passed list to datetime format
    """
    data[columns] = data[columns].apply(
        pd.to_datetime, errors='coerce', infer_datetime_format=True
    )
