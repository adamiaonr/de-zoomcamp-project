# pylint: disable=unsubscriptable-object
import unittest

import numpy as np
import pandas as pd

from nyc_bus.transform import fix_scheduled_arrival_time


class TestTransform(unittest.TestCase):
    def test_fix_scheduled_arrival_time(self):
        """
        test function that fixes 'ScheduledArrivalTime' column
        """
        data = pd.DataFrame(
            {
                'RecordedAtTime': [
                    '2023-04-11 20:00:00',
                    '2023-04-11 20:00:00',
                    '2023-04-12 02:00:00',
                    '2023-04-12 02:00:00',
                ],
                'ScheduledArrivalTime': [
                    '21:00:00',
                    '27:00:00',
                    '21:00:00',
                    '27:00:00',
                ],
                'FixedTime': [
                    '2023-04-11 21:00:00',
                    '2023-04-12 03:00:00',
                    '2023-04-11 21:00:00',
                    '2023-04-12 03:00:00',
                ],
            }
        )

        fixed_data = fix_scheduled_arrival_time(data.copy())

        np.testing.assert_array_equal(
            fixed_data['ScheduledArrivalTime'].values,
            pd.to_datetime(fixed_data['FixedTime'], infer_datetime_format=True).values,
        )

        fixed_data = fix_scheduled_arrival_time(data.copy(), tolerance=7)

        np.testing.assert_array_equal(
            fixed_data['ScheduledArrivalTime'].values,
            pd.to_datetime(fixed_data['FixedTime'], infer_datetime_format=True).values,
        )
