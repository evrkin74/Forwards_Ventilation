"""
datesandtime.py

This file contains all functions related to the management of time variables
"""

import numpy as np

def sec_to_datetime_365day(t, year0=2000, month0=1, day0=1):
    import cftime
    import datetime
    import pandas as pd
    import dask.dataframe as dd

    """
    Returns a DatetimeNoLeap object for a given elapsed time, t (seconds).
    Supports scalar values, pandas.Series, or dask.Series.
    """

    t0 = cftime.datetime(year0, month0, day0, calendar='noleap')

    """
    Returns a DatetimeNoLeap object for a given elapsed time, t (seconds)
    """

    return t.map_partitions(
            lambda partition: partition.map(
                lambda ti: (t0 + datetime.timedelta(seconds=int(ti))).year
            ),
            meta=('year', 'int64')  # Output is an integer (year)
        )

def sec_to_year(t, year0=2000, month0=1, day0=1):
    t = sec_to_datetime_365day(t, year0=year0, month0=month0, day0=day0)
    return t.year

def sec_to_month(t, year0=2000, month0=1, day0=1):
    t = sec_to_datetime_365day(t, year0=year0, month0=month0, day0=day0)
    return t.month

def sec_to_day(t, year0=2000, month0=1, day0=1):
    t = sec_to_datetime_365day(t, year0=year0, month0=month0, day0=day0)
    return t.day

def sec_to_dayofyear(t, year0=2000, month0=1, day0=1):
    t = sec_to_datetime_365day(t, year0=year0, month0=month0, day0=day0)
    return t.timetuple().tm_yday