'''Functions to compute WMO Climatological Normals periods'''

# Standard libraries
import datetime as dt

# Third-party libraries
import pandas as pd
import xarray as xr

def standard():
    '''Returns the current standard reference period'''
    year = dt.date.today().year
    end = year - year % 10 # Most recent year ending with 0
    start = end - 29
    return pd.Period(year=start, freq="30Y")

def all_periods():
    '''Returns all standard reference periods'''
    end = standard().start_time
    return pd.PeriodIndex(
        pd.date_range(start='1901', end=end, freq="10YS"),
        freq="30Y"
    )

def historic():
    '''Returns the standard reference period for long-term assessment'''
    return pd.Period(year=1961, freq="30Y")

def annual(ds, period):
    '''Computes the annual average for a period'''

    ds_period = ds.sel(time=slice(period.start_time, period.end_time))
    ds_period_mean = ds_period.mean(dim='time')

    return ds_period_mean

def monthly(ds, period):
    '''Computes the monthly averages for a period'''

    ds_period = ds.sel(time=slice(period.start_time, period.end_time))
    ds_period_mean = ds_period.groupby('time.month').mean(dim="time")

    return ds_period_mean
