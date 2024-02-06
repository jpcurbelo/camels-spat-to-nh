import xarray as xr
import pandas as pd


def reduceDataByDay(dataset, set_vars, sum_vars):

    # Convert data to daily frequency
    day_dates = pd.to_datetime(dataset.coords["time"].values).normalize()
    day_dates = xr.DataArray(day_dates, name="time", dims="time")

    # Group by day and apply appropriate reduction method for each variable
    daily_data = xr.Dataset()
    for variable in dataset.data_vars:
        if variable in sum_vars:
            # print('sum', variable)
            daily_data[variable] = dataset[variable].groupby(day_dates).sum(dim="time")
        elif variable in set_vars:
            # print('mean', variable)
            daily_data[variable] = dataset[variable].groupby(day_dates).mean(dim="time")

    return daily_data


if __name__ == "__main__":
    pass