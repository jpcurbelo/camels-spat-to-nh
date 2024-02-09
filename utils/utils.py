import os
import yaml
import xarray as xr
import pandas as pd


def reduceDataByDay(dataset, set_vars, sum_vars):
    '''
    Reduce the input dataset to daily frequency
    Args:
        dataset: xarray.Dataset, input dataset
        set_vars: list, variables to be averaged
        sum_vars: list, variables to be summed
    Returns:
        daily_data: xarray.Dataset, dataset with daily frequency
    '''

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

def load_util_data(root_dir):
    '''
    Load data from data_dir.yml and data_general.yml
    Args:
        root_dir: str, path to root directory
    Returns:
        data_dir: dict, data from data_dir.yml
        data_gen: dict, data from data_general.yml
    '''
    # **Load Data**
    data_dir_path = os.path.join(root_dir, "utils", "data_dir.yml")
    with open(data_dir_path, 'r') as ymlfile:
        data_dir = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
    data_gen_path = os.path.join(root_dir, "utils", "data_general.yml")
    with open(data_gen_path, 'r') as ymlfile:
        data_gen = yaml.load(ymlfile, Loader=yaml.FullLoader)
               
    return data_dir, data_gen

def get_unusable_basins(input_files_dir, unusable_file):
    '''
    Load unusable basins from csv file
    Args:
        input_files_dir: str, path to input files directory
        unusable_file: str, name of unusable basins file
    Returns:
        unusuable_basins: list, list of unusable basins
    '''
    # Load csv
    unusuable_basins_df = pd.read_csv(os.path.join(input_files_dir, unusable_file))
    # # Country + '_' Station_id
    # unusuable_basins = ['_'.join((row['Country'], str(row['Station_id']))) for _, row in unusuable_basins_df.iterrows()]
    
    # Station_id
    unusuable_basins = [str(row['Station_id']) for _, row in unusuable_basins_df.iterrows()]
    
    
    return set(unusuable_basins)


if __name__ == "__main__":
    pass