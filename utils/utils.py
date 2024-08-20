import os
import yaml
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import math


def reduceDataByDay(dataset, set_vars, sum_vars, input_vars_repeated, forcing_src):
    '''
    Reduce the input dataset to daily frequency
    Args:
        dataset: xarray.Dataset, input dataset
        set_vars: list, variables to be averaged
        sum_vars: list, variables to be summed
        input_vars_repeated: list, variables that appear repeatedly
    Returns:
        daily_data: xarray.Dataset, dataset with daily frequency
    '''

    # Convert data to daily frequency
    day_dates = pd.to_datetime(dataset.coords["time"].values).normalize()
    day_dates = xr.DataArray(day_dates, name="time", dims="time")

    # Group by day and apply appropriate reduction method for each variable
    daily_data = xr.Dataset()

    # Calculate the difference between consecutive time points
    time_diff = dataset['time'].diff(dim='time')
    # Convert the differences to a pandas timedelta for easier analysis
    time_diff_timedelta = time_diff.to_pandas()
    # Get the most common frequency
    inferred_frequency = time_diff_timedelta.mode()[0]

    for variable in dataset.data_vars:

        if variable in input_vars_repeated:
            var = f'{variable}_{forcing_src}'
        else:
            var = variable

        # Check if the frequency is daily - daymet
        if inferred_frequency == pd.Timedelta(days=1) and variable in variable in set_vars:
            # Do not aggregate
            daily_data[var] = dataset[variable]
        else:
            if variable in sum_vars:
                # print('sum', variable)
                daily_data[var] = dataset[variable].groupby(day_dates).sum(dim="time")
            elif variable in set_vars:
                # print('mean', variable)
                daily_data[var] = dataset[variable].groupby(day_dates).mean(dim="time")
        
        # Add max and min temperature
        if variable == 'tmean':
            daily_data[f'{var}_max'] = dataset[variable].groupby(day_dates).max(dim="time")
            daily_data[f'{var}_min'] = dataset[variable].groupby(day_dates).min(dim="time")
        elif variable == 't':
            daily_data[f'{var}_max_{forcing_src}'] = dataset[variable].groupby(day_dates).max(dim="time")
            daily_data[f'{var}_min_{forcing_src}'] = dataset[variable].groupby(day_dates).min(dim="time")

    # print('daily_data', daily_data)
    # # Print Data variables
    # # See if any data_vars are not in set_vars or sum_vars
    # for var in daily_data.data_vars:
    #     print(var)
    #     if var not in set_vars:
    #         print('Variable not in daily_data:', var)
    # aux = input('Enter to continue')
            
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

    # Filter by 'No discharge values available ...' in Reason column
    unusuable_basins_df = unusuable_basins_df[unusuable_basins_df['Reason'].str.contains('No discharge values available', case=False)]

    # Station_id
    unusuable_basins = [str(row['Station_id']) for _, row in unusuable_basins_df.iterrows()]
    
    return set(unusuable_basins)

# Function to calculate statistics for time frames
def calculate_time_stats(country_dir):
    '''
    Calculate start dates, end dates, and total time lengths for each basin
    Args:
        country_dir: str, path to country directory
    Returns:
        start_dates: list, start dates for each basin
        end_dates: list, end dates for each basin
        total_time_lengths: list, total time lengths for each basin
    '''
    files = os.listdir(country_dir)
    basin_ids = []
    start_dates = []
    end_dates = []
    total_time_lengths = []
    for file in files:
        basin_ids.append(file.split('_')[-1].split('.')[0])
        df = pd.read_csv(os.path.join(country_dir, file))
        df['date'] = pd.to_datetime(df['date'])  # Convert 'date' column to datetime
        start_dates.append(df['date'].min())
        end_dates.append(df['date'].max())
        total_time_lengths.append(round((df['date'].max() - df['date'].min()).days / 365, 1))  # Calculate total length in years and round to 1 decimal place

    return basin_ids, start_dates, end_dates, total_time_lengths

# Function to plot histograms for start dates, end dates, and total time lengths
def plot_time_statistics(time_data, labels, title, xlabel):
    '''
    Plot histograms for start dates, end dates, and total time lengths
    Args:
        time_data: list, list of time data for each country
        labels: list, labels for each country
        title: str, title for the plot
        xlabel: str, label for x-axis
    '''
    
    plt.figure(figsize=(8, 6))
    for i, data in enumerate(time_data):
        plt.hist(data, bins=20, alpha=0.5, label=labels[i])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel('Frequency')
    plt.legend()
    # plt.grid(True)
    plt.show()

# Function to calculate statistics and plot histograms
def calculate_and_plot_time_statistics(basins_dir, countries):
    '''
    Calculate statistics and plot histograms for start dates, end dates, and total time lengths
    Args:
        basins_dir: str, path to basins directory
        countries: list, list of countries
    Returns:
        time_stats: dict, dictionary containing start dates, end dates, and total time lengths for each country
    '''
    
    # Dictionary to store results
    time_stats = {}
    
    # Iterate over countries
    for country in countries:
        country_dir = os.path.join(basins_dir, f'CAMELS_spat_{country}')
        basin_ids, start_dates, end_dates, total_time_lengths = calculate_time_stats(country_dir)
        time_stats[country] = {'Station_id': basin_ids, 'Start_date': start_dates, 'End_date': end_dates, 'Total_years': total_time_lengths}
    
    # Extract data for plotting
    start_dates_data = [time_stats[country]['Start_date'] for country in countries]
    end_dates_data = [time_stats[country]['End_date'] for country in countries]
    total_time_lengths_data = [time_stats[country]['Total_years'] for country in countries]

    # Plot histograms for start dates
    plot_time_statistics(start_dates_data, countries, 'Start Dates', 'Year')

    # Plot histograms for end dates
    plot_time_statistics(end_dates_data, countries, 'End Dates', 'Year')

    # Plot histograms for total time lengths (in years)
    plot_time_statistics(total_time_lengths_data, countries, 'Total Time Lengths (Years)', 'Number of years)')
    
    # Save time_stats to a csv file for each country (joined)
    all_time_stats_df = pd.concat([pd.DataFrame(time_stats[country]).assign(Country=country) for country in countries], ignore_index=True)
    all_time_stats_df = all_time_stats_df.sort_values(by=['Country', 'Station_id'])  # Sort by country and then by Station_id
    # Reorder columns with 'Country' as the first column
    all_time_stats_df = all_time_stats_df[['Country'] + [col for col in all_time_stats_df.columns if col != 'Country']]
    # Add 'Start_decade' and 'End_decade' columns
    all_time_stats_df['Start_decade'] = all_time_stats_df['Start_date'].apply(lambda x: math.floor(x.year / 10) * 10)
    all_time_stats_df['End_decade'] = all_time_stats_df['End_date'].apply(lambda x: math.floor(x.year / 10) * 10)
    # Save to CSV with the specified file name
    all_time_stats_df.to_csv(os.path.join(basins_dir, f'camels_spat_{len(all_time_stats_df)}_dates_stats.csv'), index=False)
    
    return time_stats


if __name__ == "__main__":
    pass