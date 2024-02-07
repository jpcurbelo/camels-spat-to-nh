import os
import sys
import xarray as xr
import pandas as pd
import yaml
import time

from functools import partial
import concurrent.futures

from utils.utils import reduceDataByDay

# Get the current working directory of the notebook
current_dir = os.getcwd()
# Add the parent directory of the notebook to the Python path
# root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
root_dir = os.path.abspath(current_dir)
sys.path.append(root_dir)


def load_util_data():
    
    # **Load Data**
    data_dir_path = os.path.join(root_dir, "utils", "data_dir.yml")
    with open(data_dir_path, 'r') as ymlfile:
        data_dir = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
    data_gen_path = os.path.join(root_dir, "utils", "data_general.yml")
    with open(data_gen_path, 'r') as ymlfile:
        data_gen = yaml.load(ymlfile, Loader=yaml.FullLoader)
               
    return data_dir, data_gen

def camels_spat2nh(data_dir, data_gen):

    # **Load Data**
    ## Dirs data
    data_dir_src = data_dir['data_dir_camels_spat']
    data_dir_out = data_dir['data_dir_camels_spat_nh']
    relative_path_forc = data_dir['relative_path_forcing']
    relative_path_targ = data_dir['relative_path_target']
    ## Basins data
    basin_data_path = os.path.join(data_dir_src, 'basin_data')
    list_basin_files = sorted(os.listdir(basin_data_path))
    
    ## General data
    countries = data_gen['countries']
    data_sources = data_gen['data_sources']
    
    # Filter folders by country name (3 first letters) - create a dictionary
    basin_data_path_dict = {}
    for country in countries:
        basin_data_path_dict[country] = [basin for basin in list_basin_files if basin[:3] == country]
    
    ## Process data for each basin and save to csv file
    for country in countries:
        # Create a folder for each country
        country_dir = os.path.join(data_dir_out, f'CAMELS_spat_{country}')
        if not os.path.exists(country_dir ):
            os.makedirs(country_dir)
        # else:
        #     print(f"Directory {country_dir} already exists")
            
        # for basin_f in basin_data_path_dict[country]:
        #     processBasinSave2CSV(basin_f, basin_data_path, country_dir, relative_path_forc, 
        #                          relative_path_targ, data_sources, data_gen)
        
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # Define a partial function with fixed non-iterable arguments       
            partial_process = partial(processBasinSave2CSV, basin_data_path=basin_data_path, 
                                country_dir=country_dir, relative_path_forc=relative_path_forc,
                                relative_path_targ=relative_path_targ, data_sources=data_sources, 
                                data_gen=data_gen)

            # Process each basin concurrently
            futures = [executor.submit(partial_process, basin_f) for basin_f in basin_data_path_dict[country]]

            # Wait for all tasks to complete
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()  # Get the result if needed
                    

def processBasinSave2CSV(basin_f, basin_data_path, country_dir, 
                         relative_path_forc, relative_path_targ, 
                         data_sources, data_gen):
            
    csv_file_name = os.path.join(country_dir, basin_f + '.csv')
    if os.path.exists(csv_file_name):
    # if 5>10:
        print(f"File {csv_file_name} already exists")
    
    else:
        print('\n', basin_f)
        ## Load input data
        df_src_dict = {}
        for src in data_sources:
            folder2load = os.path.join(basin_data_path, basin_f, relative_path_forc)
            eras_files = sorted([f for f in os.listdir(folder2load) if src in f])
            
            # Initialize an empty list to store the xarray datasets
            datasets = []
            # Iterate over the files and load each dataset
            for file2load in eras_files:
                
                # If not .temp file
                if '.tmp' not in file2load:
                    
                    sys.stdout.write(f'\r>> {file2load}')
                    sys.stdout.flush()
                    
                    basin_data = xr.open_dataset(os.path.join(folder2load, file2load))
                    datasets.append(basin_data)
                
            # Concatenate all datasets along the 'time' dimension
            concatenated_dataset = xr.concat(datasets, dim='time')
                
            # Reduce basin_data to daily values
            basin_data_reduced = reduceDataByDay(concatenated_dataset, data_gen['input_vars'], 
                                                 data_gen['sum_vars'])
            # Convert the reduced basin_data to a DataFrame, dropping the 'hru' dimension
            basin_data_df = basin_data_reduced.to_dataframe().droplevel('hru').reset_index()
            
            # Save to dict
            df_src_dict[src] = basin_data_df
            
        # Merge dataframes in the dictionary
        df_merged_inp = df_src_dict[data_sources[0]].merge(df_src_dict[data_sources[1]], on='time')
        
        # Rename time by date
        df_merged_inp.rename(columns={'time': 'date'}, inplace=True)
        
        ## Load target data
        target_data = xr.open_dataset(os.path.join(basin_data_path, basin_f, relative_path_targ, 
                                                   f'{basin_f}_daily_flow_observations.nc'))
        
        # Subset by data_gen['target_vars']
        target_data = target_data[data_gen['target_vars']]
        # Convert to DataFrame
        df_target = target_data.to_dataframe().reset_index()
        # Rename time by date
        df_target.rename(columns={'time': 'date'}, inplace=True)
        # Remove duplicates
        df_target = df_target.drop_duplicates(subset=['date'])
        
        # Merge input and target dataframes
        df_merged = df_merged_inp.merge(df_target, on='date')
        
        # Save to file
        df_merged.to_csv(os.path.join(country_dir, basin_f + '.csv'), index=False)


if __name__ == '__main__':
    

    # camels_spat2nh()
    data_dir, data_gen = load_util_data()
    
    # # ## Let's profile the loop
    # # start_time = time.time()
    # # camels_spat2nh_parallel(data_dir, data_gen)
    # # ## End of process
    # # print('\n', f"--- {(time.time() - start_time):.2f} seconds ---")
    
    ## Let's profile the loop
    start_time = time.time()
    camels_spat2nh(data_dir, data_gen)
    ## End of process
    print('\n', f"--- {(time.time() - start_time):.2f} seconds ---")
    
