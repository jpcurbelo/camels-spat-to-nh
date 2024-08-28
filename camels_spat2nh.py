import os
import sys
import xarray as xr
import pandas as pd
import time
from functools import reduce

from functools import partial
import concurrent.futures

from utils.utils import reduceDataByDay, load_util_data, get_unusable_basins

# Get the current working directory of the notebook
current_dir = os.getcwd()
# Add the parent directory of the notebook to the Python path
# root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
ROOT_DIR = os.path.abspath(current_dir)
sys.path.append(ROOT_DIR)

MULTIPROCESSING = 0
MAX_WORKERS = int(os.environ.get('SLURM_CPUS_PER_TASK', 32))
ONLY_TESTING = 0

FILTER_BY_CYRIL = True

def camels_spat2nh(data_dir, data_gen, unusuable_basins):

    # **Load Data**
    ## Dirs data
    data_dir_src = data_dir['data_dir_camels_spat']
    data_dir_out = data_dir['data_dir_camels_spat_nh']
    relative_path_forc = data_dir['relative_path_forcing']
    relative_path_targ = data_dir['relative_path_target']
    ## Basins data
    basin_data_path = os.path.join(data_dir_src, 'basin_data')
    list_basin_files = sorted(os.listdir(basin_data_path))

    # Input data
    input_vars = data_gen['input_vars']
    # Get the input variables that appear repeatedly
    input_vars_repeated = set([var for var in input_vars if input_vars.count(var) > 1])

    # Drop if file already exists
    for basin_f in list_basin_files[:]:
        # Check if file exists
        csv_file_path = os.path.join(data_dir_out, f'CAMELS_spat_{basin_f[0:3]}', basin_f[4:] + '.csv')
        if os.path.exists(csv_file_path):
            print(f"File {csv_file_path} already exists")
            if basin_f[4:] in unusuable_basins:
                # Delete file
                os.remove(csv_file_path)

            # Remove from list
            list_basin_files.remove(basin_f)

    print('Basins to process:', len(list_basin_files))
       
    ## General data
    countries = data_gen['countries']
    data_sources = data_gen['data_sources']
    
    # Filter folders by country name (3 first letters) - create a dictionary
    basin_data_path_dict = {}
    for country in countries:
        basin_data_path_dict[country] = [basin for basin in list_basin_files if basin[:3] == country]

    # ## Do not delete unless you know what you are doing
    # # # Filtering by cyril basins
    # # if FILTER_BY_CYRIL:
    # #     cyril_list = get_cyril_basins()
    # # else:
    # #     cyril_list = None

    # # print('Cyril basins:', len(cyril_list))
    # # print(cyril_list[:5])
    # # print(cyril_list[-5:])

    # # return

    ## Process data for each basin and save to csv file
    for country in countries[:]:
        # Create a folder for each country
        # Check if only testing
        if ONLY_TESTING:
            country_dir = os.path.join(data_dir_out, f'CAMELS_spat_{country}_testing')
        else:
            country_dir = os.path.join(data_dir_out, f'CAMELS_spat_{country}_{len(data_sources)}sources')
            
        if not os.path.exists(country_dir ):
            os.makedirs(country_dir)
        
        if MULTIPROCESSING:

            print(f"Processing {country}...")
            with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Define a partial function with fixed non-iterable arguments       
                partial_process = partial(processBasinSave2CSV, basin_data_path=basin_data_path, 
                                    country_dir=country_dir, 
                                    relative_path_forc=relative_path_forc,
                                    relative_path_targ=relative_path_targ, 
                                    data_sources=data_sources, 
                                    data_gen=data_gen, 
                                    unusuable_basins=unusuable_basins,
                                    input_vars_repeated=input_vars_repeated)

                # Process each basin concurrently
                futures = [executor.submit(partial_process, basin_f) for basin_f in basin_data_path_dict[country]]

                # Wait for all tasks to complete and handle exceptions
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()  # Get the result if needed
                    except Exception as e:
                        print(f"Error processing {future}: {e}")

        else:
            for basin_f in basin_data_path_dict[country]:
                processBasinSave2CSV(basin_f, basin_data_path, country_dir, relative_path_forc, 
                                     relative_path_targ, data_sources, data_gen, unusuable_basins, input_vars_repeated)
                                  
def processBasinSave2CSV(basin_f, basin_data_path, country_dir, 
                         relative_path_forc, relative_path_targ, 
                         data_sources, data_gen, unusuable_basins,
                         input_vars_repeated,
                         cyril_list=None):

    print(f"Let's try {basin_f}...")
            
    basin_id = basin_f.split('_')[-1]
    csv_file_path = os.path.join(country_dir, basin_id + '.csv')

    print('csv_file_path', csv_file_path, os.path.exists(csv_file_path))

    if os.path.exists(csv_file_path):
        print(f"File {csv_file_path} already exists")
        if basin_f[4:] in unusuable_basins:
            # Delete file
            os.remove(csv_file_path)               
    elif basin_f[4:] in unusuable_basins:
        print(f"Skipping basin {basin_f} - unusable basin")
    else:
        print('\n', basin_f[:3], '->', basin_id)
        df_src_dict = {}
        for src in data_sources:

            folder2load = os.path.join(basin_data_path, basin_f, relative_path_forc)
            # print('folder2load', folder2load)
            eras_files = sorted([f for f in os.listdir(folder2load) if src in f])

            print(f'{src}_files', len(eras_files), '->', folder2load)
            
            # # Check if only testing
            # if ONLY_TESTING:
            #     continue

            # Check whether there are files to load
            if len(eras_files) == 0:
                continue
            
            # Initialize an empty list to store the xarray datasets
            datasets = []
            # Iterate over the files and load each dataset
            for file2load in eras_files:   ### [:5] for testing
                
                # If not .temp file
                if '.tmp' not in file2load:
                    
                    # sys.stdout.write(f'\r>> {file2load}')
                    # sys.stdout.flush()
                    
                    basin_data = xr.open_dataset(os.path.join(folder2load, file2load))
                    datasets.append(basin_data)
                
            # Concatenate all datasets along the 'time' dimension
            concatenated_dataset = xr.concat(datasets, dim='time')
                
            # Reduce basin_data to daily values
            basin_data_reduced = reduceDataByDay(concatenated_dataset, data_gen['input_vars'], 
                                                data_gen['sum_vars'], input_vars_repeated, src.lower())
            # Convert the reduced basin_data to a DataFrame, dropping the 'hru' dimension
            basin_data_df = basin_data_reduced.to_dataframe().droplevel('hru').reset_index()

            print('basin_data_df', basin_data_df.head())
           
            # Save to dict
            df_src_dict[src] = basin_data_df
                  
        
        # # Check if only testing
        # if ONLY_TESTING:
        #     return None

        print('basin', basin_f, '->', df_src_dict.keys())
        # Check if there are len(data_sources) data sources in df_src_dict.keys() (expected ERA5, EM_EARTH, daymet, and RDRS)

        # if len(df_src_dict.keys()) == len(data_sources):
        #     # Merge dataframes in the dictionary
        #     df_merged_inp = df_src_dict[data_sources[0]].merge(df_src_dict[data_sources[1]], on='time')
        # elif len(df_src_dict.keys()) == 1:
        #     df_merged_inp = df_src_dict[list(df_src_dict.keys())[0]]

        if len(df_src_dict.keys()) == len(data_sources):
            # Dynamically merge all dataframes in the dictionary based on the 'time' column using an outer join
            df_merged_inp = reduce(lambda left, right: pd.merge(left, right, on='time', how='outer'), 
                                [df_src_dict[src] for src in data_sources])
        elif len(df_src_dict.keys()) == 1:
            df_merged_inp = df_src_dict[list(df_src_dict.keys())[0]]
        else:
            raise ValueError("The number of data sources does not match the keys in the dictionary.")
                        
        # Rename time by date
        df_merged_inp.rename(columns={'time': 'date'}, inplace=True)

        # print('df_merged_inp', df_merged_inp.head())
        # # Print data_vars
        # for var in df_merged_inp.columns:
        #     print(var)
        #     if var not in data_gen['input_vars']:
        #         print('Variable not in inputs:', var)
        # aux = input('Enter to continue')
        
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

        # print('df_target', df_target.head())
        
        # Merge input and target dataframes
        df_merged = df_merged_inp.merge(df_target, on='date')


        # print('df_merged', df_merged.head())
        # # Print data_vars
        # for var in df_merged.columns:
        #     print(var)
        #     if var not in data_gen['input_vars']:
        #         print('Variable not in inputs:', var)
        # aux = input('Enter to continue')
        
        # Save to file
        # print("Saving to file...", os.path.join(country_dir, basin_id + '.csv'))
        # df_merged.to_csv(os.path.join(country_dir, basin_f[4:] + '.csv'), index=False)
        print("Saving to file...", csv_file_path)
        df_merged.to_csv(csv_file_path, index=False)

def get_cyril_basins():

    # Load cyril basins from data / liste_BV_CAMELS-spat_928.txt
    cyril_file = os.path.join(ROOT_DIR, 'data', 'liste_BV_CAMELS-spat_928.txt')
    with open(cyril_file, 'r') as f:
        cyril_list = f.readlines()
    # Remove 'XXX_' from the beginning of each line
    cyril_list = [line[4:].strip() for line in cyril_list]
    return cyril_list

if __name__ == '__main__':
    
    # Load data
    data_dir, data_gen = load_util_data(ROOT_DIR)
    
    # Load Unusable basins
    unusuable_basins = get_unusable_basins(data_dir['data_dir_camels_spat_nh'], data_gen['camels_spat_unusable'])

    print('Unusable basins:', len(unusuable_basins))
    print(unusuable_basins)

    ## Let's profile the loop
    start_time = time.time()
    camels_spat2nh(data_dir, data_gen, unusuable_basins)
    ## End of process
    print('\n', f"--- {(time.time() - start_time):.2f} seconds ---")
    
