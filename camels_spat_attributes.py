import os
import sys
import pandas as pd
from pathlib import Path

from utils.utils import load_util_data, get_unusable_basins

# Get the root directory of the project with Path
ROOT_DIR = Path(__file__).resolve().parents[0]

if __name__ == '__main__':
    
    print(ROOT_DIR)
    print(os.listdir(ROOT_DIR))

    # camels_spat2nh()
    data_dir, data_gen = load_util_data(str(ROOT_DIR))
    
    # Load Unusable basins
    unusuable_basins = get_unusable_basins(data_dir['data_dir_camels_spat_nh'], data_gen['camels_spat_unusable'])

    # # Load files in data_dir['data_dir_camels_spat'] / 'attributes'
    # attributes_dir = Path(data_dir['data_dir_camels_spat']) / 'attributes'

    # # Loas 1st file in attributes_dir
    # file = sorted(os.listdir(attributes_dir))[0]
    # df = pd.read_csv(attributes_dir / file)
    # print(df.head())

    att_file_dir = Path(data_dir['data_dir_camels_spat']) / 'camels_spat_attributes.csv'
    df = pd.read_csv(att_file_dir)
    print(df.head())

    # Copy attribute file to ROOT_DIR / 'data'
    att_file_dir_out = ROOT_DIR / 'data' / 'camels_spat_attributes.csv'
    df.to_csv(att_file_dir_out, index=False)
    