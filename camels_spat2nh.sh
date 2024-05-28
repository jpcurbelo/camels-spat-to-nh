#!/bin/bash
 
#SBATCH --account=hpc_c_giws_spiteri
#SBATCH --job-name=camels_spat2nh
#SBATCH --time=2-00:00:00
#SBATCH --ntasks=1              
#SBATCH --cpus-per-task=32         
#SBATCH --mem=64G
#SBATCH --output=camels_spat2nh-%j.out
 
module load python/3.11.5

# Path to your virtual environment's activation script
source venv-camelsspat/bin/activate
 
python3 camels_spat2nh.py
