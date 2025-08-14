# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Correct site and site visit codes
# Author: Timm Nawrocki
# Last Updated: 2024-08-07
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Correct site and site visit codes" corrects the site_code and site_visit_code fields of all tables in the ACCS Nelchina 2023 dataset except for the 02- Site and 03- Site Visit tables.
# ---------------------------------------------------------------------------

# Import packages
import os
import time
import pandas as pd
from akutils import *
import arcpy

# Set root directory
drive = 'D:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'Projects/VegetationEcology/AKVEG_Database')
data_folder = os.path.join(project_folder, 'Data/Data_Plots/36_accs_nelchina_2023')
akveg_folder = os.path.join(drive, root_folder, 'Projects/VegetationEcology/AKVEG_Map/Data')

# Define geodatabases
workspace_geodatabase = os.path.join(akveg_folder, 'AKVEG_Workspace.gdb')

# Define input datasets
vegetation_input = os.path.join(data_folder, 'working/05_vegetationcover_accsnelchina2023.csv')
abiotic_input = os.path.join(data_folder, 'working/06_abiotictopcover_accsnelchina2023.csv')
tussock_input = os.path.join(data_folder, 'working/07_wholetussockcover_accsnelchina2023.csv')
ground_input = os.path.join(data_folder, 'working/08_groundcover_accsnelchina2023.csv')
shrub_input = os.path.join(data_folder, 'working/11_shrubstructure_accsnelchina2023.csv')
env_input = os.path.join(data_folder, 'working/12_environment_accsnelchina2023.csv')
soil_input = os.path.join(data_folder, 'working/13_soilmetrics_accsnelchina2023.csv')

# Define output datasets
vegetation_output = os.path.join(data_folder, 'working/05_vegetationcover_accsnelchina2023_corrected.csv')
abiotic_output = os.path.join(data_folder, '06_abiotictopcover_accsnelchina2023.csv')
tussock_output = os.path.join(data_folder, '07_wholetussockcover_accsnelchina2023.csv')
ground_output = os.path.join(data_folder, '08_groundcover_accsnelchina2023.csv')
shrub_output = os.path.join(data_folder, 'working/11_shrubstructure_accsnelchina2023_corrected.csv')
env_output = os.path.join(data_folder, '12_environment_accsnelchina2023.csv')
soil_output = os.path.join(data_folder, '13_soilmetrics_accsnelchina2023.csv')

# Set overwrite option
arcpy.env.overwriteOutput = True

# Specify core usage
arcpy.env.parallelProcessingFactor = '0'

# Set workspace
arcpy.env.workspace = workspace_geodatabase

# Reformat site_visit_code in all tables
print('Reformat site_visit_code in all tables...')
iteration_start = time.time()
count = 1
input_list = [vegetation_input, abiotic_input, tussock_input,
              ground_input, shrub_input, env_input, soil_input]
output_list = [vegetation_output, abiotic_output, tussock_output,
               ground_output, shrub_output, env_output, soil_output]
# Process each input file
for input_file in input_list:
    print(f'\tReformatting table {count} of {len(input_list)}...')
    # Read input data
    input_data = pd.read_csv(input_file)
    # Correct site code if it exists
    if 'site_code' in input_data.columns:
        input_data['site_code'] = input_data['site_code'].str.replace('C2023', 'C')
    # Correct site visit code if it exists
    if 'site_visit_code' in input_data.columns:
        input_data['site_visit_code'] = input_data['site_visit_code'].str.replace('C2023', 'C')
    output_file = output_list[count-1]
    input_data.to_csv(output_file, index=False, encoding='UTF-8')
    count += 1
end_timing(iteration_start)