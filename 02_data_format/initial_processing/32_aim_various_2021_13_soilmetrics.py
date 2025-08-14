# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Soil Metrics for AIM Various 2021 Data
# Author: Amanda Droghini
# Last Updated: 2025-04-03
# Usage: Must be executed in a Python 3.11+ distribution.
# Description: "Format Soil Metrics for AIM Various 2021 Data" uses data from soils surveys to extract measurements for pH, conductivity, and temperature stored in two separate tables. The script appends unique site visit identifiers, replaces missing values with appropriate null values, assigns temperature values to appropriate soil horizon, performs QA/QC checks to ensure values are within a reasonable range, and enforces formatting to match the AKVEG template. The script also merges new data with an existing, internal dataset of GMT-2 sites. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import os
import pandas as pd
import numpy as np

# Define root directory
drive = 'C:/'
root_folder = os.path.join(drive, 'ACCS_Work')

# Define folder structure
project_folder = os.path.join(root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation',
                              'AKVEG_Database')
schema_folder = os.path.join(project_folder, 'Data')
plot_folder = os.path.join(schema_folder, 'Data_Plots', '32_aim_various_2021')
source_folder = os.path.join(plot_folder, 'source', 'blm_aim_seward_pen_2021_soils_data')

# Define inputs
site_visit_input = os.path.join(plot_folder, '03_sitevisit_aimvarious2021.csv')
soil_metrics_input = os.path.join(source_folder, 'soil_horizon_deliverable.csv')
soil_temperature_input = os.path.join(source_folder, 'soil_deliverable.csv')
gmt2_soil_metrics_input = os.path.join(plot_folder, 'archive', '13_soilmetrics_gmt22021.csv')
template_input = os.path.join(schema_folder, 'Data_Entry', '13_soil_metrics.xlsx')
dictionary_input = os.path.join(schema_folder, 'Tables_Metadata', 'database_dictionary.xlsx')

# Define output
soil_metrics_output = os.path.join(plot_folder, '13_soilmetrics_aimvarious2021.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input)
soil_metrics_original = pd.read_csv(soil_metrics_input)
soil_temperature_original = pd.read_csv(soil_temperature_input)
gmt2_soil_metrics_original = pd.read_csv(gmt2_soil_metrics_input)
template = pd.read_excel(template_input)
dictionary = pd.read_excel(dictionary_input)

# Format soil temperature data
soil_temperature = (soil_temperature_original.assign(site_code = soil_temperature_original['plot_id'].str.upper())
                    .filter(items=["site_code", "soil_temperature_c", "soil_temperature_depth_cm"])
                    .dropna(axis=0, subset=['soil_temperature_c'])
                    .sort_values(by=['site_code']))

# Restrict site visit df to only relevant projects
project_list = ['aim_kobuknortheast_2021', 'aim_kobukwest_2021']
site_visit = site_visit_original[site_visit_original["project_code"].isin(project_list)]
site_visit = site_visit.filter(items=['site_code', 'site_visit_id'])

# Format df that contains ph & ec values
# Calculate measurement depth by taking the mean of the top and bottom depth measurements
# Drop sites for which both ph & ec are null (n=1)
# Convert all plot ids to uppercase; join with site visit to obtain site visit codes
soil_metrics = (soil_metrics_original.assign(site_code = soil_metrics_original['plot_id'].str.upper(),
                                             water_measurement=False,
                                             measure_depth_cm=soil_metrics_original[['top_depth_cm', 'bottom_depth_cm']].mean(axis=1))
                .dropna(axis=0, how='all', subset=['horizon_ph', 'horizon_ec_us'])
                .merge(site_visit, how='left', left_on='site_code', right_on='site_code')
                .rename(columns={"site_visit_id":"site_visit_code",
                                 "horizon_ph": "ph",
                                 "horizon_ec_us": "conductivity_mus"})
                .sort_values(by=['site_visit_code', 'measure_depth_cm'])
                .merge(soil_temperature, how='left', left_on='site_code', right_on='site_code')
                .fillna(value={"soil_temperature_c": -999, "soil_temperature_depth_cm": -999}) # No temperature data for site KSNE-06
                )
# Keep temperature value for only the relevant horizon
# Use top depth and bottom depth to find which horizon the soil temperature value belongs to. If soil temperature depth is at horizon boundary, assign it to the top-most horizon
soil_metrics = soil_metrics.assign(soil_temperature_keep=np.where((soil_metrics['soil_temperature_depth_cm']>soil_metrics['top_depth_cm']) & (soil_metrics['soil_temperature_depth_cm']<=soil_metrics['bottom_depth_cm']),'True','False'))
soil_metrics['temperature_deg_c'] = np.where(soil_metrics['soil_temperature_keep']=='True',soil_metrics['soil_temperature_c'],-999)

# Drop all columns that aren't in the template and reorder them to match data entry template
soil_metrics = soil_metrics[template.columns]

# Ensure that none of the columns have null values
soil_metrics.isna().sum()

# Ensure that number of temperature values match number in original file
soil_temperature_original['soil_temperature_c'].count() == soil_metrics[soil_metrics['temperature_deg_c'] > -999].temperature_deg_c.count()

# Combine with GMT-2 data (previously formatted)
soil_metrics_final = pd.concat([soil_metrics, gmt2_soil_metrics_original])
soil_metrics_final = soil_metrics_final.sort_values(by='site_visit_code')
soil_metrics_final.shape[0] == soil_metrics.shape[0] + gmt2_soil_metrics_original.shape[0]

# Verify that values are within a reasonable range
soil_metrics_final[soil_metrics_final['ph'] > -999].ph.describe() # Very alkaline values belong to the same sites - Assume OK
soil_metrics_final[soil_metrics_final['conductivity_mus'] > -999].conductivity_mus.describe()
soil_metrics_final[soil_metrics_final['temperature_deg_c'] > -999].temperature_deg_c.describe()
soil_metrics_final.measure_depth_cm.describe()
soil_metrics_final.water_measurement.value_counts()
soil_metrics_final.isna().sum()

# Export dataframe
soil_metrics_final.to_csv(soil_metrics_output, index=False, encoding='UTF-8')
