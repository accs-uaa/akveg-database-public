# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2019 ABR Arctic Refuge Soil Metrics Data
# Author: Amanda Droghini
# Last Updated: 2025-06-03
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2019 ABR Arctic Refuge Soil Metrics Data" prepares soil data for ingestion in the AKVEG
# database. The script appends unique site visit identifiers, re-classifies
# categorical values to match constraints in the AKVEG database, replaces empty observations with appropriate null
# values, and performs QC checks. The output is a CSV table that can be converted and included in a SQL INSERT
# statement.
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
                              'AKVEG_Database', 'Data')
plot_folder = os.path.join(project_folder, 'Data_Plots', '29_abr_arcticrefuge_2019')
source_folder = os.path.join(plot_folder, 'source')

# Define inputs
site_visit_input = os.path.join(plot_folder, '03_sitevisit_abrarcticrefuge2019.csv')
soils_input = os.path.join(source_folder, 'abr_anwr_ns_lc_els_deliverable.csv')
template_input = os.path.join(project_folder, 'Data_Entry', '13_soil_metrics.xlsx')

# Define output
soils_output = os.path.join(plot_folder, '13_soilmetrics_abrarcticrefuge2019.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input, usecols=['site_code', 'site_visit_code'])
soils_original = pd.read_csv(soils_input,
                             usecols=['plot_id',
                                      'soil_ph_at_10cm', 'soil_ec_us_at_10cm', 'soil_ph_at_30cm', 'water_ph',
                                      'water_ec_us'])
template = pd.read_excel(template_input)

# Obtain site visit code
soils_data = (soils_original.assign(join_key=soils_original['plot_id'].str.removesuffix('_2019'))
              .merge(site_visit_original, how='right', left_on='join_key', right_on='site_code'))

# Drop sites with no data
soils_data = soils_data.replace(to_replace=-999, value=np.nan)
soils_data = soils_data.dropna(how='all',
                               subset=['soil_ph_at_10cm', 'soil_ec_us_at_10cm', 'soil_ph_at_30cm', 'water_ph',
                                       'water_ec_us'])

## Ensure all sites in soils table exist in site visit table
## Not every site in the site visit table will have an entry in the soils table e.g., aerial sites for which no soils
# data were collected
print(soils_data['plot_id'].isna().sum())
print(soils_data['site_visit_code'].isna().sum())

# Standardize null values
print(soils_data.isna().sum())

## Replace no data fields with appropriate null values
soils_data = soils_data.fillna(value=-999)

# Convert dataframe to long format
soils_reshape = (soils_data.drop(columns=['plot_id', 'join_key', 'site_code'])
        .set_index(['site_visit_code'])
        .stack(future_stack=True)
        .reset_index())

# Parse measurement depth, water measurement, and measurement type from original column name
soils_reshape = (soils_reshape.assign(measure_type = np.where(soils_reshape['level_1'].str.contains('ph'), 'ph', 'ec'),
                   water_measurement = np.where(soils_reshape['level_1'].str.contains('water'), 'TRUE', 'FALSE'),
                   measure_depth_cm = np.where(soils_reshape['level_1'].str.contains('30cm'), 30, 10))
        .drop(columns=['level_1'])
        .rename(columns={0: "value"})
        .replace(to_replace=-999, value=np.nan))

# Drop rows with no data
soils_reshape = soils_reshape.dropna(subset=['value'])

# Widen the df slightly: place measurements for pH and EC into two different columns
soils_reshape = soils_reshape.set_index(['site_visit_code', 'measure_type','water_measurement', 'measure_depth_cm'])
## Need to ensure index results in unique combination of values
soils_reshape = soils_reshape.unstack('measure_type').reset_index()

# Final formatting

## Rename columns
soils_reshape.columns = ['site_visit_code', 'water_measurement', 'measure_depth_cm', 'conductivity_mus', 'ph']

## Populate values for missing columns
## Replace NaN with -999
soils_final = (soils_reshape.assign(temperature_deg_c = -999)
               .replace(to_replace=np.nan, value=-999))

## Reorder columns to match template
soils_final = soils_final[template.columns]

# QC data
print(soils_final.loc[soils_final.conductivity_mus > -999].describe())
print(soils_final.isna().sum())

# Export as CSV
soils_final.to_csv(soils_output, index=False, encoding='UTF-8')
