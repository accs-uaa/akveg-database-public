# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2022 ABR Various Soil Metrics Data
# Author: Amanda Droghini
# Last Updated: 2024-11-26
# Usage: Must be executed in an ArcGIS Pro Python 3.11+ distribution.
# Description: "Format 2022 ABR Various Soil Metrics Data" prepares soil data recorded during vegetation surveys for ingestion in the AKVEG database. The script appends unique site visit identifiers, corrects erroneous values, parses information from notes field, replaces empty observations with appropriate null values, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import os
import pandas as pd
import numpy as np

# Define root directory
drive = 'C:/'
root_folder = os.path.join(drive, 'ACCS_Work')

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation',
                              'AKVEG_Database')
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '28_abr_various_2022')
source_folder = os.path.join(plot_folder, 'source')
schema_folder = os.path.join(project_folder, 'Data')

# Define input files
site_visit_input = os.path.join(plot_folder, '03_sitevisit_abrvarious2022.csv')
soil_metrics_input = os.path.join(source_folder, 'tnawrocki_deliverable_two_els.txt')
template_input = os.path.join(schema_folder, 'Data_Entry', '13_soil_metrics.xlsx')

# Define output files
soil_metrics_output = os.path.join(plot_folder, '13_soilmetrics_abrvarious2022.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input)
soil_metrics_original = pd.read_csv(soil_metrics_input, delimiter='|')
template = pd.read_excel(template_input)

# Drop unnecessary columns
soil_metrics = soil_metrics_original[['plot_id', 'site_ph_calc', 'site_ec_us_calc',
                                      'site_chemistry_calc', 'soil_sample_method',
                                      'microtopography', 'soil_dom_mineral_40cm',
                                      'soil_obs_maximum_depth_cm',
                                      'env_field_note', 'env_field_start_ts']]

# Format site code
## Drop year suffix
soil_metrics = soil_metrics.assign(join_key=np.where(soil_metrics['plot_id'].str.contains('_20+\d{2}$|-veg$',
                                                                                          regex=True),
                                                     soil_metrics['plot_id'].str.replace('_20+\d{2}$|-veg', '',
                                                                                         regex=True),
                                                     soil_metrics['plot_id']))

# SUWA sites require additional formatting: append 2-digit year to existing plot_id
soil_metrics = soil_metrics.assign(survey_year=soil_metrics['env_field_start_ts'].str[2:4])
soil_metrics = soil_metrics.assign(join_key=np.where(soil_metrics['join_key'].str.startswith('SUWA'),
                                                                        soil_metrics['join_key'].str.cat(soil_metrics['survey_year'], sep=''), soil_metrics['join_key']))

# Obtain site visit code
site_visit = site_visit_original[['site_code', 'site_visit_id']]
soil_metrics = soil_metrics.merge(site_visit, how='right', left_on='join_key', right_on='site_code')

## Ensure all sites in site_visit table are included in soil_metrics table
soil_metrics['site_visit_id'].isna().sum()
soil_metrics['plot_id'].isna().sum()
site_visit['site_visit_id'].equals(soil_metrics['site_visit_id'])

# Standardize null values
soil_metrics.isna().sum()

## Replace no data fields with appropriate null values
soil_metrics.replace(['No Data', 'Not Assessed', 'Unknown', 'Not Determined', 'not applicable', 'soils not sampled', -998], np.nan, inplace=True)

null_values = {"soil_sample_method": 'NULL',
               "microtopography": 'NULL',
               "soil_dom_mineral_40cm": 'NULL',
               "ssoil_obs_maximum_depth_cm": -999}
soil_metrics = soil_metrics.fillna(value=null_values)

# Format conductivity values
## High EC values (>10,000) are associated with brackish or saline sites
soil_metrics['site_ec_us_calc'].describe()

# Rename column
soil_metrics.rename(columns={'site_ec_us_calc':'conductivity_mus'}, inplace=True)

## Round to nearest decimal point
soil_metrics['conductivity_mus'] = soil_metrics['conductivity_mus'].round(decimals=2)

# Format pH values
## Ensure that range of values is between 0 and 14
soil_metrics[soil_metrics['site_ph_calc'] > -999].site_ph_calc.describe()

## Rename column
soil_metrics.rename(columns={'site_ph_calc':'ph'}, inplace=True)

## Round to nearest decimal point
soil_metrics['ph'] = soil_metrics['ph'].round(decimals=1)

# Format water measurement
soil_metrics['soil_sample_method'].value_counts()
## Assumed that 'Water' sites indicate that pH and EC values were taken from the water; most of the sites have some version of 'no data' listed for the soil sample method, except for 6 which list 'surface'
soil_metrics['water_measurement'] = np.where((soil_metrics['soil_dom_mineral_40cm'] == 'Water') &
                                             (soil_metrics['conductivity_mus'] > -999)
                                             & (soil_metrics['ph'] > -999),
                                             'TRUE',
                                             'FALSE')

# Verify that all values are boolean
soil_metrics['water_measurement'].isna().sum()
soil_metrics['water_measurement'].value_counts()

# Format measure depth
soil_metrics['soil_sample_method'].value_counts()

## Information was not explicitly recorded. Use soil sample method and depth of active layer to make an educated guess
conditions = [(soil_metrics['ph'] == -999) &
              (soil_metrics['conductivity_mus'] == -999),
              soil_metrics['water_measurement'] == 'TRUE',
              (soil_metrics['water_measurement'] == 'FALSE') &
              (soil_metrics['soil_sample_method'] == 'surface'),
              soil_metrics['soil_obs_maximum_depth_cm'] == -999,
              abs(soil_metrics['soil_obs_maximum_depth_cm']) >= 50,
              abs(soil_metrics['soil_obs_maximum_depth_cm']) < 50
              ]
choices = [-999, 0, 10, -999, 50, abs(soil_metrics['soil_obs_maximum_depth_cm'])]

soil_metrics['measure_depth_cm'] = np.select(conditions, choices, default=-999)

## Ensure values are reasonable (do not exceed 50 cm)
soil_metrics[soil_metrics['measure_depth_cm']>-999].measure_depth_cm.describe()

# Format soil temperature data
## Information is largely missing, but is occasionally listed in env_field_note column. Parse out those data where it is easy to do so. Unfortunately, cannot extract temperatures in Celsius because regex picks up on soil horizon data as well
soil_metrics['temperature_string'] = soil_metrics['env_field_note'].str.extract(r'(\d+F\b)', expand=False)
soil_metrics['temperature_degrees'] = soil_metrics['temperature_string'].str.extract(r'(\d+)', expand = False)

## Site SUWA_T31_0713_20130622 should be -2F, not 2F
soil_metrics.loc[soil_metrics['site_visit_id'] == 'SUWA_T31_0713_20130622', 'temperature_degrees'] = -2

## Ensure change occurred
soil_metrics[(soil_metrics['site_visit_id'] == 'SUWA_T31_0713_20130622')]

## Convert degrees column to numeric
soil_metrics['temperature_degrees'].fillna(-999, inplace=True)
soil_metrics['temperature_degrees'] = soil_metrics['temperature_degrees'].astype(str).astype(int)

## Convert values in Fahrenheit to Celsius
soil_metrics['temperature_deg_c'] = np.where(soil_metrics['temperature_degrees'] == -999,
                                             -999,
                                             ((soil_metrics['temperature_degrees'] - 32) * 5/9))

## Round to nearest decimal point
soil_metrics['temperature_deg_c'] = soil_metrics['temperature_deg_c'].round(decimals=1)

## Verify that values are within a reasonable range
soil_metrics[soil_metrics['temperature_deg_c'] > -999].temperature_deg_c.describe()

# Final formatting

## Rename site visit id to site visit code (schema 2.0)
soil_metrics_final = soil_metrics.rename(columns={"site_visit_id": "site_visit_code"})

## Drop all columns that aren't in the template and reorder them to match data entry template
soil_metrics_final = soil_metrics_final[template.columns]

# QA/QC
soil_metrics_final.describe()
soil_metrics_final.isna().sum()

# Export dataframe
soil_metrics_final.to_csv(soil_metrics_output, index=False, encoding='UTF-8')
