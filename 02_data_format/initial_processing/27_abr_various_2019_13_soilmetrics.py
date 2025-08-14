# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2019 ABR Various Soil Metrics Data
# Author: Amanda Droghini
# Last Updated: 2024-10-02
# Usage: Must be executed in an ArcGIS Pro Python 3.11+ distribution.
# Description: "Format 2019 ABR Various Soil Metrics Data" prepares soil data recorded during vegetation surveys for ingestion in the AKVEG database. The script appends unique site visit identifiers, corrects erroneous values, parses information from notes field, replaces empty observations with appropriate null values, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = os.path.join(project_folder, 'Data_Plots', '27_abr_various_2019')
source_folder = os.path.join(plot_folder, 'source')

# Define input files
site_visit_input = os.path.join(plot_folder, '03_sitevisit_abrvarious2019.csv')
soil_metrics_input = os.path.join(source_folder, 'deliverable_tnawrocki_els.xlsx')
template_input = os.path.join(project_folder, 'Data_Entry', '13_soil_metrics.xlsx')

# Define output files
soil_metrics_output = os.path.join(plot_folder, '13_soilmetrics_abrvarious2019.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input)
soil_metrics_original = pd.read_excel(soil_metrics_input)
template = pd.read_excel(template_input)

# Drop unnecessary columns
soil_metrics = soil_metrics_original[['plot_id', 'site_ph_calc', 'site_ec_us_calc',
                                      'site_chemistry_calc', 'soil_sample_method',
                                      'microtopography', 'soil_dom_mineral_40cm', 'soil_restrict_depth_probe_cm', 'env_field_note']]

# Format site code
soil_metrics = soil_metrics.assign(site_code = soil_metrics['plot_id'].str.upper())
soil_metrics = soil_metrics.assign(site_code = soil_metrics['site_code'].str.replace('-', '_'))

# Obtain site visit code
## Use right join to drop 27 sites with missing or erroneous data
site_visit = site_visit_original[['site_code', 'site_visit_code']]
soil_metrics = pd.merge(soil_metrics, site_visit, how='right', left_on='site_code', right_on='site_code')

## Ensure all site codes have a site visit id
soil_metrics['site_visit_code'].isna().sum()
soil_metrics.shape[0] == site_visit_original.shape[0]

# Replace NA with appropriate null values
soil_metrics.isna().sum()
null_values = {"soil_sample_method": 'NULL', "microtopography": 'NULL', "soil_dom_mineral_40cm": 'NULL', "soil_restrict_depth_probe_cm": -999}
soil_metrics = soil_metrics.fillna(value=null_values)

# Format conductivity values
soil_metrics['site_ec_us_calc'].describe()

## Convert suspicious value to -999. There are some other values that have very high EC but that are associated with brackish or saline sites - Assume that makes sense
soil_metrics['conductivity_mus'] = np.where(soil_metrics['site_ec_us_calc'] == 999999, -999, soil_metrics['site_ec_us_calc'])

## Ensure change was made
soil_metrics['conductivity_mus'].describe()

# Format pH values
soil_metrics[soil_metrics['site_ph_calc'] > -999].site_ph_calc.describe()

## Replace pH values of 0 with -999 (n=5). Likely represent null values, as 'no data' is listed under the 'site_chemistry_calc' column

soil_metrics['ph'] = np.where(soil_metrics['site_ph_calc'] == 0, -999, soil_metrics['site_ph_calc'])

## Verify that zero values no longer exist and that range of non-null values is reasonable
soil_metrics[soil_metrics['ph'] > -999].ph.describe()

# Format water measurement
## Unsure whether to include 'No Data' and 'Not Assessed' plots in this re-classification. Some of the environmental
# notes suggest that would be appropriate, but may not apply to all plots.
soil_metrics['water_measurement'] = np.where((soil_metrics['soil_sample_method'].str.contains('surface|metal probe|No Data|Not Assessed', regex=True)) &
                                             (soil_metrics['microtopography'] == 'Water') & (soil_metrics['site_ec_us_calc'] > -999)
                                             & (soil_metrics['site_ph_calc'] > -999), 'TRUE', 'FALSE')

# Manually consulted environment notes column for remaining 'surface' plots. Change 2 sites to water_measurement = TRUE
# based on those notes.
soil_metrics.loc[soil_metrics["plot_id"] == "NOAT_T42_25", "water_measurement"] = 'TRUE'
soil_metrics.loc[soil_metrics["plot_id"] == "ania_t08-03", "water_measurement"] = 'TRUE'

# Verify that all values are boolean
soil_metrics['water_measurement'].isna().sum()
soil_metrics['water_measurement'].value_counts()

# Format measure depth
## Information was not explicitly recorded. Use soil sample method to make an educated guess
soil_metrics['soil_sample_method'].value_counts()

## Attempted to extract as much information as possible from env_field_note column, but gave up for the sake of efficiency
conditions = [soil_metrics['plot_id'].str.contains('katm_aardvark_03|katm_anaconda_01|katm_t18-01|katm_t37-03|katm_t44-07|KEFJ_Tqc_03|katm_t13-11|katm_t26-01|KEFJ_Tpb2_05', regex=True),
              soil_metrics['plot_id'] == 'KEFJ_Tpb2_04',
              soil_metrics['plot_id'].str.contains('KEFJ_Tpb2_06|KEFJ_Tcb2_04|KEFJ_Tlg_02', regex=True),
              soil_metrics['plot_id'] == 'katm_t26-02',
              soil_metrics['plot_id'] == 'KEFJ_Tcb2_02',
              (soil_metrics['ph'] == -999) &
              (soil_metrics['conductivity_mus'] ==- 999),
              soil_metrics['water_measurement'] == 'TRUE',
              (soil_metrics['water_measurement'] == 'FALSE') &
              (soil_metrics['soil_sample_method'] == 'surface'),
              abs(soil_metrics['soil_restrict_depth_probe_cm']) >= 50,
              abs(soil_metrics['soil_restrict_depth_probe_cm']) < 50,
              soil_metrics['soil_sample_method'].str.contains('No Data|Not Assessed|NULL', regex=True)
              ]
choices = [40, 24, 35, 36, 30, -999, 0, 10, 50, abs(soil_metrics['soil_restrict_depth_probe_cm']), -999]

soil_metrics['measure_depth_cm'] = np.select(conditions, choices, default=-999)

# Format soil temperature data
## Information is largely missing, but is occasionally listed in env_field_note column. Parse out those data where it is easy to do so.
soil_metrics['temperature_string'] = soil_metrics['env_field_note'].str.extract(r'(\d+C|\d+F)', expand=False)
soil_metrics['temperature_units'] = soil_metrics['temperature_string'].str.extract(r'(C|F)', expand = False)
soil_metrics['temperature_degrees'] = soil_metrics['temperature_string'].str.extract(r'(\d+)', expand = False)

## Convert degrees column to numeric
soil_metrics['temperature_degrees'].fillna(-999, inplace=True)
soil_metrics['temperature_degrees'] = soil_metrics['temperature_degrees'].astype(str).astype(int)

## Convert values in Fahrenheit to Celsius
conditions = [soil_metrics['temperature_units'] == 'C',
              soil_metrics['temperature_units'] == 'F']
choices = [soil_metrics['temperature_degrees'], ((soil_metrics['temperature_degrees'] - 32) * 5/9)]
soil_metrics['temperature_deg_c'] = np.select(conditions, choices, default=-999)

## Round to nearest decimal point
soil_metrics['temperature_deg_c'] = soil_metrics['temperature_deg_c'].round(decimals=1)

## Verify that values are within a reasonable range
soil_metrics[soil_metrics['temperature_deg_c'] > -999].temperature_deg_c.describe()

# Final formatting
## Drop all columns that aren't in the template and reorder them to match data entry template
soil_metrics_final = soil_metrics[template.columns]

# QA/QC
soil_metrics_final.describe()
soil_metrics_final.isna().sum()

# Export dataframe
soil_metrics_final.to_csv(soil_metrics_output, index=False, encoding='UTF-8')
