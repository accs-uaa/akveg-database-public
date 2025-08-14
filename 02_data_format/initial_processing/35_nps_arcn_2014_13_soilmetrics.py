# ---------------------------------------------------------------------------
# Format Soil Metrics for NPS Arctic Network 2014 data
# Author: Amanda Droghini
# Last Updated: 2025-05-28
# Usage: Must be executed in an ArcGIS Pro Python 3.11+ distribution.
# Description: "Format Soil Metrics for NPS Arctic Network 2014 data" prepares soil data recorded during vegetation surveys for ingestion in the AKVEG database. The script appends unique site visit identifiers, replaces empty observations with appropriate null values, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import os
import pandas as pd

# Define directories
drive = 'C:/'
root_folder = os.path.join(drive, 'ACCS_Work')

# Define folder structure
project_folder = os.path.join(root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation',
                              'AKVEG_Database', 'Data')
plot_folder = os.path.join(project_folder, 'Data_Plots', '35_nps_arcn_2014')
source_folder = os.path.join(plot_folder, 'source')

# Define inputs
site_visit_input = os.path.join(plot_folder, '03_sitevisit_npsarcn2014.csv')
soil_metrics_input = os.path.join(source_folder, 'dbo_soilhorizon.csv')
template_input = os.path.join(project_folder, 'Data_Entry', '13_soil_metrics.xlsx')

# Define outputs
soil_metrics_output = os.path.join(plot_folder, '13_soilmetrics_npsarcn2014.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input, usecols=["site_code", "site_visit_code"])
soil_metrics_original = pd.read_csv(soil_metrics_input, usecols=["NODE", "PLOT", "UPDEP", "LOWDEP", "pH"])
template = pd.read_excel(template_input)

# Format site code
soil_metrics = soil_metrics_original.assign(plot_string=soil_metrics_original.PLOT.astype(str))
soil_metrics['site_code'] = soil_metrics.NODE.str.cat(soil_metrics.plot_string.str.zfill(2), sep="_")

# Obtain site visit code
## Use left join since not all sites have soils data
soil_metrics = pd.merge(soil_metrics, site_visit_original,
                        how='left', left_on='site_code', right_on='site_code')

## Ensure all site codes have a site visit id
soil_metrics['site_visit_code'].isna().sum()

# Explore null values
soil_metrics.isna().sum()
soil_metrics.pH.isna().sum()

## Drop rows for which ph is null, since there is no other measurement included in this table
soil_metrics = soil_metrics.dropna(subset=['pH'])

# Calculate measurement depth
## Take the mean of the top and bottom depth measurements
soil_metrics = soil_metrics.assign(measure_depth_cm=soil_metrics[['UPDEP', 'LOWDEP']].mean(axis=1))

# Populate remaining columns
soil_metrics = (soil_metrics.assign(water_measurement=False,
                                   conductivity_mus=-999,
                                   temperature_deg_c=-999
                                   )
                .rename(columns={"pH":"ph"})
                .round(1))

# Drop all columns that aren't in the template and reorder them to match data entry template
soil_metrics = soil_metrics[template.columns]

# Address duplicate entries
# If >1 entry exists for the same measurement depth, take the average pH value
soil_metrics = soil_metrics.groupby(['site_visit_code', 'water_measurement', 'measure_depth_cm']).mean().reset_index()

# Ensure that none of the columns have null values
print(soil_metrics.isna().sum())

# Verify that values are within a reasonable range
print(soil_metrics.describe())

# Export dataframe
soil_metrics.to_csv(soil_metrics_output, index=False, encoding='UTF-8')
