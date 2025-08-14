# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2019 ABR Arctic Refuge Soil Horizons Data
# Author: Amanda Droghini
# Last Updated: 2025-06-03
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2019 ABR Arctic Refuge Soil Horizons Data" prepares soil data for ingestion in the AKVEG
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
horizons_input = os.path.join(source_folder, 'abr_anwr_ns_lc_soil_horizon_deliverable.csv')
lookup_horizons_input = os.path.join(plot_folder, 'working', 'lookup_horizons_abrarcticrefuge2019.csv')
template_input = os.path.join(project_folder, 'Data_Entry', '14_soil_horizons.xlsx')
dictionary_input = os.path.join(project_folder, 'Tables_Metadata', 'database_dictionary.xlsx')

# Define output
horizons_output = os.path.join(plot_folder, '14_soilhorizons_abrarcticrefuge2019.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input, usecols=['site_code', 'site_visit_code'])
horizons_original = pd.read_csv(horizons_input,
                             usecols=['plot_id', 'horizon_number', 'horizon_code', 'top_depth_cm', 'bottom_depth_cm',
                                      'soil_texture', 'clay_percent', 'soil_color_hue', 'soil_color_value',
                                      'soil_color_chroma', 'soil_structure'])
lookup_horizons = pd.read_csv(lookup_horizons_input, na_values="")
template = pd.read_excel(template_input)
dictionary = pd.read_excel(dictionary_input)

# Define functions
## Checks whether horizon codes exist in data dictionary
def dictionary_check(data, data_columns, dictionary):
    for i in data_columns:
        temp = data.loc[:, i].unique()
        temp = temp[temp != "NULL"]

        if "suffix" in i:
            dictionary_values = dictionary[dictionary.field == 'soil_horizon_suffix'].data_attribute_id.to_numpy()
            temp_contains = np.isin(temp, dictionary_values, assume_unique=True)

            if False in temp_contains:
                print(i, "has non-matching values")
            else:
                print(i, "all values match")
        else:
            dictionary_values = dictionary[dictionary.field == 'soil_horizon_type'].data_attribute_id.to_numpy()
            temp_contains = np.isin(temp, dictionary_values, assume_unique=True)

            if False in temp_contains:
                print(i, "has non-matching values")
            else:
                print(i, "all values match")

# Obtain site visit code
horizons_data = (horizons_original.assign(join_key=horizons_original['plot_id'].str.removesuffix('_2019'))
              .merge(site_visit_original, how='left', left_on='join_key', right_on='site_code'))

## Ensure all sites in soils table exist in site visit table
## Not every site in the site visit table will have an entry in the soils table e.g., aerial sites for which no soils
# data were collected
print(horizons_data['plot_id'].isna().sum())
print(horizons_data['site_visit_code'].isna().sum())

# Standardize null values
print(horizons_data.isna().sum())

## Replace no data fields with appropriate null values
horizons_data = horizons_data.replace(['Unknown',
                                       'Not Available', 'Not Determined', 'Not Assessed', 'No Data', -997, -998],
np.nan)

null_values = {"soil_texture": 'NULL',
               "clay_percent": -999,
               "soil_color_hue": 'NULL',
               "soil_color_value": -999,
               "soil_color_chroma": -999,
               "soil_structure": 'NULL'
               }
horizons_data = horizons_data.fillna(value=null_values)

## Replace na in lookup tables
lookup_horizons = lookup_horizons.fillna("NULL")

# Format horizon thickness and depth extend
## Depth extend can only be TRUE when the lowest measured horizon is -999
print(horizons_data.bottom_depth_cm.describe())  ## No missing values; depth extend will always be False

horizons_data = (horizons_data.assign(thickness_cm=horizons_data.bottom_depth_cm-horizons_data.top_depth_cm,
                                      depth_extend='FALSE')
                 .rename(columns={"top_depth_cm": "depth_upper",
                                   "bottom_depth_cm": "depth_lower",
                                  "horizon_number": "horizon_order"})
        .sort_values(by=['site_visit_code', 'horizon_order'])
        )

print(horizons_data.thickness_cm.describe())
print(horizons_data.depth_extend.unique())

# Format primary horizon and horizon suffixes
## Use lookup table
horizons_data = pd.merge(horizons_data, lookup_horizons,
                        how='left', on='horizon_code')

## Ensure that all values are included in the data dictionary
values_check = ["horizon_primary", 'horizon_secondary', 'horizon_suffix_1',
                'horizon_suffix_2', 'horizon_suffix_3', 'horizon_suffix_4']

dictionary_check(horizons_data, values_check, dictionary)

# Format texture
print(horizons_data.soil_texture.value_counts())

# Correct values with appropriate constrained values
conditions = [horizons_data.soil_texture.str.contains("(^loamy).*?(sand)", regex=True),
              (horizons_data.soil_texture.str.startswith('coarse sand')) |
              (horizons_data.soil_texture.str.startswith('very fine sand'))]

choices = ['loamy sand', 'sand']

horizons_data = horizons_data.assign(texture_name = np.select(conditions, choices, default=horizons_data.soil_texture))

## Convert values to shorthand
## Values that do not have a match in the database dictionary are converted to NaN

dictionary_values = dictionary[dictionary.field == 'soil_texture'].filter(items=['data_attribute_id', 'data_attribute'])

horizons_data = horizons_data.merge(dictionary_values, how='left', left_on='texture_name', right_on='data_attribute')

horizons_data = horizons_data.rename(columns={"data_attribute_id": "texture"})
horizons_data.texture = horizons_data.texture.replace(to_replace=np.nan, value='NULL')

print(horizons_data.texture.value_counts())

# Format soil structure
print(horizons_data.soil_structure.value_counts())

## Correct values to match database dictionary
horizons_data = horizons_data.assign(structure=np.where(horizons_data.soil_structure == 'single grained',
                                                          'single grain', horizons_data.soil_structure))

# Format soil hue
print(horizons_data.soil_color_hue.value_counts())

## Convert values to uppercase
## Replace value that doesn't exist in the database dictionary with NULL
horizons_data = horizons_data.assign(matrix_hue = np.where(horizons_data.soil_color_hue == 'Variegated', 'NULL',
                                                        horizons_data.soil_color_hue.str.upper()))

print(horizons_data.matrix_hue.value_counts())

# Format soil value
print(horizons_data.soil_color_value.dtype)
print(horizons_data.soil_color_value.value_counts())

## Convert to numeric
horizons_data = horizons_data.assign(matrix_value=pd.to_numeric(horizons_data.soil_color_value))

horizons_data.loc[horizons_data.matrix_value > -999].matrix_value.describe()  ## Ensure
# non-missing values are
# between 0 and 10

# Format soil chroma
print(horizons_data.soil_color_chroma.dtype)
print(horizons_data.soil_color_chroma.value_counts())

## Explore entries for which soil hue is N; chroma should be -1
horizons_data.loc[horizons_data.matrix_hue == 'N'].filter(items=['soil_color_chroma'])

horizons_data = horizons_data.assign(matrix_chroma=np.where(horizons_data.matrix_hue == 'N', -1,
                                                            pd.to_numeric(horizons_data.soil_color_chroma)))

print(horizons_data.loc[horizons_data.matrix_chroma > -999].matrix_chroma.describe())  ## Ensure
# non-missing values are between -1 and 8

# Format clay percent
print(horizons_data.loc[horizons_data.clay_percent > -999].clay_percent.describe())  ## Ensure non-missing values are
# between 0 and 100

## No changes to be made

# Format for export

# Populate remaining columns with appropriate null values
horizons_final = horizons_data.assign(total_coarse_fragment_percent=-999,
                                      gravel_percent=-999,
                                      cobble_percent=-999,
                                      stone_percent=-999,
                                      boulder_percent=-999,
                                      matrix_value=-999,
                                      matrix_chroma=-999,
                                      nonmatrix_feature ='NULL',
                                      nonmatrix_hue='NULL',
                                      nonmatrix_value=-999,
                                      nonmatrix_chroma=-999)

horizons_final = horizons_final[template.columns]

# QC
temp = horizons_final.describe(include='all')

# Export as CSV
horizons_final.to_csv(horizons_output, index=False, encoding='UTF-8')
