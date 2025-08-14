# ---------------------------------------------------------------------------
# Format Soil Horizons for NPS Arctic Network 2014 data
# Author: Amanda Droghini
# Last Updated: 2025-04-23
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Soil Horizons for NPS Arctic Network 2014 data" prepares soil data recorded during vegetation surveys for ingestion in the AKVEG database. The script appends unique site visit identifiers, uses lookup tables to translate values into ones that are included in the AKVEG database dictionary, replaces empty observations with appropriate null values, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import os
import numpy as np
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
soil_horizons_input = os.path.join(source_folder, 'dbo_soilhorizon.csv')
lookup_horizons_input = os.path.join(plot_folder, 'working', 'lookup_soil_horizons.csv')
lookup_texture_input = os.path.join(plot_folder, 'working', 'lookup_soil_texture.csv')
lookup_structure_input = os.path.join(plot_folder, 'working', 'lookup_soil_structure.csv')
template_input = os.path.join(project_folder, 'Data_Entry', '14_soil_horizons.xlsx')
dictionary_input = os.path.join(project_folder, 'Tables_Metadata', 'database_dictionary.xlsx')

# Define outputs
soil_horizons_output = os.path.join(plot_folder, '14_soilhorizons_npsarcn2014.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input, usecols=["site_code", "site_visit_code"])
soil_horizons_original = pd.read_csv(soil_horizons_input)
lookup_horizons = pd.read_csv(lookup_horizons_input, na_values="")
lookup_texture = pd.read_csv(lookup_texture_input, keep_default_na=False)
lookup_structure = pd.read_csv(lookup_structure_input, keep_default_na=False)
template = pd.read_excel(template_input)
dictionary = pd.read_excel(dictionary_input)

# Format site code
soil_horizons = soil_horizons_original.assign(plot_string=soil_horizons_original.PLOT.astype(str))
soil_horizons['site_code'] = soil_horizons.NODE.str.cat(soil_horizons.plot_string.str.zfill(2), sep="_")

# Obtain site visit code
## Use left join since not all sites have soils data
soil_horizons = pd.merge(soil_horizons, site_visit_original,
                        how='left', left_on='site_code', right_on='site_code')

## Ensure all site codes have a site visit id
soil_horizons['site_visit_code'].isna().sum()

# Replace missing values with appropriate null values
null_values = {"LOWDEP": -999, "TEXTURE": "NULL", "GR": -999, "CB": -999, "ST": -999,
               "PRIMOIST": "NULL",
               "PRISTRUC": "NULL", "pH": -999}
soil_horizons = soil_horizons.fillna(null_values)

## Replace na in lookup tables
lookup_horizons = lookup_horizons.fillna("NULL")

# Format horizon order
## One site has a bedrock horizon for which depth_lower is null (changed to -999)
soil_horizons = (soil_horizons.assign(thickness_cm=np.where(soil_horizons.LOWDEP > -999, soil_horizons.LOWDEP-soil_horizons.UPDEP, -999))
        .sort_values(by=['site_visit_code', 'UPDEP'], ignore_index=True)
        .assign(horizon_order=soil_horizons.groupby(by="site_visit_code").cumcount().add(1))
        .rename(columns={"UPDEP": "depth_upper", "LOWDEP": "depth_lower"})
        .sort_values(by=['site_visit_code', 'horizon_order'])
        )

# Format depth_extend
## The value can only be TRUE for the lowest measured horizon; all other horizons are FALSE.

## Find max horizon order for each plot
soil_horizons['max_horizon'] = soil_horizons.groupby('site_visit_code')['horizon_order'].transform('max')

soil_horizons = soil_horizons.assign(depth_extend = np.where((soil_horizons.horizon_order == soil_horizons.max_horizon)
                                                             & (soil_horizons.depth_lower == -999),
                                                             True,
                                                             False))

soil_horizons.depth_extend.value_counts() # Should only be one value with True

# Format primary horizon and horizon suffixes
## Use lookup table
soil_horizons = pd.merge(soil_horizons, lookup_horizons,
                        how='left', left_on='HORIZON', right_on='horizon')

## Ensure that all values are included in the data dictionary
values_check = ["horizon_primary", 'horizon_secondary', 'horizon_suffix_1',
                'horizon_suffix_2', 'horizon_suffix_3', 'horizon_suffix_4']

for i in values_check:
    temp = soil_horizons.loc[:, i].unique()
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

# Format texture
## Use lookup table
soil_horizons = pd.merge(soil_horizons, lookup_texture,
                        how='left', left_on='TEXTURE', right_on='TEXTURE')

## Correct weirdly formatted values
soil_horizons = soil_horizons.assign(texture = np.where(soil_horizons.texture.isna(), 'l', soil_horizons.texture))

## Ensure that all values are included in the data dictionary
dictionary_values = dictionary[dictionary.field == 'soil_texture'].data_attribute_id.to_numpy()
temp = soil_horizons.loc[:, "texture"].unique()
temp = temp[temp != "NULL"]
temp_contains = np.isin(temp, dictionary_values, assume_unique=True)

if False in temp_contains:
    print("texture has non-matching values")
else:
    print("texture all values match")

# Format coarse fragment percents
## Decided not to interpret any of the NA values as 0. If any of stone, gravel, or cobble is -999, cannot calculate total coarse fragments %
soil_horizons = (soil_horizons.assign(total_coarse_fragment_percent = np.where((soil_horizons.ST != -999) &
                                                                               (soil_horizons.GR != -999) &
                                                                      (soil_horizons.CB != -999), soil_horizons.ST + soil_horizons.GR + soil_horizons.CB,
                                                                               -999))
                 .rename(columns={"ST": "stone_percent",
                         "GR": "gravel_percent",
                         "CB": "cobble_percent"})
                 )

## Explore values
temp = soil_horizons[soil_horizons.total_coarse_fragment_percent < 0].filter(items=['total_coarse_fragment_percent', 'stone_percent', 'cobble_percent', 'gravel_percent']) ## Ensure that at least one of the fragment columns is -999
temp = soil_horizons[soil_horizons.total_coarse_fragment_percent != -999].filter(items=['total_coarse_fragment_percent', 'stone_percent', 'cobble_percent', 'gravel_percent']).describe() # Ensure that non-null values are between 0 and 100

# Format structure
## Use lookup table
soil_horizons = pd.merge(soil_horizons, lookup_structure,
                        how='left', left_on='PRISTRUC', right_on='original_structure')

## Replace remaining values with NULL
soil_horizons[soil_horizons.structure.isna()].PRISTRUC.unique()
soil_horizons['structure'] = soil_horizons.structure.fillna("NULL")

## Ensure values match dictionary values
dictionary_values = dictionary[dictionary.field == 'soil_structure'].data_attribute_id.to_numpy()
temp = soil_horizons.loc[:, "structure"].unique()
temp = temp[temp != "NULL"]
temp_contains = np.isin(temp, dictionary_values, assume_unique=True)

if False in temp_contains:
    print("structure has non-matching values")
else:
    print("structure all values match")

# Format matrix hue

## Split on 'YR' to remove additional information
soil_horizons = soil_horizons.assign(matrix_hue = soil_horizons.PRIMOIST.str.split(pat="YR|Y|TR|yr|y", regex=True, expand=True)[0])

## Add back 'YR' where appropriate
soil_horizons = soil_horizons.assign(matrix_hue = np.where(soil_horizons.PRIMOIST.str.contains(pat="YR|Y|TR|yr|y", regex=True), soil_horizons.matrix_hue + "YR", soil_horizons.matrix_hue))

# Correct non-matching values
values_replace = {
    "SI-2.5YR": "2.5YR",
    "\\,": ".",
    "25": "2.5",
    "GLEYR|GleYR|-|G1 2.5/1": "NULL",
    "7.5R3/2": "7.5R",
    "735YR": "7.5YR",
    "N4/0|N3/0": "N",
    " ": ""
}

soil_horizons = soil_horizons.assign(matrix_hue = soil_horizons.matrix_hue.replace(values_replace, regex=True))

## Ensure values match dictionary values
dictionary_values = dictionary[dictionary.field == 'soil_hue'].data_attribute_id.to_numpy()
temp = soil_horizons.loc[:, "matrix_hue"].unique()
temp = temp[temp != "NULL"]
temp_contains = np.isin(temp, dictionary_values, assume_unique=True)

if False in temp_contains:
    print("matrix hue has non-matching values")
else:
    print("matrix hue all values match")

# Populate remaining columns with appropriate null values
horizons_final = soil_horizons.assign(clay_percent=-999,
                                      boulder_percent=-999,
                                      matrix_value=-999,
                                      matrix_chroma=-999,
                                      nonmatrix_feature ='NULL',
                                      nonmatrix_hue='NULL',
                                      nonmatrix_value=-999,
                                      nonmatrix_chroma=-999)

## Drop all columns that aren't in the template and reorder them to match data entry template
horizons_final = horizons_final[template.columns]

# QA/QC
temp = horizons_final.describe(include='all')

# Export dataframe
horizons_final.to_csv(soil_horizons_output, index=False, encoding='UTF-8')
