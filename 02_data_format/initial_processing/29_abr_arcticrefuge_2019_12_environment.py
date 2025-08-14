# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2019 ABR Arctic Refuge Environment Data
# Author: Amanda Droghini
# Last Updated: 2025-05-30
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2019 ABR Arctic Refuge Environment Data" prepares environment data recorded during vegetation
# surveys for ingestion in the AKVEG database. The script appends unique site visit identifiers, re-classifies
# categorical values to match constraints in the AKVEG database, replaces empty observations with appropriate null
# values, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT
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
                              'AKVEG_Database')
schema_folder = os.path.join(project_folder, 'Data')
plot_folder = os.path.join(schema_folder, 'Data_Plots', '29_abr_arcticrefuge_2019')
source_folder = os.path.join(plot_folder, 'source')

# Define inputs
site_visit_input = os.path.join(plot_folder, '03_sitevisit_abrarcticrefuge2019.csv')
environment_ground_input = os.path.join(source_folder, 'abr_anwr_ns_lc_els_deliverable.csv')
environment_aerial_input = os.path.join(source_folder, 'abr_anwr_ns_lc_veg_aerial_deliverable_part2_wider.csv')
template_input = os.path.join(schema_folder, 'Data_Entry', '12_environment.xlsx')
dictionary_input = os.path.join(schema_folder, 'Tables_Metadata', 'database_dictionary.xlsx')

# Define output
environment_output = os.path.join(plot_folder, '12_environment_abrarcticrefuge2019.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input, usecols=['site_code', 'site_visit_code'])
ground_original = pd.read_csv(environment_ground_input,
                              usecols=['plot_id', 'physiography', 'subsurface_terrain', 'surface_terrain',
                                       'macrotopography', 'microtopography', 'microrelief', 'soil_sample_method',
                                       'soil_surface_organic_thick_cm', 'soil_dom_texture_40cm',
                                       'soil_rock_depth_probe_cm', 'soil_class', 'soil_moisture', 'drainage',
                                       'soil_obs_maximum_depth_cm', 'water_depth_cm',
                                       'water_above_below_surf',
                                       'cryoturb_ynu', 'disturbance_class', 'env_field_note'])
aerial_original = pd.read_csv(environment_aerial_input,
                              usecols=['plot_id', 'physiography', 'macrotopography', 'microtopography',
                                       'surface_terrain',
                                       'microrelief', 'water_depth_cm', 'disturbance_class', 'cryoturb_ynu',
                                       'soil_moisture', 'veg_aerial_field_note'])
template = pd.read_excel(template_input)
dictionary = pd.read_excel(dictionary_input)

# Rename 'field note' column to match ground data
aerial_data = aerial_original.rename(columns={"veg_aerial_field_note": "env_field_note"})

# Remove duplicate site
# 'anwrlc_4900_2019' present in both ground and aerial datasheets; should only be in aerial
ground_data = ground_original.loc[ground_original['plot_id'] != 'anwrlc_4900_2019']

# Append ground and aerial data
environment = pd.concat([ground_data, aerial_data], axis=0)

# Obtain site visit code
environment = (environment.assign(join_key=environment['plot_id'].str.removesuffix('_2019'))
               .merge(site_visit_original, how='right', left_on='join_key', right_on='site_code'))

# Drop duplicates
## Ground sites listed on aerial datasheet and vice-versa. Choose columns on which to drop that are the most
# frequently filled out
## Ensure ignore_index is set to 'True' to avoid errors when verifying for equality in the next step
environment = environment.dropna(subset=['surface_terrain', 'macrotopography', 'microtopography'], how='all',
                                 ignore_index=True)

## Ensure all sites in site_visit table are included in environment table
print(environment['site_visit_code'].isna().sum())
print(environment['plot_id'].isna().sum())
print(site_visit_original['site_visit_code'].equals(environment['site_visit_code']))

# Standardize null values
print(environment.isna().sum())

## Replace no data fields with appropriate null values
environment = environment.replace(['Unknown', 'Not Determined', 'Not Assessed', 'No Data', -997, -998], np.nan)

null_values = {"subsurface_terrain": 'NULL',
               "surface_terrain": 'NULL',
               "physiography": 'NULL',
               "macrotopography": 'NULL',
               "microtopography": 'NULL',
               "microrelief": -999,
               "soil_sample_method": 'NULL',
               "soil_surface_organic_thick_cm": -999,
               "soil_dom_texture_40cm": 'NULL',
               "soil_rock_depth_probe_cm": -999,
               "soil_obs_maximum_depth_cm": -999,
               "soil_class": 'not determined',
               "soil_moisture": "NULL",
               "drainage": 'NULL',
               "water_depth_cm": -999,
               "water_above_below_surf": 'NULL',
               "cryoturb_ynu": 'NULL',
               "disturbance_class": 'NULL',
               "env_field_note": "NULL",
               }
environment = environment.fillna(value=null_values)

# Format physiography
## All terms match constrained values in the AKVEG database
environment = environment.assign(physiography=np.where(environment['physiography'] != 'NULL',
                                                       environment['physiography'].str.lower(),
                                                       environment['physiography']))

print(environment['physiography'].value_counts())

# Format geomorphology
print(environment['surface_terrain'].value_counts())

conditions = [environment['surface_terrain'].str.contains('Loess'),
              (environment['surface_terrain'] == 'Active Marine Beach') |
              (environment['surface_terrain'] == 'Active Tidal Flat'),
              (environment['surface_terrain'] == 'Alluvial Fan Abandoned Deposit') | (
                      environment['surface_terrain'] == 'Old Alluvial Fan'),
              (environment['surface_terrain'] == 'Alluvial-Marine Deposit') &
              ((environment['physiography'] != 'riverine') &
               (environment['physiography'] != 'coastal')),
              ((environment['surface_terrain'].str.contains('Abandoned Channel Deposit')) |
               (environment['surface_terrain'].str.contains('Abandoned Overbank Deposit'))) &
              (~environment['surface_terrain'].str.contains('Delta')),
              ((environment['surface_terrain'].str.contains('Active Channel')) |
               (environment['surface_terrain'].str.contains('Inactive Channel'))) &
              (~environment['surface_terrain'].str.contains('Delta')),
              ((environment['surface_terrain'].str.contains('Active Overbank Deposit')) |
               (environment['surface_terrain'].str.contains('Inactive Overbank Deposit'))) &
              (~environment['surface_terrain'].str.contains('Delta')),
              environment['surface_terrain'].str.startswith('Colluvial'),
              environment['surface_terrain'] == 'Delta Abandoned Overbank Deposit',
              environment['surface_terrain'].str.startswith('Delta'),
              environment['surface_terrain'].str.contains('Basin, ice-poor'),
              environment['surface_terrain'].str.contains('Basin, ice-rich'),
              (environment['surface_terrain'] == 'Drained Lake Basin') | (
                          environment['surface_terrain'] == 'Thaw Basins and Thaw Lakes') | ((environment[
                  'macrotopography'] == 'Basins Or Depressions') & (environment['surface_terrain'] == 'NULL')),
              environment['surface_terrain'].str.startswith('Eolian'),
              environment['surface_terrain'].str.startswith('Fluvial Deposit'),
              environment['surface_terrain'] == 'Glacial Deposit',
              environment['surface_terrain'] == 'Glaciofluvial Deposit',
              environment['surface_terrain'] == 'Hillside Colluvium',
              (environment['surface_terrain'].str.startswith('Inactive')),
              (environment['surface_terrain'].str.startswith('Moraine')) | (
                      environment['surface_terrain'] == 'Older Till'),
              environment['surface_terrain'].str.contains('Alluvial Terrace'),
              environment['surface_terrain'] == 'Shallow Isolated Riverine Lake',
              environment['surface_terrain'] == 'Solifluction Deposit',
              environment['macrotopography'] == 'Plateau',
              environment['macrotopography'] == 'Flood Basin'
              ]

choices = ['aeolian deposit, silt', 'marine, shore', 'alluvial plain',
           'fluviomarine terrace', 'alluvial plain',
           'floodplain', 'floodplain',
           'colluvial deposit',
           'fluviomarine terrace', 'delta',
           'basin, ice-poor', 'basin, ice-rich', 'basin', 'aeolian deposit, sand', 'alluvial plain', 'glacial deposit',
           'glaciofluvial deposit',
           'colluvial deposit', 'marine, shore',
           'glacial deposit', 'alluvial plain', 'aquatic, lake fresh', 'colluvial deposit', 'plateau',
           'floodplain']

environment['geomorphology'] = np.select(conditions, choices, default='NULL')

## Explore null values
temp = environment.loc[environment['geomorphology'] == 'NULL']   ## Field notes for both sites indicate 'unsure about
# geomorphology'

# Format macrotopography

## Rename column to prevent overwrite
environment = environment.rename(columns={"macrotopography": "macro_original"})

## Examine existing values
print(environment['macro_original'].value_counts())

conditions = [(environment['surface_terrain'].str.contains('Bog')),
              (environment['surface_terrain'].str.contains('Fen')),
              (environment['surface_terrain'].str.contains('Abandoned Channel Deposit') |
               environment['surface_terrain'].str.contains('Abandoned Overbank Deposit')) &
              (~environment['surface_terrain'].str.startswith('Delta')),
              (environment['surface_terrain'].str.contains('Alluvial Terrace')),
              (environment['surface_terrain'].str.startswith('Alluvial')) &
              (environment['macro_original'] == 'Flat or fluvial related'),
              (environment['surface_terrain'].str.contains('Channel Deposit') &
               (~environment['surface_terrain'].str.contains('Abandoned')) &
               (~environment['macro_original'].str.contains('Bar'))),
              environment['surface_terrain'].str.contains('Tidal Flat'),
              (environment['macro_original'].str.contains('Bar')) &
              ((environment['geomorphology'] == 'floodplain') | (environment['geomorphology'] == 'delta')),
              (environment['surface_terrain'].str.contains('Overbank')) &
              ((environment['geomorphology'] == 'floodplain') | (environment['geomorphology'] == 'delta')) &
              (~environment['macro_original'].str.contains('Bar')),
              (environment['macro_original'].str.startswith('Basin')) |
              (environment['macro_original'].str.contains('Drain|Depression', regex=True)) |
              (environment['surface_terrain'].str.contains('Thaw Basin')) | (
                  environment['surface_terrain'].str.startswith('Drained Lake Basin')),
              environment['macro_original'].str.contains('Bluff'),
              environment['macro_original'] == 'Crest',
              (environment['macro_original'] == 'Flood Basin') &
              (~environment['surface_terrain'].str.contains('Abandoned')) &
              (~environment['surface_terrain'].str.contains('Bog')),
              ((environment['macro_original'] == 'Floodplain Step') |
               (environment['macro_original'] == 'Levee')) &
              ((~environment['surface_terrain'].str.contains('Abandoned')) &
               (environment['surface_terrain'] != 'Eolian Active Sand Dune')),
              (environment['macro_original'].str.contains('Lake Margin')),
              (environment['macro_original'] == 'Toeslope'),
              (environment['macro_original'] == 'Foot Slope'),
              environment['macro_original'].str.contains('Slope') &
              environment['macro_original'].str.contains('Plan'),
              environment['macro_original'].str.contains('Slope') &
              environment['macro_original'].str.contains('Concave'),
              environment['macro_original'].str.contains('Slope') &
              environment['macro_original'].str.contains('Convex'),
              (environment['macro_original'].str.contains('Slope', case=False)) &
              (~environment['macro_original'].str.contains('Plan|Concave|Convex', regex=True)) &
              (~environment['surface_terrain'].str.contains('Moraine')),
              ((environment['macro_original'] == 'Marine Beach') |
               (environment['surface_terrain'] == 'Active Marine Beach')) &
              (environment['surface_terrain'] != 'Active Tidal Flat'),
              environment['macro_original'] == 'Shoulder',
              environment['macro_original'].str.contains('Dune') |
              environment['surface_terrain'].str.contains('Dune'),
              (environment['surface_terrain'] == 'Recent Alluvial Terrace') &
              (environment['macro_original'] == 'Tread'),
              (environment['macro_original'] == 'Waterbodies') &
              (environment['geomorphology'].str.contains('aquatic, lake')),
              (environment['surface_terrain'].str.contains('Moraine')),
              environment['microtopography'].str.contains('High-centered'),
              environment['microtopography'].str.contains('Low-centered'),
              (environment['microtopography'] == 'Mixed High and Low-centered Polygons'),
              (environment['geomorphology'] == 'floodplain') & (environment['macro_original'] == 'Riser'),
              environment['surface_terrain'] == 'Old Alluvial Fan',
              ]

choices = ['organic deposits, bog', 'organic deposits, fen', 'floodplain abandoned', 'alluvial terrace',
           'alluvial flat', 'floodplain channel deposit',
           'tidal flat', 'floodplain bar', 'floodplain terrace', 'depression', 'bluff', 'ridge',
           'floodplain basin', 'floodplain terrace', 'lake shore', 'toeslope', 'footslope',
           'slope planar', 'slope concave',
           'slope convex', 'slope', 'beach', 'shoulder',
           'dunes', 'floodplain terrace', 'lake shore', 'moraine',
           'polygons high-center', 'polygons low-center', 'polygons mixed', 'floodplain terrace', 'alluvial fan'
           ]

environment['macrotopography'] = np.select(conditions, choices, default='NULL')

## Explore remaining null values
temp = environment[environment['macrotopography'] == 'NULL']
print(temp['macro_original'].value_counts()) # All NULL values are OK

## Verify whether values match constrained values
temp = environment[environment['macrotopography'] != 'NULL']
temp = np.isin(temp['macrotopography'].unique(),
               [dictionary['data_attribute']])
print(False in temp)  # Should be False

# Format microtopography

## Rename column to prevent overwrite
environment = environment.rename(columns={"microtopography": "micro_original"})

## Examine existing values
environment['micro_original'].value_counts()

conditions = [environment['micro_original'] == 'Hummocks',
              environment['micro_original'] == 'Undifferentiated mounds',
              environment['micro_original'] == 'Mounds (ice and peat related)',
              environment['micro_original'] == 'Soil-covered Rocks',
              environment['micro_original'] == 'Gelifluction lobes',
              environment['micro_original'].str.contains('Circles'),
              (environment['micro_original'].str.startswith('Water tracks')) | (
                  environment['macro_original'].str.startswith('Water Tracks')),
              environment['micro_original'].str.startswith('Stripes'),
              environment['micro_original'] == 'Steps (non-sorted, sorted)',
              environment['micro_original'] == 'Nets (non-sorted, sorted)',
              environment['micro_original'].str.contains('Polygon', case=False),
              environment['micro_original'] == 'Strangmoor',
              (environment['micro_original'] == 'Drainage Patterns') | (environment['micro_original'] == 'Scour '
                                                                                                         'Channels '
                                                                                                         'and Ridges'),
              environment['micro_original'] == 'Riverbed Cobbles or Boulders',
              environment['micro_original'] == 'Feather patterns (in fens)',
(environment['micro_original'] == 'Water') & (environment['macro_original'] == 'Basins Or Depressions'),
              (environment['site_visit_code'] == 'Complexes') | (environment['micro_original'] == 'Nonpatterned') | (
                      environment['micro_original'] == 'Pits (small features)') | (environment['micro_original'] ==
                                                                                   'Small Dunes')

              ]

choices = ['hummocks', 'mounds', 'peat mounds',
           'soil-covered rocks', 'gelifluction lobes', 'circles (non-sorted, sorted)',
           'water tracks', 'stripes (non-sorted, sorted)', 'steps (non-sorted, sorted)', 'nets (non-sorted, sorted)',
           'polygonal', 'string', 'drainageway', 'block field', 'patterned ground', 'ponds', 'NULL']

environment['microtopography'] = np.select(conditions, choices, default='NULL')

## Explore remaining null values
temp = environment[(environment['microtopography'] == 'NULL') & (environment['micro_original'] != 'Nonpatterned')]
print(temp['micro_original'].value_counts())

## Ensure that values match constrained values
temp = environment[environment['microtopography'] != 'NULL']
temp = np.isin(temp['microtopography'].unique(),
               [dictionary['data_attribute']])
print(False in temp)  # Should be false

# Format moisture regime
print(environment['soil_moisture'].value_counts())

## Re-classify soil moisture values
## Used same classification scheme as 27_abr_various_2019.
## Unsure about 'wet' class.
conditions = [(environment['soil_moisture'] == 'Wet') &
              (environment['drainage'].str.contains('Very poorly drained|Flooded', regex=True)),
              environment['soil_moisture'] == 'Wet',
              environment['soil_moisture'] == 'Moist',
              environment['soil_moisture'] == 'Dry',
              environment['soil_moisture'] == 'Aquatic']

choices = ['hydric', 'hygric', 'mesic', 'xeric', 'aquatic']

environment['moisture_regime'] = np.select(conditions, choices, default='NULL')

print(environment['moisture_regime'].value_counts())

# Format drainage

## Rename column to prevent overwrite
environment = environment.rename(columns={"drainage": "drainage_original"})

print(environment['drainage_original'].value_counts())  ## Only one row has data on drainage

environment = environment.assign(drainage = np.where(environment['drainage_original'] == 'Very poorly drained',
                                                     'poorly drained', 'NULL'))

print(environment['drainage'].value_counts())

## Ensure that combination of moisture regime & drainage values make sense
temp = pd.crosstab(environment['moisture_regime'], environment['drainage'])

# Format disturbance
print(environment['disturbance_class'].value_counts())

conditions = [environment['disturbance_class'].str.startswith('Absent'),
              environment['disturbance_class'].str.startswith('Fluvial'),
              environment['disturbance_class'] == 'Thermokarst',
              (environment['disturbance_class'].str.startswith('Eolian')),
              environment['disturbance_class'] == 'Avian grazing',
              environment['disturbance_class'].str.contains('Storm surges'),
              environment['disturbance_class'] == 'Undifferentiated Trail',
              environment['disturbance_class'] == 'Animals, Wildlife',  # Notes said 'caribou trails'
              environment['disturbance_class'] == 'Marine wave erosion']

choices = ['none', 'riparian', 'permafrost dynamics', 'aeolian process',
           'wildlife foraging', 'weather process',
           'trail', 'wildlife trails', 'tidal']

environment['disturbance'] = np.select(conditions, choices, default='NULL')

print(environment['disturbance'].value_counts())

# Format disturbance years
## Assume that all disturbances are recent
environment['disturbance_time_y'] = np.where(environment['disturbance'].str.contains('none|NULL', regex=True), -999, 0)

## Verify that values were correctly converted
print(pd.crosstab(environment['disturbance'], environment['disturbance_time_y']))

# Format water depth

## Explore values
print(environment.loc[environment['water_depth_cm']>-999].water_depth_cm.describe())

## Explore relationship between water depth and surface water columns to look for inconsistencies e.g.,
# negative water depth but water above surface
temp = environment.loc[environment['water_depth_cm']>-999].filter(items=['water_depth_cm', 'water_above_below_surf'])

## All value pairings seem logical - No other changes to make.

## Rename column
environment = environment.rename(columns={"water_depth_cm": "depth_water_cm"})

# Format surface water
print(environment['water_above_below_surf'].value_counts())

conditions = [environment['water_above_below_surf'] == 'Above',
              environment['water_above_below_surf'] == 'Below']
choices = [True, False]

environment['surface_water'] = np.select(conditions, choices, default='NULL')

## Ensure that values were properly converted
print(environment['surface_water'].value_counts())

# Format depth moss duff
print(environment.loc[environment['soil_surface_organic_thick_cm'] > -999].soil_surface_organic_thick_cm.describe())

## Rename column. No other changes needed.
environment = environment.rename(columns={"soil_surface_organic_thick_cm": "depth_moss_duff_cm"})

# Format depth restrictive layer
print(environment.loc[environment['soil_rock_depth_probe_cm'] > -999].soil_rock_depth_probe_cm.describe())

## Rename column. No other changes needed.
environment = environment.rename(columns={"soil_rock_depth_probe_cm": "depth_restrictive_layer_cm"})

# Format restrictive type
environment = environment.assign(restrictive_type=np.where(environment['depth_restrictive_layer_cm'] != -999,
                                                           'rock unconsolidated',
                                                           'NULL'))

## Ensure all values (except NULL) correspond with a constrained value
print(environment.restrictive_type.value_counts())

# Format microrelief

## Explore values
print(environment.microrelief.value_counts())

## Replace range with midpoint value
environment = environment.assign(microrelief_cm=environment.microrelief.replace(['<10 cm', '10-29 cm', '30-49 cm',
                                                                                 '50-74 cm'],
                                                                                ["5", "20", "40", "62"]))

## Convert column to numeric
environment['microrelief_cm'] = environment['microrelief_cm'].astype(int)
print(environment.loc[environment['microrelief_cm']>-999].microrelief_cm.describe())

# Format soil class
print(environment['soil_class'].value_counts())

## Rename column
environment = environment.rename(columns={"soil_class": "soil_class_original"})

## Convert to lowercase
environment = environment.assign(soil_class = environment['soil_class_original'].str.lower())

## Replace 'gelisols' with 'not available'. Identification to subgroup is required, but not provided.
environment = environment.assign(soil_class = np.where(environment['soil_class'] == 'gelisols', 'not available',
                                                       environment['soil_class']))

## Verify whether values match constrained values
temp = np.isin(environment['soil_class'].unique(),
               [dictionary['data_attribute']])
print(False in temp) # Should return False

# Format cryoturbation
print(environment.cryoturb_ynu.value_counts())

## Replace values with appropriate boolean values
environment['cryoturbation'] = environment['cryoturb_ynu'].replace(['No', 'Yes'], ['FALSE', 'TRUE'])

## Verify that values were properly converted
print(environment['cryoturbation'].value_counts())

# Format dominant texture at 40 cm
print(environment['soil_dom_texture_40cm'].value_counts())

## Convert to lowercase
environment['dominant_texture_40_cm'] = environment['soil_dom_texture_40cm'].str.lower()

## Explore constrained values
dictionary[dictionary['field'] == 'soil_texture'].data_attribute.unique()

## Replace values with values that match constrained values
environment['dominant_texture_40_cm'] = environment['dominant_texture_40_cm'].map({'loamy': 'loam',
                                                                                   'sandy': 'sand'},
                                                                                  na_action='ignore')
environment['dominant_texture_40_cm'] = environment['dominant_texture_40_cm'].fillna('NULL')

## Verify that values were properly converted
print(environment['dominant_texture_40_cm'].value_counts())

# Final formatting

## Populate values for missing columns
environment_final = environment.assign(disturbance_severity='NULL',
                                       depth_15_percent_coarse_fragments_cm=-999)

## Drop all columns that aren't in the template
## Reorder columns to match data entry template
environment_final = environment_final[template.columns]

# Export dataframe
environment_final.to_csv(environment_output, index=False, encoding='UTF-8')
