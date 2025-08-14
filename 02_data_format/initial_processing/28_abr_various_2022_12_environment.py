# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2022 ABR Various Environment Data
# Author: Amanda Droghini
# Last Updated: 2025-05-05
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2022 ABR Various Environment Data" prepares environment data recorded during vegetation
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
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '28_abr_various_2022')
source_folder = os.path.join(plot_folder, 'source')
schema_folder = os.path.join(project_folder, 'Data')

# Define inputs
site_visit_input = os.path.join(plot_folder, '03_sitevisit_abrvarious2022.csv')
environment_input = os.path.join(source_folder, 'tnawrocki_deliverable_two_els.txt')
template_input = os.path.join(schema_folder, 'Data_Entry', '12_environment.xlsx')
dictionary_input = os.path.join(schema_folder, 'Tables_Metadata', 'database_dictionary.xlsx')

# Define output
environment_output = os.path.join(plot_folder, '12_environment_abrvarious2022.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input, usecols=['site_code', 'site_visit_id'])
environment_original = pd.read_csv(environment_input, delimiter='|',
                                   usecols=['plot_id', 'env_field_start_ts', 'slope_degrees', 'veg_structure_ecotype',
                                            'physiography', 'surface_terrain', 'macrotopography', 'microtopography',
                                            'microrelief', 'soil_surface_organic_thick_cm', 'soil_dom_texture_40cm',
                                            'soil_class', 'soil_moisture', 'drainage', 'water_depth_cm',
                                            'water_above_below_surf', 'cryoturb_ynu', 'disturbance_class',
                                            'soil_restrict_layer', 'soil_restrict_depth_probe_cm',
                                            'soil_rock_depth_probe_cm', 'soil_obs_maximum_depth_cm'
                                            ])
template = pd.read_excel(template_input)
dictionary = pd.read_excel(dictionary_input)

# Format site code
## Drop year suffix
environment = environment_original.assign(
    join_key=np.where(environment_original['plot_id'].str.contains('_20+\\d{2}$|-veg$', regex=True),
                      environment_original['plot_id'].str.replace('_20+\\d{2}$|-veg', '', regex=True),
                      environment_original['plot_id']))

# SUWA sites require additional formatting: append 2-digit year to existing plot_id
environment = environment.assign(survey_year=environment['env_field_start_ts'].str[2:4])
environment = environment.assign(join_key=np.where(environment['join_key'].str.startswith('SUWA'),
                                                   environment['join_key'].str.cat(environment['survey_year'], sep=''),
                                                   environment['join_key']))

# Obtain site visit code
environment = environment.merge(site_visit_original, how='right', left_on='join_key', right_on='site_code')

## Ensure all sites in site_visit table are included in environment table
environment['site_visit_id'].isna().sum()
environment['plot_id'].isna().sum()
site_visit_original['site_visit_id'].equals(environment['site_visit_id'])

# Standardize null values
environment.isna().sum()

## Replace no data fields with appropriate null values
environment = environment.replace(['No Data', 'Not Assessed', 'Unknown', 'Not Determined', -998], np.nan)

null_values = {"slope_degrees": -999,
               "physiography": 'NULL',
               "surface_terrain": 'NULL',
               "macrotopography": 'NULL',
               "microtopography": 'NULL',
               "microrelief": -999,
               "soil_surface_organic_thick_cm": -999,
               "soil_dom_texture_40cm": 'NULL',
               "soil_class": 'not available',
               "soil_moisture": "NULL",
               "drainage": 'NULL',
               "water_depth_cm": -999,
               "water_above_below_surf": 'NULL',
               "cryoturb_ynu": 'NULL',
               "disturbance_class": 'NULL',
               "soil_restrict_layer": 'NULL',
               "soil_restrict_depth_probe_cm": -999,
               "soil_rock_depth_probe_cm": -999}
environment = environment.fillna(value=null_values)

# Format physiography
## All terms match constrained values in the AKVEG database
environment['physiography'].value_counts()

environment = environment.assign(physiography=np.where(environment['physiography'] != 'NULL',
                                                       environment['physiography'].str.lower(),
                                                       environment['physiography']))

environment['physiography'].value_counts()

# Format geomorphology
environment['surface_terrain'].value_counts()

conditions = [environment['surface_terrain'] == 'Active Gravelly Marine Beach',
              (environment['surface_terrain'] == 'Active Tidal Flat'),
              (environment['surface_terrain'] == 'Alluvial Plain Deposit') |
              (environment['surface_terrain'].str.contains('Alluvial Terrace')),
              (environment['surface_terrain'] == 'Alluvial-Marine Deposit') &
              (environment['physiography'] == 'riverine'),
              (environment['surface_terrain'] == 'Alluvial-Marine Deposit') &
              (environment['physiography'] == 'coastal'),
              (environment['surface_terrain'] == 'Alluvial-Marine Deposit') &
              ((environment['physiography'] != 'riverine') &
               (environment['physiography'] != 'coastal')),
              environment['surface_terrain'] == 'Alluvial-Marine Terrace',
              environment['surface_terrain'].str.startswith('Brackish'),
              ((environment['surface_terrain'].str.contains('Abandoned Channel Deposit')) |
               (environment['surface_terrain'].str.contains('Abandoned Overbank Deposit'))) &
              (environment['surface_terrain'].str.contains('Delta') == False),
              ((environment['surface_terrain'].str.contains('Active Channel')) |
               (environment['surface_terrain'].str.contains('Inactive Channel'))) &
              (environment['surface_terrain'].str.contains('Delta') == False),
              ((environment['surface_terrain'].str.contains('Active Overbank Deposit')) |
               (environment['surface_terrain'].str.contains('Inactive Overbank Deposit')) |
               (environment['surface_terrain'].str.contains('Active Deposit')) |
               (environment['surface_terrain'].str.contains('Inactive Deposit'))) &
              (environment['surface_terrain'].str.contains('Delta') == False),
              environment['surface_terrain'].str.startswith('Colluvial'),
              ((environment['surface_terrain'].str.contains('Lake')) |
               (environment['surface_terrain'].str.contains('Pond')) |
               (environment['surface_terrain'].str.contains('Thermokarst Pit'))) &
              ((environment['surface_terrain'].str.contains('Brackish') == False) &
               (environment['surface_terrain'].str.contains('Basin') == False)),
              environment['surface_terrain'] == 'Delta Abandoned Overbank Deposit',
              environment['surface_terrain'].str.startswith('Delta'),
              (environment['surface_terrain'].str.contains('Basin, ice-poor')),
              environment['surface_terrain'].str.contains('Basin, ice-rich') |
              (environment['surface_terrain'].str.contains('Basin, pingo')),
              environment['surface_terrain'].str.startswith('Eolian'),
              environment['surface_terrain'].str.contains('Floodplain'),
              environment['surface_terrain'] == 'Hillside Colluvium',
              (environment['surface_terrain'].str.startswith('Inactive')),
              (environment['surface_terrain'] == 'Lacustrine Deposit') &
              (environment['macrotopography'].str.startswith('Basin')),
              (environment['surface_terrain'] == 'Lacustrine Deposit') &
              (environment['macrotopography'].str.startswith('Basin') == False),
              environment['surface_terrain'].str.contains('Loess'),
              (environment['surface_terrain'].str.contains('River')) &
              (environment['surface_terrain'].str.contains('riverine', case=False) == False),
              environment['surface_terrain'] == 'Lowland Headwater Stream',
              environment['surface_terrain'].str.startswith('Moraine'),
              environment['surface_terrain'] == 'Nearshore Water',
              (environment['surface_terrain'] == 'Solifluction Deposit') |
              environment['surface_terrain'].str.contains('Retransported Deposit'),
              ]

choices = ['marine, shore', 'marine, shore', 'alluvial plain', 'floodplain', 'marine, shore',
           'fluviomarine terrace', 'fluviomarine terrace', 'aquatic, lake brackish', 'alluvial plain',
           'floodplain', 'floodplain',
           'colluvial deposit', 'aquatic, lake',
           'fluviomarine terrace', 'delta', 'basin, ice-poor', 'basin, ice-rich',
           'aeolian deposit, sand', 'floodplain', 'colluvial deposit', 'marine, shore',
           'basin, ice-poor', 'lacustrine deposit', 'aeolian deposit, silt', 'aquatic, river', 'headwater stream',
           'glacial deposit', 'marine, ocean',
           'colluvial deposit']

environment['geomorphology'] = np.select(conditions, choices, default='NULL')

# Format macrotopography

## Rename column to prevent overwrite
environment = environment.rename(columns={"macrotopography": "macro_original"})

## Examine existing values
environment['macro_original'].value_counts()

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
              ((environment['surface_terrain'].str.contains('Channel') |
                (environment['surface_terrain'].str.contains('Overbank'))) &
               (~environment['surface_terrain'].str.contains('Abandoned'))),
              (environment['macro_original'].str.startswith('Basin')) |
              (environment['macro_original'].str.contains('Drain|Depression', regex=True)) |
              (environment['surface_terrain'].str.contains('Basin')),
              environment['macro_original'].str.contains('Bluff'),
              ((environment['macro_original'] == 'Braided Channels and Bars') |
               (environment['macro_original'] == 'Channel')) &
              ((~environment['surface_terrain'].str.contains('Abandoned')) &
               (environment['surface_terrain'] != 'Bogs') &
               (~environment['surface_terrain'].str.contains('Fen')) &
               (~environment['surface_terrain'].str.contains('Lake'))),
              environment['macro_original'] == 'Crest',
              (environment['macro_original'] == 'Flood Basin') &
              (~environment['surface_terrain'].str.contains('Abandoned')) &
              (~environment['surface_terrain'].str.contains('Bog')),
              ((environment['macro_original'] == 'Floodplain Step') |
               (environment['macro_original'] == 'Levee')) &
              ((~environment['surface_terrain'].str.contains('Abandoned')) &
               (environment['surface_terrain'] != 'Eolian Active Sand Dune')),
              (environment['macro_original'] == 'Polygonized Pond Margins') |
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
               (environment['surface_terrain'] == 'Active Gravelly Marine Beach')) &
              (environment['surface_terrain'] != 'Active Tidal Flat'),
              environment['macro_original'] == 'Pingo',
              environment['macro_original'] == 'Shoulder',
              environment['macro_original'].str.contains('Dune') |
              environment['surface_terrain'].str.contains('Dune'),
              (environment['surface_terrain'] == 'Recent Alluvial Terrace') &
              (environment['macro_original'] == 'Tread'),
              environment['macro_original'] == 'Undulating',
              (environment['macro_original'] == 'Waterbodies') &
              (environment['geomorphology'].str.contains('aquatic, lake')),
              (environment['surface_terrain'].str.contains('Moraine')),
              (environment['geomorphology'] == 'marine, ocean') &
              (environment['surface_terrain'] == 'Nearshore Water'),
              environment['microtopography'].str.contains('High-centered'),
              environment['microtopography'].str.contains('Low-centered'),
              (environment['microtopography'] == 'Mixed High and Low-centered Polygons'),
              environment['macro_original'] == 'Lake, Islands Present',
              environment['surface_terrain'].str.contains('Sand Sheet')
              ]

choices = ['organic deposits, bog', 'organic deposits, fen', 'floodplain abandoned', 'alluvial terrace',
           'alluvial flat', 'floodplain channel deposit',
           'tidal flat', 'floodplain bar', 'depression', 'bluff', 'floodplain channel deposit', 'ridge',
           'floodplain basin', 'floodplain terrace', 'lake shore', 'toeslope', 'footslope',
           'slope planar', 'slope concave',
           'slope convex', 'slope', 'beach', 'pingo', 'shoulder',
           'dunes', 'floodplain terrace', 'undulating', 'lake shore', 'moraine', 'nearshore zone',
           'polygons high-center', 'polygons low-center', 'polygons mixed', 'lake shore', 'sand sheet'
           ]

environment['macrotopography'] = np.select(conditions, choices, default='NULL')

## Explore remaining null values
temp = environment[environment['macrotopography'] == 'NULL']
temp['macro_original'].value_counts()

## Verify whether values match constrained values
temp = environment[environment['macrotopography'] != 'NULL']
temp = np.isin(temp['macrotopography'].unique(),
               [dictionary['data_attribute']])
False in temp  # Should be false

# Format microtopography

## Rename column to prevent overwrite
environment = environment.rename(columns={"microtopography": "micro_original"})

## Examine existing values
environment['micro_original'].value_counts()

conditions = [environment['macro_original'] == 'Polygonized Pond Margins',
              environment['macro_original'] == 'Channel',
              environment['micro_original'] == 'Hummocks',
              environment['micro_original'] == 'Undifferentiated mounds',
              environment['micro_original'] == 'Peat mounds',
              environment['micro_original'] == 'Mounds (ice and peat related)',
              environment['micro_original'].str.startswith('Tree mounds'),
              environment['micro_original'] == 'Mounds caused by wildlife',
              environment['micro_original'] == 'Soil-covered Rocks',
              environment['micro_original'] == 'Rocks, Blockfields',
              environment['micro_original'] == 'Gelifluction lobes',
              environment['micro_original'] == 'Rocky Mounds/Outcrops',
              environment['micro_original'].str.contains('Circles'),
              environment['micro_original'] == 'Ice-cored mounds',
              environment['micro_original'].str.startswith('Water tracks'),
              environment['micro_original'].str.startswith('Stripes'),
              environment['micro_original'] == 'Steps (non-sorted, sorted)',
              environment['micro_original'] == 'Nets (non-sorted, sorted)',
              environment['micro_original'] == 'Troughs (Degraded ice-wedges)',
              environment['micro_original'].str.contains('Polygon', case=False),
              environment['micro_original'] == 'Ice-rafted debris',
              environment['macro_original'] == 'Ridge And Swale'
              ]

choices = ['polygonal', 'channeled', 'hummocks', 'mounds', 'peat mounds', 'peat mounds', 'mounds caused by trees',
           'mounds caused by wildlife',
           'soil-covered rocks',
           'block field', 'gelifluction lobes', 'outcrops', 'circles (non-sorted, sorted)', 'ice-cored mounds',
           'water tracks', 'stripes (non-sorted, sorted)', 'steps (non-sorted, sorted)', 'nets (non-sorted, sorted)',
           'ice wedge', 'polygonal', 'debris', 'ridges and swales']

environment['microtopography'] = np.select(conditions, choices, default='NULL')

## Explore remaining null values
temp = environment[environment['microtopography'] == 'NULL']
temp['micro_original'].value_counts()

## Ensure that values match constrained values
temp = environment[environment['microtopography'] != 'NULL']
temp = np.isin(temp['microtopography'].unique(),
               [dictionary['data_attribute']])
False in temp  # Should be false

# Format moisture regime
environment['soil_moisture'].value_counts()

## Re-classify soil moisture values
## Not sure what to do with 'wet' class. Used same classification scheme as 27_abr_various_2019
conditions = [(environment['soil_moisture'] == 'Wet') &
              (environment['drainage'].str.contains('Very poorly drained|Flooded', regex=True)),
              environment['soil_moisture'] == 'Wet',
              environment['soil_moisture'] == 'Moist',
              environment['soil_moisture'] == 'Dry',
              environment['soil_moisture'] == 'Aquatic']

choices = ['hydric', 'hygric', 'mesic', 'xeric', 'aquatic']

environment['moisture_regime'] = np.select(conditions, choices, default='NULL')

# Format drainage

## Rename column to prevent overwrite
environment = environment.rename(columns={"drainage": "drainage_original"})

environment['drainage_original'].value_counts()

conditions = [(environment['soil_moisture'] == 'Aquatic') & (environment['soil_class'] == 'Water'),
              (environment['soil_moisture'] == 'Aquatic') & (environment['soil_dom_texture_40cm'] == 'Water'),
              environment['drainage_original'] == 'Flooded',
              environment['drainage_original'] == 'Well drained',
              environment['drainage_original'].str.contains('excessively', case=False),
              environment['drainage_original'].str.contains('Moderately well drained|Somewhat poorly drained',
                                                            regex=True),
              environment['drainage_original'].str.contains('Very poorly drained|Poorly drained', regex=True)]
choices = ['aquatic', 'aquatic', 'flooded', 'well drained', 'well drained',
           'moderately drained', 'poorly drained']

environment['drainage'] = np.select(conditions, choices, default='NULL')

## Ensure that combination of moisture regime & drainage values make sense
temp = pd.crosstab(environment['moisture_regime'], environment['drainage'])

# Format disturbance
environment['disturbance_class'].value_counts()

conditions = [environment['disturbance_class'].str.startswith('Absent'),
              environment['disturbance_class'].str.startswith('Fluvial'),
              environment['disturbance_class'] == 'Thermokarst',
              (environment['disturbance_class'].str.startswith('Eolian')) |
              (environment['disturbance_class'] == 'Wind') |
              (environment['disturbance_class'] == 'Dust'),
              environment['disturbance_class'] == 'Fire',
              environment['disturbance_class'].str.contains('Geomorphic|Ice Scour', regex=True),
              environment['disturbance_class'] == 'Mammal excavations',
              environment['disturbance_class'].str.contains('Beaver'),
              environment['disturbance_class'].str.contains('Storm surges'),
              environment['disturbance_class'] == 'Ice Road or Pad',
              environment['disturbance_class'] == 'Disturbance complex',
              environment['disturbance_class'] == 'Structures and Debris']

choices = ['none', 'riparian', 'permafrost dynamics', 'aeolian process',
           'fire', 'geomorphic process', 'wildlife digging', 'wildlife beaver', 'weather process',
           'developed site', 'disturbance complex', 'structure']

environment['disturbance'] = np.select(conditions, choices, default='NULL')

## Ensure that combination of moisture regime & drainage values make sense
temp = pd.crosstab(environment['disturbance_class'], environment['disturbance'])

# Format disturbance years
## Assume that all disturbances are recent
environment['disturbance_time_y'] = np.where(environment['disturbance'].str.contains('none|NULL', regex=True), -999, 0)

## Verify that values were correctly converted
pd.crosstab(environment['disturbance'], environment['disturbance_time_y'])

# Format water depth
environment['water_depth_cm'].describe()

## Explore relationship between water depth and surface water columns to look for inconsistencies e.g.,
# negative water depth but water above surface
temp = environment[['water_depth_cm', 'water_above_below_surf']]
# Remove zero/null values
temp = temp[temp['water_depth_cm'] != 0]
temp = temp[temp['water_depth_cm'] > -999]

temp = temp.assign(water_depth_boolean=np.where(temp['water_depth_cm'] < 0, 'negative', 'positive'))
pd.crosstab(temp['water_depth_boolean'], temp['water_above_below_surf'])

## All value pairings seem logical - No other changes to make.

## Rename column
environment = environment.rename(columns={"water_depth_cm": "depth_water_cm"})

# Format surface water
environment['water_above_below_surf'].value_counts()

conditions = [environment['water_above_below_surf'] == 'Above',
              environment['water_above_below_surf'] == 'Below']
choices = ['TRUE', 'FALSE']

environment['surface_water'] = np.select(conditions, choices, default='NULL')

## Ensure that values were properly converted
environment['surface_water'].value_counts()

# Format depth moss duff
environment[environment['soil_surface_organic_thick_cm'] > -999].soil_surface_organic_thick_cm.describe()

## Rename column. No other changes needed.
environment = environment.rename(columns={"soil_surface_organic_thick_cm": "depth_moss_duff_cm"})

# Format depth restrictive layer
environment[environment['soil_restrict_depth_probe_cm'] > -999].soil_restrict_depth_probe_cm.describe()

## Rename column. No other changes needed.
environment = environment.rename(columns={"soil_restrict_depth_probe_cm": "depth_restrictive_layer_cm"})

# Format restrictive type
environment.soil_restrict_layer.value_counts()

## Convert string to lowercase (except for NULL)
environment['restrictive_type'] = np.where(environment['soil_restrict_layer'] != 'NULL',
                                           environment['soil_restrict_layer'].str.lower(),
                                           'NULL')

## Replace values that don't correspond to constrained values
environment = environment.assign(
    restrictive_type=environment['restrictive_type'].replace('relatively impermeable layer', 'NULL'))

## Ensure all values (except NULL) correspond with a constrained value
environment.restrictive_type.value_counts()
np.isin(environment.restrictive_type.unique(), [dictionary['data_attribute']])

# Format microrelief

## Explore values
environment.microrelief.value_counts()

## Replace range with midpoint
environment = environment.assign(microrelief_cm=environment['microrelief'].replace(['<10 cm', '10-29 cm', '30-49 cm',
                                                                                    '50-74 cm',
                                                                                    '75-99 cm', '100-149 cm',
                                                                                    '150-199 cm'
                                                                                    ],
                                                                                   ["5", "20", "40", "62", "87", "125",
                                                                                    "175"]))

## Convert column to numeric
environment['microrelief_cm'] = environment['microrelief_cm'].astype(int)
environment.microrelief_cm.describe()

# Format soil class
environment['soil_class'].value_counts()

## Rename column
environment = environment.rename(columns={"soil_class": "soil_class_original"})

## Convert to lowercase; replace 'NULL' with 'not determined'
environment['soil_class'] = np.where(environment['soil_class_original'] == 'NULL',
                                     'not determined',
                                     environment['soil_class_original'].str.lower())

## Replace 'gelaquents' with appropriate subgroup: All gelaquents are typic gelaquents
environment['soil_class'] = environment['soil_class'].str.replace('^gelaquents',
                                                                  'typic gelaquents', regex=True)

## Verify whether values match constrained values
temp = np.isin(environment['soil_class'].unique(),
               [dictionary['data_attribute']])

## Find which values are not included in the data dictionary
temp = pd.DataFrame({'is_in_dict': temp[0:84]})  # Convert to data frame
temp['soil_class'] = environment['soil_class'].unique()  # Append soil class

## Replace values that are not in the data dictionary with 'not available'
## Remaining values refer to soil orders, rather than suborders
missing_class = temp[temp['is_in_dict'] == False]
missing_class = missing_class['soil_class']
missing_class_list = missing_class.to_list()
environment['soil_class'] = environment['soil_class'].replace(missing_class_list, 'not available')

# Format cryoturbation
environment.cryoturb_ynu.value_counts()

## Replace values with appropriate boolean values
environment['cryoturbation'] = environment['cryoturb_ynu'].replace(['No', 'Yes'], ['FALSE', 'TRUE'])

## Verify that values are either TRUE or FALSE
environment['cryoturbation'].value_counts()

# Format dominant texture at 40 cm
environment['soil_dom_texture_40cm'].value_counts()
dictionary[dictionary['field'] == 'soil_texture'].data_attribute.unique()

## Convert to lowercase
environment['dominant_texture_40_cm'] = environment['soil_dom_texture_40cm'].str.lower()

## Replace values with values that match constrained values
environment['dominant_texture_40_cm'] = environment['dominant_texture_40_cm'].map({'loamy': 'loam',
                                                                                   'sandy': 'sand', 'clayey': 'clay'},
                                                                                  na_action='ignore')
environment['dominant_texture_40_cm'] = environment['dominant_texture_40_cm'].fillna('NULL')

## Verify that values were properly converted
environment['dominant_texture_40_cm'].value_counts()

# Format depth at 15% coarse fragments
# Need to confirm with ABR that this is the right column to use
environment[environment['soil_rock_depth_probe_cm'] > -999].soil_rock_depth_probe_cm.describe()

## If depth at 15% rock is much greater than depth of soil observation, convert to -999
environment['depth_15_percent_coarse_fragments_cm'] = np.where(
    (abs(environment['soil_rock_depth_probe_cm']) > abs(environment['soil_obs_maximum_depth_cm'] + 50)), -999,
    environment['soil_rock_depth_probe_cm'])

# Populate values for missing columns
environment_final = environment.assign(disturbance_severity='NULL')

# Final formatting

## Rename site visit id to site visit code (schema 2.0)
environment_final = environment_final.rename(columns={"site_visit_id": "site_visit_code"})

## Drop all columns that aren't in the template and reorder them to match data entry template
environment_final = environment_final[template.columns]

# Export dataframe
environment_final.to_csv(environment_output, index=False, encoding='UTF-8')
