# ---------------------------------------------------------------------------
# Format Soil Horizons for NPS Arctic Network 2014 data
# Author: Amanda Droghini
# Last Updated: 2025-04-25
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Soil Horizons for NPS Arctic Network 2014 data" prepares soil data recorded during vegetation
# surveys for ingestion in the AKVEG database. The script appends unique site visit identifiers, uses lookup tables
# to translate values into ones that are included in the AKVEG database dictionary, replaces empty observations with
# appropriate null values, and performs QA/QC checks. The script depends upon formatted Site Visit and Soil Horizons
# tables. The output is a CSV table that can be converted and included in
# a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import os
import numpy as np
import pandas as pd
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Define directories
drive = 'C:/'
root_folder = os.path.join(drive, 'ACCS_Work')

# Define folder structure
project_folder = os.path.join(root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation',
                              'AKVEG_Database')
credential_folder = os.path.join(project_folder, 'Credentials')
repository_folder = os.path.join(root_folder, 'Repositories', 'akveg-database')
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '35_nps_arcn_2014')
source_folder = os.path.join(plot_folder, 'source')

# Define inputs
site_visit_input = os.path.join(plot_folder, '03_sitevisit_npsarcn2014.csv')
soil_horizons_input = os.path.join(plot_folder, '14_soilhorizons_npsarcn2014.csv')
environment_input = os.path.join(source_folder, 'dbo_plot.csv')
template_input = os.path.join(project_folder, 'Data', 'Data_Entry', '12_environment.xlsx')

## Database inputs
authentication_file = os.path.join(credential_folder, 'akveg_public_read/authentication_akveg_public_read.csv')
query_file = os.path.join(repository_folder, '05_queries', 'standard', 'Query_00h_DatabaseDictionary.sql')

# Define outputs
environment_output = os.path.join(plot_folder, '12_environment_npsarcn2014.csv')

# Connect to AKVEG database
database_connection = connect_database_postgresql(authentication_file)

# Read in data
site_visit_original = pd.read_csv(site_visit_input, usecols=["site_code", "site_visit_code"])
soil_horizons_original = pd.read_csv(soil_horizons_input)
environment_original = pd.read_csv(environment_input,
                                   usecols=['Node', 'Plot', 'LS', 'LF1', 'LF2', 'LF3', 'LF4', 'MF1', 'MF2', 'Comp',
                                            'ShpVt', 'FldFrq', 'Frzn', 'WatTbl', 'Wet', 'Organic'])
template = pd.read_excel(template_input)

# Query database
## To check that values in formatted dataset correspond to values in the database
dictionary_read = open(query_file, 'r')
dictionary_query = dictionary_read.read()
dictionary_read.close()
dictionary_data = query_to_dataframe(database_connection, dictionary_query)
database_connection.close()

# Format site code
environment = environment_original.assign(plot_string=environment_original.Plot.astype(str))
environment['site_code'] = environment.Node.str.cat(environment.plot_string.str.zfill(2), sep="_")

# Obtain site visit code
## Use right join to drop 1 site (CKR-08) that is missing data
environment = pd.merge(environment, site_visit_original, how='right', left_on='site_code', right_on='site_code')

## Ensure all site codes have a site visit id
environment['site_visit_code'].isna().sum()
environment.shape[0] == site_visit_original.shape[0]

# Standardize null values
environment.isna().sum()

## Replace no data fields with appropriate null values
null_values = {"LF1": 'NULL',
               "LF2": 'NULL',
               "LF3": 'NULL',
               "LF4": 'NULL',
               "MF1": 'NULL',
               "MF2": 'NULL',
               "ShpVt": "NULL",
               "FldFrq": 'NULL',
               "Organic": -999
               }

environment = (environment.replace([999], -999)
               .fillna(value=null_values))

# Format geomorphology

## Reclassify values to match database dictionary
environment = environment.assign(geomorphology=environment.LS.str.removesuffix('s'))
conditions = [environment.geomorphology == 'river valley',
              environment.LF1 == 'barrier island',
              environment.LF1 == 'spit',
              environment.geomorphology.str.startswith('lagoon'),
              environment.geomorphology == 'lava plain',
              environment.geomorphology.str.contains('sand|dune', regex=True),
              environment.geomorphology == 'lake plain',
              environment.geomorphology == 'till plain',
              environment.geomorphology.str.contains('piedmont|upland|foothill', regex=True),
              environment.LF1.str.contains("valley"),
              environment.geomorphology.str.contains('thermokarst'),
              ]

choices = ['floodplain', 'barrier island', 'spit',
           'lagoon', 'volcanic, lava flow deposit', 'aeolian deposit, sand',
           'lacustrine deposit', 'glacial deposit', 'hill', 'valley', 'NULL']

environment.geomorphology = np.select(conditions, choices, default=environment.geomorphology)

## Check that all values are included in data dictionary
dictionary_values = dictionary_data[dictionary_data.field == 'geomorphology'].data_attribute.to_numpy()
np.isin(environment[environment.geomorphology != 'NULL'].geomorphology.unique(), dictionary_values, assume_unique=True)

# Format physiography
## Can only determine for a few sites
conditions = [environment.LS == 'river valley',
              environment.LS.str.contains('coast'),
              environment.LS.str.contains('piedmont|upland|hill', regex=True),
              environment.geomorphology == 'lacustrine deposit',
              environment.LF1 == 'lava flow'
              ]

choices = ['riverine', 'coastal', 'upland', 'lacustrine', 'volcanic']

environment = environment.assign(physiography=np.select(conditions, choices, default='NULL'))

## Check that all values are included in data dictionary
dictionary_values = dictionary_data[dictionary_data.field == 'physiography'].data_attribute.to_numpy()
np.isin(environment[environment.physiography != 'NULL'].physiography.unique(), dictionary_values, assume_unique=True)

# Format macrotopography
environment.LF1.unique()

conditions = [(environment.LF1 == 'beach ridge') | (environment.LF2 == 'beach ridge'),
              environment.LF1 == 'beach',
              environment.LF1 == 'beach plain',
              ((environment.LF2 == 'moraine') & (environment.LF1 != 'lateral moraine')) | (
                      environment.LF1 == 'moraine'),
              environment.LF1 == 'lateral moraine',
              environment.LF1 == 'end moraine',
              (environment.LF1 == 'stream terrace') | (
                      (environment.LF1 == 'flood plain') & (environment.FldFrq == 'Rare')),
              environment.LF2 == 'alluvial flat',
              (environment.LF1 == 'flood plain') & (environment.LF2 == 'point bar'),
              (environment.LF1 == 'flood plain') & (environment.LF2 == 'oxbow'),
              (environment.LF2 == 'backswamp') & (environment.FldFrq == 'Freq'),
              (environment.LF1 == 'flood plain') & ((environment.LF2 == 'NULL') | (environment.LF2 == 'muskeg') | (
                      environment.LF2 == 'patterned ground')) & (environment.FldFrq != 'Rare'),
              environment.LF1 == 'faceted spur',
              (environment.LF1 == 'ridge') | ((environment.LF1 == 'Hill') & (environment.Comp == 'crest')) | (
                      environment.LF1 == 'interfluve'),
              ((environment.LF1.str.contains('slope')) | (
                      (environment.LF1 == 'mountain') & (environment.LF2 == 'hillslope'))) | (
                      (environment.LF1.str.contains('valley')) & (environment.LF2 == 'NULL') & (
                  environment.Comp.str.contains('slope'))) & (
                      environment.ShpVt == 'Linear'),
              (environment.LF1.str.contains('slope')) | (
                      (environment.LF1.str.contains('valley')) & (environment.LF2 == 'NULL') & (
                  environment.Comp.str.contains('slope'))) & (environment.ShpVt == 'Concave'),
              (environment.LF1.str.contains('slope') & (environment.ShpVt == 'Convex')),
              (environment.LF1.str.contains('slope') & (environment.ShpVt == 'NULL')),
              (environment.LF1 == 'fen') | ((environment.LF1.str.contains('valley')) & (environment.LF2 == 'fen')),
              (environment.LS == 'dune field') | (environment.LF1.str.contains('dune')),
              environment.LF2 == 'closed depression',
              (environment.LF1 == 'peat plateau') & (environment.Organic >= 40),
              (environment.LF1 == 'peat plateau') & (environment.Organic < 40),
              (environment.LF1 == 'washover fan') | (environment.LF2 == 'washover fan'),
              (environment.LF1 == 'back-barrier flat') | (environment.LF2 == 'back-barrier flat'),
              environment.LF1 == 'maar',
              (environment.LF1 == 'Hill') & (environment.Comp == 'side slope') & (environment.ShpVt == 'Convex'),
              environment.LF1 == 'till-floored lake plain',
              environment.LF1 == 'sand sheet',
              (environment.LF1.str.contains('marsh')) | (environment.LF1 == 'creep') | (
                      environment.LF1 == 'patterned ground') | (environment.LF1 == 'valley floor') | (
                      environment.LF1 == 'lava flow') | (environment.LF1 == 'block field') | (
                      environment.LF1 == 'lake plain') | ((environment.LF1 == 'spit') & (environment.LF2 == 'NULL')) | (
                      environment.LF1 == 'NULL')
              ]

choices = ['beach ridge', 'beach', 'beach plain', 'moraine', 'moraine lateral', 'moraine terminal',
           'floodplain abandoned', 'alluvial flat', 'floodplain bar', 'floodplain oxbow', 'floodplain terrace', 'NULL',
           'spur', 'ridge', 'slope planar', 'slope concave', 'slope convex', 'slope', 'organic deposits, fen', 'dunes',
           'depression', 'organic deposits, undifferentiated', 'NULL', 'washover fan', 'back-barrier flat', 'maar',
           'slope convex', 'till-floored lake plain', 'sand sheet', 'NULL']

environment = environment.assign(macrotopography=np.select(conditions, choices, default='NULL'))

## Check that all values are included in data dictionary
dictionary_values = dictionary_data[dictionary_data.field == 'macrotopography'].data_attribute.to_numpy()
np.isin(environment[environment.macrotopography != 'NULL'].macrotopography.unique(), dictionary_values,
        assume_unique=True)

# Format microtopography
conditions = [(environment.LF1 == 'talus slope') | (environment.LF2 == 'talus slope'),
              (environment.LF1 == 'scree slope') | (environment.LF2 == 'scree slope'),
              (environment.LF1 == 'block field') | (environment.LF2 == 'block field'),
              environment.LF2 == 'thermokarst depression',
              (environment.LF2 == 'drainageway') | (environment.LF3 == 'drainageway') | (
                      environment.LF4 == 'drainageway'),
              (environment.LF2 == 'swale') | (environment.MF1 == 'swale'),
              (environment.LF1 == 'flood plain') & ((environment.Comp == 'Tread') | (environment.Comp == 'rise')),
              environment.MF1 == 'solifluction lobe',
              environment.MF1 == 'ice wedge polygons',
              environment.MF1.str.contains('polygon'),
              environment.MF1.str.contains('hummock'),
              environment.MF1 == 'frost boil',
              environment.MF1 == 'sorted circles',
              environment.MF1 == 'stripes',
              environment.MF1 == 'channel',
              environment.MF1 == 'palsa']

choices = ['talus', 'scree', 'block field', 'thermokarst', 'drainageway', 'treads and risers', 'solifluction lobes',
           'ridges and swales', 'ice wedge', 'polygonal', 'hummocks', 'frost scars and boils',
           'circles (non-sorted, sorted)',
           'stripes (non-sorted, sorted)', 'channeled', 'mounds']

environment = environment.assign(microtopography=np.select(conditions, choices, default='NULL'))

## Check that all values are included in data dictionary
dictionary_values = dictionary_data[dictionary_data.field == 'microtopography'].data_attribute.to_numpy()
np.isin(environment[environment.microtopography != 'NULL'].microtopography.unique(), dictionary_values,
        assume_unique=True)

# Format depth to water
environment = environment.assign(depth_water_cm=np.where(environment.WatTbl == -999, -999, environment.WatTbl * -1))

# Format surface water
## Limited data
environment = environment.assign(surface_water=np.where(environment.depth_water_cm > 0, True, False))

# Format depth moss/duff layer
environment = environment.rename(columns={"Organic": "depth_moss_duff_cm"})
environment.depth_moss_duff_cm.describe()

# Format depth at 15% coarse fragments
## Use information from soil horizons table

## For each site, find the first instance where total_coarse_fragments = 15
coarse_fragments = soil_horizons_original.loc[soil_horizons_original['total_coarse_fragment_percent'] >= 15]
coarse_fragments = coarse_fragments.assign(
    min_horizon=coarse_fragments.groupby('site_visit_code')['horizon_order'].transform('min'))
coarse_fragments = coarse_fragments.assign(
    keep_row=coarse_fragments['horizon_order'].eq(coarse_fragments['min_horizon']))
coarse_fragments = coarse_fragments.loc[coarse_fragments['keep_row'] == True]
coarse_fragments = coarse_fragments.drop(columns=['horizon_order', 'depth_lower', 'texture',
                                                  'total_coarse_fragment_percent', 'min_horizon', 'keep_row'])

## Merge with environment table
environment = pd.merge(environment, coarse_fragments, 'left', on='site_visit_code')

## Replace NaN with appropriate null value
environment['depth_15_percent_coarse_fragments_cm'] = environment.depth_upper.fillna(value=-999)

# Format dominant texture at 40 cm
# Use information from soil horizons table
texture_40cm = soil_horizons_original.loc[soil_horizons_original['depth_lower'] >= 40]
texture_40cm = texture_40cm.assign(
    min_horizon=texture_40cm.groupby('site_visit_code')['horizon_order'].transform('min'),
    max_horizon=texture_40cm.groupby('site_visit_code')['horizon_order'].transform('max'))

## Create conditions: If transition from one horizon to the other is at 40cm, choose the lower horizon
conditions = [(texture_40cm['horizon_order'].eq(texture_40cm['min_horizon'])) & (texture_40cm['depth_lower'] != 40),
              (texture_40cm['min_horizon'].eq(texture_40cm['max_horizon'])) & (texture_40cm['depth_lower'] == 40),
              (texture_40cm['max_horizon'].eq(texture_40cm['horizon_order'])) & (texture_40cm['depth_upper'] == 40)]
choices = [True, True, True]

texture_40cm = texture_40cm.assign(keep_row=np.select(conditions, choices, default=False))
texture_40cm = texture_40cm.loc[texture_40cm['keep_row'] == True]

## Merge with environment table
texture_40cm = texture_40cm.drop(columns=['horizon_order', 'depth_upper', 'depth_lower',
                                          'total_coarse_fragment_percent', 'min_horizon', 'keep_row'])

environment = pd.merge(environment, texture_40cm, 'left', on='site_visit_code')

## Replace NaN with appropriate null value
environment = environment.assign(dominant_texture_40_cm=environment.texture.fillna(value='NULL'))

# Format cryoturbation
environment = environment.assign(cryoturbation=np.where(environment.Frzn == -999, False, True))

# Populate remaining columns with appropriate null values
environment_final = environment.assign(moisture_regime="NULL",
                                       drainage="NULL",
                                       disturbance='NULL',
                                       disturbance_severity='NULL',
                                       disturbance_time_y=-999,
                                       depth_restrictive_layer_cm=-999,
                                       restrictive_type="NULL",
                                       microrelief_cm=-999,
                                       soil_class='not determined')

## Drop all columns that aren't in the template and reorder them to match data entry template
environment_final = environment_final[template.columns]

# QA/QC
temp = environment_final.describe(include='all')

# Export dataframe
environment_final.to_csv(environment_output, index=False, encoding='UTF-8')
