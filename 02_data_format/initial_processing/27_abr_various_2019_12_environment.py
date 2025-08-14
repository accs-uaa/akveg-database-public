# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2019 ABR Various Environment Data
# Author: Amanda Droghini
# Last Updated: 2025-05-05
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2019 ABR Various Environment Data" prepares environment data recorded during vegetation
# surveys for ingestion in the AKVEG database. The script appends unique site visit identifiers, re-classifies
# categorical values to match constraints in the AKVEG database, replaces empty observations with appropriate null
# values, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT
# statement.
# Acknowledgments: Thank you to Aaron F. Wells for helping map surface terrain values to geomorphology values.
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
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '27_abr_various_2019')
source_folder = os.path.join(plot_folder, 'source')
schema_folder = os.path.join(project_folder, 'Data')

# Define input files
site_visit_input = os.path.join(plot_folder, '03_sitevisit_abrvarious2019.csv')
environment_input = os.path.join(source_folder, 'deliverable_tnawrocki_els.xlsx')
elevation_input = os.path.join(source_folder, 'deliverable_tnawrocki_plot.xlsx')
template_input = os.path.join(schema_folder, 'Data_Entry', '12_environment.xlsx')
dictionary_input = os.path.join(schema_folder, 'Tables_Metadata', 'database_dictionary.xlsx')

# Define output files
environment_output = os.path.join(plot_folder, '12_environment_abrvarious2019.csv')

# Read in data
site_visit_original = pd.read_csv(site_visit_input)
environment_original = pd.read_excel(environment_input)
elevation_original = pd.read_excel(elevation_input)
template = pd.read_excel(template_input)
dictionary = pd.read_excel(dictionary_input)

# Drop unnecessary columns
environment_original.columns
environment = environment_original[['plot_id', 'slope_degrees', 'physiography', 'surface_terrain',
                                    'macrotopography', 'microtopography', 'soil_moisture', 'drainage',
                                    'disturbance_class', 'water_depth_cm', 'soil_surface_organic_thick_cm',
                                    'soil_restrict_layer', 'soil_restrict_depth_probe_cm', 'microrelief',
                                    'water_above_below_surf', 'soil_class', 'cryoturb_ynu', 'soil_dom_texture_40cm',
                                    'soil_rock_depth_probe_cm', 'soil_obs_maximum_depth_cm', 'site_ph_calc',
                                    'env_field_note']]
elevation = elevation_original[['plot_id', 'elevation_m']]

# Append elevation data
## Remove year suffix so that plot ids match
elevation = elevation.assign(plot_id=elevation['plot_id'].str.replace('(_|-)\\d+$', '', regex=True))
environment = pd.merge(environment, elevation, how='left', left_on='plot_id', right_on='plot_id')

# Format site code
environment = environment.assign(site_code=environment['plot_id'].str.upper())
environment = environment.assign(site_code=environment['site_code'].str.replace('-', '_'))

# Obtain site visit code
## Use right join to drop 27 sites with missing or erroneous data
site_visit = site_visit_original.filter(items=['site_code', 'site_visit_code'])
environment = pd.merge(environment, site_visit, how='right', left_on='site_code', right_on='site_code')

## Ensure all site codes have a site visit id
environment['site_visit_code'].isna().sum()
environment['plot_id'].isna().sum()
environment['site_visit_code'].equals(site_visit['site_visit_code'])

# Standardize null values

## Replace -997 and -998 values with -999
environment = environment.replace([-997, -998], [-999, -999])

## Replace NA with appropriate null values
environment.isna().sum()
null_values = {"drainage": 'NULL', "disturbance_class": 'NULL', "macrotopography": 'NULL',
               "microtopography": 'NULL', "water_depth_cm": -999, "soil_surface_organic_thick_cm": -999,
               "soil_restrict_layer": 'NULL', "soil_restrict_depth_probe_cm": -999, "microrelief": -999,
               "water_above_below_surf": 'NULL', "soil_class": 'not available', "cryoturb_ynu": 'NULL',
               "soil_dom_texture_40cm": 'NULL', "soil_rock_depth_probe_cm": -999, "soil_obs_maximum_depth_cm": -999}
environment = environment.fillna(value=null_values)

# Format physiography
## All terms match constrained values in the AKVEG database
environment = environment.assign(physiography=environment['physiography'].str.lower())

## Reclassify a few sites based on expert recommendation
conditions = [(environment['surface_terrain'] == 'Eolian Active Sand Dune') |
              (environment['surface_terrain'] == 'Eolian Inactive Sand Dune') &
              (environment['physiography'] == 'riverine'),
              (environment['surface_terrain'] == 'Headwater Active Overbank Deposit') |
              (environment['surface_terrain'] == 'Headwater Inactive Overbank Deposit'),
              (environment['surface_terrain'] == 'Headwater Stream or Floodplain') &
              (environment['site_visit_code'] != 'ANIA_T08_03_20140718'),
              environment['surface_terrain'].str.startswith('Lowland Headwater Floodplain'),
              (environment['surface_terrain'] == 'Meander Abandoned Overbank Deposit') &
              (environment['physiography'] == 'riverine'),
              (environment['surface_terrain'] == 'Meander Fine Abandoned Channel Deposit') &
              (environment['physiography'] == 'riverine'),
              (environment['surface_terrain'] == 'Moderately Steep Headwater Floodplain') &
              (environment['physiography'] == 'alpine')]

choices = ['upland', 'riverine', 'riverine', 'riverine', 'lowland', 'lowland', 'riverine']
environment['physiography'] = np.select(conditions, choices, default=environment['physiography'])

environment['physiography'].value_counts()

# Format geomorphology
environment['surface_terrain'].value_counts()

conditions = [environment['site_code'] == 'SISA_T2_022',
              environment['surface_terrain'] == 'Abandoned Marine Beach',
              environment['surface_terrain'] == 'Active Gravelly Marine Beach',
              (environment['surface_terrain'] == 'Active Marine Beach') & (environment['physiography'] == 'coastal'),
              (environment['surface_terrain'] == 'Active Marine Beach') & (environment['physiography'] != 'coastal'),
              (environment['surface_terrain'] == 'Active Sandy Marine Beach') |
              (environment['surface_terrain'] == 'Active Tidal Flat'),
              (environment['surface_terrain'] == 'Alluvial Fan') & (environment['site_code'] == 'NPRA_T400_06'),
              (environment['surface_terrain'] == 'Alluvial Fan') & (environment['site_code'] != 'NPRA_T400_06'),
              (environment['surface_terrain'] == 'Alluvial Fan Abandoned Deposit') & (environment['slope_degrees'] < 3),
              (environment['surface_terrain'] == 'Alluvial Fan Abandoned Deposit') & (
                      environment['slope_degrees'] >= 3),
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
              (environment['surface_terrain'].str.startswith('Ancient Marine')),
              environment['surface_terrain'] == 'Avalanche Deposit, non-volcanic',
              environment['surface_terrain'] == 'Bay',
              (environment['surface_terrain'].str.startswith('Bedrock')) & (
                      environment['macrotopography'] == 'Plateau'),
              (environment['surface_terrain'].str.startswith('Bedrock')) & (environment['elevation_m'] < 300),
              (environment['surface_terrain'].str.startswith('Bedrock')) & (environment['elevation_m'] >= 300),
              environment['surface_terrain'].str.startswith('Brackish Shallow Lake'),
              ((environment['surface_terrain'].str.contains('Abandoned Channel Deposit')) |
               (environment['surface_terrain'].str.contains('Abandoned Overbank Deposit'))) &
              (~environment['surface_terrain'].str.contains('Delta')),
              ((environment['surface_terrain'].str.contains('Active Channel')) |
               (environment['surface_terrain'].str.contains('Inactive Channel')) |
               (environment['surface_terrain'] == 'Undifferentiated Channel Deposit')) &
              (~environment['surface_terrain'].str.contains('Delta')),
              ((environment['surface_terrain'].str.contains('Active Overbank Deposit')) |
               (environment['surface_terrain'].str.contains('Inactive Overbank Deposit')) |
               (environment['surface_terrain'].str.contains('Active Deposit')) |
               (environment['surface_terrain'].str.contains('Inactive Deposit')) |
               (environment['surface_terrain'] == 'Alluvial Fan Overbank Deposit')) &
              ((~environment['surface_terrain'].str.contains('Delta')) &
               (environment['surface_terrain'] != 'Undifferentiated Fine Active Overbank Deposit')),
              (environment['surface_terrain'] == 'Coastal Active Dune') |
              (environment['surface_terrain'] == 'Coastal Inactive Dune'),
              environment['surface_terrain'] == 'Coastal Lagoon Deposit',
              environment['surface_terrain'].str.startswith('Colluvial'),
              (environment['surface_terrain'].str.contains('Lake')) &
              (~environment['surface_terrain'].str.contains('Brackish')) &
              (~environment['surface_terrain'].str.contains('Basin')) &
              (~environment['surface_terrain'].str.contains('Bottom')),
              environment['surface_terrain'] == 'Delta Abandoned Overbank Deposit',
              environment['surface_terrain'].str.startswith('Delta'),
              (environment['surface_terrain'].str.contains('Basin, ice-poor')),
              (environment['surface_terrain'] == 'Drained Basin, ice-rich center') |
              (environment['surface_terrain'].str.startswith('Drained Lake Basin, ice-rich')) |
              (environment['surface_terrain'] == 'Drained Lake Basin, pingo'),
              environment['surface_terrain'] == 'Emerged Estuarine Marine Deposit',
              environment['surface_terrain'].str.startswith('Eolian'),
              environment['surface_terrain'] == 'Fluvial Deposit, undifferentiated',
              environment['surface_terrain'] == 'Frozen Upland Silt',
              environment['surface_terrain'] == 'Glacial Deposit',
              (environment['surface_terrain'] == 'Glacier') & (environment['elevation_m'] < 100),
              (environment['surface_terrain'] == 'Glacier') & (environment['elevation_m'] > 500),
              (environment['surface_terrain'].str.contains('Glaciofluvial')) &
              (~environment['surface_terrain'].str.contains('active', case=False)),
              (environment['surface_terrain'] == 'Glaciofluvial Outwash Active-cover Deposit') |
              (environment['surface_terrain'] == 'Glaciofluvial Outwash Inactive-cover Deposit'),
              environment['surface_terrain'].str.startswith('Glaciolacustrine'),
              environment['surface_terrain'] == 'Glaciomarine Deposit',
              environment['site_visit_code'] == 'ANIA_T08_03_20140718',
              environment['surface_terrain'].str.contains('Floodplain'),
              environment['surface_terrain'] == 'Hillside Colluvium',
              environment['surface_terrain'].str.startswith('Igneous'),
              environment['surface_terrain'] == 'Ignimbrite Deposit, non-welded',
              (environment['surface_terrain'].str.startswith('Inactive')),
              (environment['surface_terrain'].str.startswith('Lacustrine Beach')) |
              (environment['surface_terrain'] == 'Lacustrine, riverine waterbody sediments'),
              (environment['surface_terrain'] == 'Lacustrine Deposit') &
              (environment['macrotopography'].str.startswith('Basin')),
              (environment['surface_terrain'] == 'Lacustrine Deposit') &
              (~environment['macrotopography'].str.startswith('Basin')),
              environment['surface_terrain'] == 'Lagoon',
              environment['surface_terrain'] == 'Landslide Deposit',
              environment['surface_terrain'] == 'Lava Flow Deposit, mafic',
              environment['surface_terrain'].str.contains('Loess'),
              (environment['surface_terrain'].str.contains('River')) &
              (~environment['surface_terrain'].str.contains('riverine', case=False)),
              environment['surface_terrain'].str.contains('Retransported Deposit'),
              environment['surface_terrain'].str.startswith('Marine Terrace'),
              environment['surface_terrain'] == 'Metamorphic, noncarbonate',
              (environment['surface_terrain'].str.startswith('Moraine')) |
              (environment['surface_terrain'].str.contains('Till')),
              environment['surface_terrain'] == 'Mountain Headwater Stream',
              environment['surface_terrain'] == 'Nearshore Water',
              environment['surface_terrain'] == 'Net',
              (environment['surface_terrain'] == 'Old Alluvial Fan') & (environment['slope_degrees'] < 3),
              (environment['surface_terrain'] == 'Old Alluvial Fan') & (environment['slope_degrees'] >= 3),
              (environment['surface_terrain'] == 'Organic Deposit') &
              (environment['macrotopography'].str.startswith('Basin')),
              environment['surface_terrain'] == 'Peat Margin Swamp',
              environment['surface_terrain'].str.startswith('Pyroclastic Flow Deposit'),
              (environment['surface_terrain'] == 'Schist') |
              (environment['surface_terrain'].str.startswith('Sedimentary Bedrock')) |
              (environment['surface_terrain'] == 'Sedimentary, sandstone'),
              environment['surface_terrain'] == 'Sedimentary, noncarbonate',
              (environment['surface_terrain'] == 'Slump Deposit') |
              (environment['surface_terrain'] == 'Solifluction Deposit'),
              environment['surface_terrain'] == 'Spring',
              environment['surface_terrain'].str.startswith('Talus'),
              environment['surface_terrain'].str.startswith('Tephra'),
              (environment['surface_terrain'].str.startswith('Thaw Basin, ice-rich')) |
              (environment['surface_terrain'] == 'Thaw Basin, pingo'),
              environment['surface_terrain'] == 'Thaw Basins and Thaw Lakes',
              (environment['surface_terrain'].str.contains('Tidal Flat')) |
              (environment['surface_terrain'] == 'Tidal Gut') |
              (environment['surface_terrain'] == 'Undifferentiated Fine Active Overbank Deposit'),
              environment['surface_terrain'] == 'Volcanic Vent, undifferentiated',
              environment['surface_terrain'] == 'Water',

              ]
choices = ['NULL',
           'marine, terrace', 'marine, shore', 'marine, shore', 'marine, terrace', 'marine, shore', 'alluvial plain',
           'hill', 'alluvial plain', 'hill', 'alluvial plain', 'floodplain', 'marine, shore',
           'fluviomarine terrace', 'fluviomarine terrace', 'marine, terrace', 'colluvial deposit', 'marine, bay',
           'plateau', 'hill', 'mountain', 'aquatic, lake brackish', 'alluvial plain', 'floodplain', 'floodplain',
           'aeolian deposit, sand',
           'marine, shore', 'colluvial deposit', 'aquatic, lake',
           'fluviomarine terrace', 'delta', 'basin, ice-poor', 'basin, ice-rich', 'marine, terrace',
           'aeolian deposit, sand', 'alluvial plain', 'aeolian deposit, silt',
           'glacial deposit', 'valley, lowland', 'mountain', 'glaciofluvial deposit', 'floodplain',
           'glaciolacustrine deposit', 'marine, shore', 'headwater stream',
           'floodplain', 'colluvial deposit', 'volcanic, lava flow deposit',
           'volcanic, pyroclastic flow deposit',
           'marine, shore', 'lacustrine deposit', 'basin, ice-poor',
           'lacustrine deposit', 'lagoon',
           'colluvial deposit', 'volcanic, lava flow deposit',
           'aeolian deposit, silt', 'aquatic, river', 'colluvial deposit',
           'marine, terrace', 'mountain', 'glacial deposit',
           'headwater stream', 'marine, ocean', 'basin, ice-poor',
           'alluvial plain', 'hill', 'basin, ice-poor', 'basin, ice-poor', 'volcanic, pyroclastic flow deposit',
           'mountain', 'hill', 'colluvial deposit', 'headwater stream', 'mountain',
           'volcanic, tephra', 'basin, ice-rich', 'basin, ice-poor', 'marine, shore',
           'volcano', 'aquatic, lake']

environment['geomorphology'] = np.select(conditions, choices, default='NULL')

# Format macrotopography

## Rename column to prevent overwrite
environment = environment.rename(columns={"macrotopography": "macro_original"})

## Examine existing values
environment['macro_original'].value_counts()

conditions = [environment['site_code'] == 'SISA_T2_022',
              environment['site_code'] == 'ALAG_T05_02',
              environment['site_code'] == 'ANIA_T07_06',
              (environment['surface_terrain'] == 'Lacustrine Deposit') &
              (environment['macro_original'].str.contains('Lake Margin')) &
              (environment['soil_surface_organic_thick_cm'] >= 20),
              (environment['surface_terrain'] == 'Organic Deposit') &
              (environment['site_ph_calc'] > 5.5),
              (environment['surface_terrain'] == 'Organic Deposit') &
              (environment['site_ph_calc'] <= 5.5),
              environment['surface_terrain'].str.contains('Bog'),
              (environment['surface_terrain'].str.contains('Fen')) |
              (environment['surface_terrain'].str.startswith('Igneous Intrusive')),
              (((environment['macro_original'] == 'Marine Beach')) |
               (environment['surface_terrain'].str.startswith('Lacustrine Beach')) |
               ((environment['surface_terrain'].str.contains('Active', case=False)) &
                (environment['surface_terrain'].str.contains('Beach')))) &
              (environment['surface_terrain'] != 'Active Tidal Flat'),
              (environment['macro_original'] == 'Dome-Shaped') |
              (environment['macro_original'].str.contains('Dune', case=True)) |
              (environment['surface_terrain'] == 'Coastal Active Dune') |
              (environment['surface_terrain'] == 'Coastal Inactive Dune'),
              (environment['surface_terrain'].str.startswith('Eolian')) &
              (environment['surface_terrain'].str.contains('Dune')),
              (environment['macro_original'].str.startswith('Moraine') |
               environment['surface_terrain'].str.startswith('Moraine')) &
              (environment['macro_original'] != 'Lake Margins'),
              environment['surface_terrain'] == 'Nearshore Water',
              (environment['surface_terrain'].str.startswith('Alluvial Fan')) &
              (environment['surface_terrain'].str.contains('Channel')),
              (environment['surface_terrain'].str.startswith('Alluvial')) &
              (environment['macro_original'] == 'Flat or fluvial related'),
              (environment['surface_terrain'].str.startswith('Alluvial Fan')),
              (environment['surface_terrain'].str.contains('Alluvial Terrace')) |
              (environment['surface_terrain'] == 'Fluvial Deposit, undifferentiated'),
              ((environment['surface_terrain'].str.contains('Abandoned Channel Deposit')) |
               (environment['surface_terrain'].str.contains('Abandoned Overbank Deposit'))) &
              (~environment['surface_terrain'].str.startswith('Delta')),
              (environment['surface_terrain'].str.contains('Channel Deposit') &
               (~environment['surface_terrain'].str.contains('Abandoned')) &
               (~environment['macro_original'].str.contains('Bar'))),
              (environment['surface_terrain'].str.contains('Tidal Flat')) |
              (environment['surface_terrain'] == 'Tidal Gut') |
              (environment['surface_terrain'] == 'Glaciomarine Deposit') |
              (environment['surface_terrain'] == 'Coastal Lagoon Deposit'),
              (environment['macro_original'].str.contains('Bar')) &
              ((environment['surface_terrain'].str.contains('Channel') |
                (environment['surface_terrain'].str.contains('Overbank'))) &
               (~environment['surface_terrain'].str.contains('Abandoned'))),
              (environment['macro_original'].str.startswith('Basin')) |
              (environment['macro_original'].str.contains('Drain|Depression', regex=True)),
              environment['macro_original'].str.contains('Bluff'),
              ((environment['macro_original'] == 'Braided Channels and Bars') |
               (environment['macro_original'] == 'Channel')) &
              ((~environment['surface_terrain'].str.contains('Abandoned')) &
               (~environment['surface_terrain'].str.contains('Bog')) &
               (~environment['surface_terrain'].str.contains('Fen')) &
               (~environment['surface_terrain'].str.contains('Lake'))),
              ((environment['macro_original'].str.startswith('Cliff')) |
               (environment['macro_original'] == 'Crest')) &
              (~environment['surface_terrain'].str.contains('Active Sand Dune', case=False)),
              (environment['macro_original'] == 'Flood Basin') &
              (~environment['surface_terrain'].str.contains('Abandoned')) &
              (~environment['surface_terrain'].str.contains('Bog')) &
              (~environment['surface_terrain'].str.contains('Fen')),
              ((environment['macro_original'] == 'Floodplain Step') |
               (environment['macro_original'] == 'Levee') |
               (environment['macro_original'] == 'Tread')) &
              (~environment['surface_terrain'].str.contains('Abandoned') &
               ((environment['geomorphology'] == 'floodplain') |
                (environment['geomorphology'] == 'delta'))),
              environment['surface_terrain'] == 'Glacier',
              ((environment['geomorphology'] == 'aquatic, lake')) &
              (~environment['macro_original'].str.contains('Basin')),
              ((environment['surface_terrain'].str.contains('Lacustrine'))) &
              (environment['macro_original'].str.contains('Lake')),
              (environment['macro_original'] == 'Toeslope'),
              (environment['macro_original'].str.contains('Slope')) &
              (environment['macro_original'].str.contains('Plan')),
              (environment['macro_original'].str.contains('Slope')) &
              (environment['macro_original'].str.contains('Concave')),
              (environment['macro_original'].str.contains('Slope')) &
              (environment['macro_original'].str.contains('Convex')),
              (environment['macro_original'].str.contains('Slope', case=False)) &
              (~environment['macro_original'].str.contains('Plan|Concave|Convex', regex=True)),
              environment['macro_original'] == 'Pingo',
              environment['macro_original'] == 'Ridge And Swale',
              environment['macro_original'] == 'Shoulder',
              (environment['macro_original'].str.contains('Lake Margin')),
              environment['macro_original'] == 'Undulating',
              (environment['macro_original'] == 'Waterbodies') &
              (environment['geomorphology'].str.startswith('aquatic, lake')),
              environment['macro_original'] == 'Wave Cut Bench'
              ]

choices = ['NULL', 'organic deposits, bog', 'organic deposits, fen', 'organic deposits, fen', 'organic deposits, fen',
           'organic deposits, bog', 'organic deposits, bog',
           'organic deposits, fen',
           'beach', 'dunes',
           'dunes', 'moraine', 'nearshore zone',
           'floodplain channel deposit',
           'alluvial flat', 'alluvial fan', 'alluvial terrace',
           'floodplain abandoned', 'floodplain channel deposit',
           'tidal flat', 'floodplain bar', 'depression', 'bluff', 'floodplain channel deposit', 'ridge',
           'floodplain basin', 'floodplain terrace', 'glacier', 'lake shore', 'lake shore',
           'toeslope',
           'slope planar',
           'slope concave',
           'slope convex', 'slope', 'pingo', 'depression', 'shoulder', 'lake shore',
           'undulating', 'lake shore', 'bench'
           ]

environment['macrotopography'] = np.select(conditions, choices, default='NULL')

## Ensure that values match constrained values
environment['macrotopography'].value_counts()

## Explore remaining null values
temp = environment[environment['macrotopography'] == 'NULL']
temp['macro_original'].value_counts()

# Format microtopography

## Rename column to prevent overwrite
environment = environment.rename(columns={"microtopography": "micro_original"})

## Examine existing values
environment['micro_original'].value_counts()

conditions = [environment['site_code'] == 'SISA_T2_022',
              environment['surface_terrain'].str.startswith('Talus'),
              environment['macro_original'] == 'Human modifed slope',
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
              environment['micro_original'].str.contains('Polygon'),
              environment['macro_original'] == 'Ridge And Swale',
              (environment['macro_original'] == 'Drainage'),
              environment['micro_original'] == 'Mixed pits and polygons'
              ]

choices = ['NULL', 'talus', 'anthroscape', 'hummocks', 'mounds', 'peat mounds', 'peat mounds', 'mounds caused by trees',
           'mounds caused by wildlife',
           'soil-covered rocks',
           'boulder field', 'gelifluction lobes', 'outcrops', 'circles (non-sorted, sorted)', 'ice-cored mounds',
           'water tracks', 'stripes (non-sorted, sorted)', 'steps (non-sorted, sorted)', 'nets (non-sorted, sorted)',
           'ice wedge', 'polygonal', 'ridges and swales', 'drainageway', 'patterned ground']

environment['microtopography'] = np.select(conditions, choices, default='NULL')

## Ensure that values match constrained values
environment['microtopography'].value_counts()

## Explore remaining null values
temp = environment[environment['microtopography'] == 'NULL']
temp['micro_original'].value_counts()

# Format moisture regime
environment['soil_moisture'].value_counts()
conditions = [environment['site_code'] == 'GAAR_T98_06',
              (environment['soil_moisture'] == 'Wet') &
              (environment['drainage'].str.contains('Very poorly drained|Flooded', regex=True)),
              environment['soil_moisture'] == 'Wet',
              environment['soil_moisture'] == 'Moist',
              environment['soil_moisture'] == 'Dry',
              environment['soil_moisture'] == 'Aquatic']
choices = ['NULL', 'hydric', 'hygric', 'mesic', 'xeric', 'aquatic']
environment['moisture_regime'] = np.select(conditions, choices, default='NULL')

## Confirm results
environment['moisture_regime'].value_counts()
## Compare number of NULL values with no data entries in original table. Add 1 for erroneous site
sum(map(lambda i: i in ['No Data', 'Not Assessed'], environment['soil_moisture']))

# Format drainage
environment['drainage'].value_counts()

conditions = [environment['site_code'] == 'GAAR_T98_06',
              (environment['soil_moisture'] == 'Aquatic') & (environment['soil_class'] == 'Water'),
              (environment['soil_moisture'] == 'Aquatic') & (environment['soil_dom_texture_40cm'] == 'Water'),
              environment['drainage'] == 'Flooded',
              environment['drainage'] == 'Well drained',
              environment['drainage'].str.contains('excessively', case=False),
              environment['drainage'].str.contains('Moderately well drained|Somewhat poorly drained', regex=True),
              environment['drainage'].str.contains('Very poorly drained|Poorly drained', regex=True)]
choices = ['NULL', 'aquatic', 'aquatic', 'flooded', 'well drained', 'well drained',
           'moderately drained', 'poorly drained']

environment['drainage'] = np.select(conditions, choices, default='NULL')

## Ensure that combination of moisture regime & drainage values make sense
temp = pd.crosstab(environment['moisture_regime'], environment['drainage'])

# Format disturbance
environment['disturbance_class'].value_counts()

conditions = [environment['disturbance_class'].str.startswith('Absent'),
              environment['disturbance_class'].str.startswith('Fluvial'),
              environment['disturbance_class'] == 'Thermokarst',
              environment['disturbance_class'].str.startswith('Eolian'),
              environment['disturbance_class'] == 'Wind',
              environment['disturbance_class'] == 'Fire',
              environment['disturbance_class'].str.contains(
                  'Geomorphic|Landslide|Colluvial|Glacial|Lacustrine \\(drainage\\)', regex=True),
              environment['disturbance_class'] == 'Marine',
              environment['disturbance_class'] == 'Mammal excavations',
              environment['disturbance_class'] == 'Animals, Wildlife',
              environment['disturbance_class'] == 'Beaver Impacts',
              environment['disturbance_class'] == 'Undifferentiated Trail',
              environment['disturbance_class'] == 'Wheeled Vehicle Trail',
              environment['disturbance_class'] == 'Campsite',
              environment['disturbance_class'] == 'Snow Avalanche',
              environment['site_code'] == 'CHCO_T31_06',
              environment['site_code'] == 'NPRA_T155_06']

choices = ['none', 'riparian', 'permafrost dynamics', 'aeolian process', 'aeolian process',
           'fire', 'geomorphic process', 'tidal', 'wildlife digging', 'wildlife trampling', 'wildlife beaver',
           'trail', 'ATV use',
           'structure', 'weather process', 'road', 'historic anthropogenic']

environment['disturbance'] = np.select(conditions, choices, default='NULL')

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
temp = temp[temp['water_above_below_surf'] != 'No Data']

temp = temp.assign(water_depth_boolean=np.where(temp['water_depth_cm'] < 0, 'negative', 'positive'))
pd.crosstab(temp['water_depth_boolean'], temp['water_above_below_surf'])

## Two sites with inconsistent data. Values for site WRST_T48_03 should be changed to NULL - Not enough information
# to determine what is going on. Water depth value for WRST_T64_06 is likely correct (+200 cm, site is listed as a
# kettle lake), but surface water should be changed to True.

## Rename column
environment = environment.rename(columns={"water_depth_cm": "depth_water_cm"})

## Correct site with suspicious value
environment.loc[environment['site_code'] == 'WRST_T48_03', 'depth_water_cm'] = -999

## Verify that value was properly converted
environment[environment['site_code'] == 'WRST_T48_03'].depth_water_cm

# Format surface water
environment['water_above_below_surf'].value_counts()

conditions = [environment['site_code'] == 'WRST_T48_03',
              environment['site_code'] == 'WRST_T64_06',
              environment['water_above_below_surf'] == 'Above',
              environment['water_above_below_surf'] == 'Below']
choices = ['NULL', 'TRUE', 'TRUE', 'FALSE']
environment['surface_water'] = np.select(conditions, choices, default='NULL')

## Ensure that values were properly converted
environment['surface_water'].value_counts()

# Format depth moss duff
environment[environment['soil_surface_organic_thick_cm'] > -999].soil_surface_organic_thick_cm.describe()

## Correct 999 to -999
environment['depth_moss_duff_cm'] = np.where(environment['soil_surface_organic_thick_cm'] == 999, -999,
                                             environment['soil_surface_organic_thick_cm'])

## Verify values
environment[environment['depth_moss_duff_cm'] > -999].depth_moss_duff_cm.describe()

# Format depth restrictive layer
environment[environment['soil_restrict_depth_probe_cm'] > -999].soil_restrict_depth_probe_cm.describe()

## Rename column. No other changes needed.
environment = environment.rename(columns={"soil_restrict_depth_probe_cm": "depth_restrictive_layer_cm"})

# Format restrictive type
environment.soil_restrict_layer.value_counts().sum()
environment['soil_restrict_layer'].isna().sum()

## Convert string to lowercase
environment['restrictive_type'] = environment['soil_restrict_layer'].str.lower()

## Replace values that don't correspond to constrained values
environment['restrictive_type'] = environment['restrictive_type'].replace(['null', 'no data', 'densic materials',
                                                                           'not assessed'],
                                                                          ['NULL', 'NULL', 'densic layer',
                                                                           'NULL'])

## Ensure all values (except NULL) correspond with a constrained value
environment.restrictive_type.value_counts()
np.isin(environment.restrictive_type.unique(), [dictionary['data_attribute']])

# Format microrelief

## Explore values
environment.microrelief.value_counts()

## Convert to numeric. Choose midpoint of range as value to replace with
environment = environment.assign(
    microrelief_cm=environment.microrelief.replace(['<10 cm', '10-29 cm', '30-49 cm', '50-74 cm',
                                                    '75-99 cm', '100-149 cm', '150-199 cm',
                                                    '>=200 cm', 'Not Assessed', 'No Data'],
                                                   ["5", "20", "40", "62", "87", "125", "175", "200", "-999", "-999"]))

## Convert column to numeric
environment['microrelief_cm'] = environment['microrelief_cm'].astype(int)
environment.microrelief_cm.describe()

# Format soil class
environment['soil_class'].value_counts()

## Convert to lowercase
environment['soil_class'] = environment['soil_class'].str.lower()

## Replace 'water' with 'not available' ('water' is not a constrained value)
environment['soil_class'] = environment['soil_class'].replace(['water'], ['not available'])

## Verify whether values match constrained values
temp = np.isin(environment['soil_class'].unique(),
               [dictionary['data_attribute']])
len(temp[temp == False])

# Format cryoturbation
environment.cryoturb_ynu.value_counts()

## Replace values with appropriate boolean values
environment['cryoturbation'] = environment['cryoturb_ynu'].replace(['No', 'Unknown', 'NULL', 'Yes'],
                                                                   ['FALSE', 'NULL', 'NULL', 'TRUE'])

## Verify that values are either TRUE, FALSE, or NULL
environment['cryoturbation'].value_counts()

# Format dominant texture at 40 cm
environment['soil_dom_texture_40cm'].unique()
dictionary[dictionary['field'] == 'soil_texture'].data_attribute.unique()

## Convert to lowercase
environment['dominant_texture_40_cm'] = environment['soil_dom_texture_40cm'].str.lower()

## Replace values with values that match constrained values
environment['dominant_texture_40_cm'] = environment['dominant_texture_40_cm'].map(
    {'loamy': 'loam', 'sandy': 'sand', 'clayey': 'clay'}, na_action='ignore')
environment['dominant_texture_40_cm'] = environment['dominant_texture_40_cm'].fillna('NULL')

## Verify that values were properly converted
environment['dominant_texture_40_cm'].value_counts()

# Populate values for missing columns
environment_final = environment.assign(disturbance_severity='NULL',
                                       depth_15_percent_coarse_fragments_cm=-999)

# Final formatting
## Drop all columns that aren't in the template and reorder them to match data entry template
environment_final = environment_final[template.columns]

# Export dataframe
environment_final.to_csv(environment_output, index=False, encoding='UTF-8')
