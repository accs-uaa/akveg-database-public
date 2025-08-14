# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site table for  Yukon Biophysical Inventory Data System plot data
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-06-19
# Usage: Must be executed in an ArcGIS Pro Python 3.11+ distribution.
# Description: "Format Site table for  Yukon Biophysical Inventory Data System plot data" extracts relevant
# information from an ESRI shapefile, re-projects to NAD83, drops sites with missing data, replaces values with the
# correct constrained values, and performs QC checks. The output is a CSV file that can be used to write a SQL INSERT
# statement for ingestion into the AKVEG database.
# ---------------------------------------------------------------------------

# Import packges
import arcpy
import numpy as np
import pandas as pd
from akutils import *
from pathlib import Path

# Define directories

## Define root directories
drive = Path('C:/')
root_folder = Path('ACCS_Work')

## Define folder structure
project_folder = drive / root_folder / 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '52_yukon_biophysical_2023'
source_folder = plot_folder / 'source' / 'ECLDataForAlaska_20240919' / 'YBIS_Data'

# Set workspace
workspace_gdb = drive / root_folder / 'Projects' / 'AKVEG_Map' / 'Data' / 'AKVEG_Workspace.gdb'
arcpy.env.workspace = str(workspace_gdb)  ## Convert to string; arcpy.env.workspace doesn't support pathlib

# Define files

## Define inputs
plot_original = source_folder / 'YBISPlotLocations.shp'
veg_input = source_folder / 'Veg_2024Apr09.xlsx'
template_input = project_folder / 'Data_Entry' / '02_site.xlsx'
dictionary_input = project_folder / 'Tables_Metadata' / 'database_dictionary.xlsx'

# Define intermediate datasets
plot_project = workspace_gdb / 'YBIS_2024'

## Define output
site_output = plot_folder / '02_site_yukonbiophysical2023.csv'

# Set environment options

## Set overwrite option
arcpy.env.overwriteOutput = True

## Define coordinate systems
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4269)  ## Set output coordinate system to NAD83
input_coords = arcpy.SpatialReference(4326)
transformation = 'WGS_1984_(ITRF00)_To_NAD_1983'

## Define functions
def convert_int(value):
    if pd.isna(value):  ## Check for both np.nan and None
        return -999
    try:
        return int(str(value).strip())  ## Convert to string + trim whitespace
    except ValueError:
        return -999

# Re-project data to NAD83
arcpy.management.Project(str(plot_original), str(plot_project), in_coor_system=input_coords,
                         transform_method=transformation)

# Add XY coordinates
arcpy.management.AddXY(str(plot_project))

# Read in data
site_original = geodatabase_to_dataframe(str(plot_project))  ## Convert to string; akutils doesn't support pathlib
veg_original = pd.read_excel(veg_input, usecols=["Project ID", "Plot ID", "Scientific name", "Veg cover pct"])
template = pd.read_excel(template_input)
dictionary = pd.read_excel(dictionary_input)

# Retain relevant columns
site = site_original.filter(items={'Project_ID', 'Plot_ID', 'Survey_dat', 'Plot_type_', 'POINT_X', 'POINT_Y', 'Coord_prec',
                                   'Coord_sour'})

# Drop sites without enough data
site = (site.replace(to_replace=[' ', 'UNK'], value=np.nan)  ## Replace unknowns & empty spaces with NaN
        .dropna(axis='index', how='any', subset=['POINT_X', 'POINT_Y', 'Survey_dat'])  ## Drop sites without
        # coordinates or dates
        .loc[site.Plot_type_ != 'Plot and soil information only']  ## Drop sites without vegetation data
        .dropna(axis='index', how='all', subset=['Coord_sour', 'Coord_prec']))  ## Drop sites with unknown
# coordinate precision

## Drop sites with unreliable survey dates (n=4)
site = site.assign(observe_month = pd.to_datetime(site.Survey_dat, format='%Y %b %d').dt.month)
site = site.loc[(site.observe_month >= 4) & (site.observe_month <= 10)]

# Replace null values
print(site.isna().sum())

site = site.fillna({'Plot_type_': 'unknown',
                    'Coord_prec': '-999'})

# Explore coordinates
site[["POINT_X", "POINT_Y"]].describe()  ## Values are reasonable

# Format site code

## Create combination of project + plot ID for easier retrieval/comparison later on
site = site.assign(site_code=site.Project_ID.astype(str) + '_' + site.Plot_ID.astype(str))
print(site.site_code.nunique() == site.shape[0])  ## Ensure ids are unique

# Format horizontal error
print(site.Coord_prec.value_counts())

site = site.assign(h_error_m = np.where(site['Coord_prec'] == '>100m', '200',
                                        site['Coord_prec'].str.replace(pat='[m]$', repl='', regex=True)))

site.h_error_m = site.h_error_m.apply(convert_int)

print(site.h_error_m.value_counts())

# Format plot dimensions
print(site.Plot_type_.value_counts())

## Create replacement map
replacement_map = {
    '20 m diameter': '10 radius',
    '10 m diameter': '5 radius',
    '1x1 m': '1×1',
    '10x10 m': '10×10',
    '10x5 m': '5×10',
    '20x20 m': '20×20',
    'approximately 100x100 m observed from an aircraft (size may vary)': '100×100',
    'Non-standard plot of irregular shape (e.g., trough in polygonal landscape)': 'unknown',
    'Unable to determine plot type for older data': 'unknown',
    'Trees: 10x10m; Shrubs: 5x5m; Herbs: 1x1m': '5×5',
    'Trees: 20x20m; Shrubs: 5x5m; Herbs: 1x1m': '5×5'
}

## Replace values to match constrained values in data dictionary
site['plot_dimensions_m'] = site['Plot_type_'].replace(replacement_map)

## Drop sites with unknown plot dimensions
site = site.loc[~(site.plot_dimensions_m == 'unknown')]

## Ensure all entries correspond to a value in the data dictionary
print(site.plot_dimensions_m.isin(dictionary.data_attribute).all())

# Format positional accuracy
print(site.Coord_sour.value_counts())

## Create replacement map
replacement_map = {
    'NGPS': 'consumer grade GPS',
    'DGPS': 'consumer grade GPS',
    'ARGOS': 'consumer grade GPS',
    'ORTHOPHOTO1.0': 'image interpretation',
    'DIGITIZED50K': 'map interpretation',
    'MAP50K': 'map interpretation',
    'DIGITIZED250K': 'map interpretation',
    'MAP250K': 'map interpretation',
    'MAP': 'map interpretation'
}

## Replace values to match constrained values in data dictionary
site['positional_accuracy'] = site['Coord_sour'].replace(replacement_map)

## Explore entries with 'unknown' positional accuracy
temp = site.loc[site['positional_accuracy'].isna()]
print(temp.h_error_m.value_counts())

## Correct 3 sites with unknown positional accuracy and relatively low positional error
site.loc[(site['positional_accuracy'].isna()) &
         (site['h_error_m'] <= 10), 'positional_accuracy'] = 'consumer grade GPS'

## Drop remaining sites with unknown positional accuracy and high (n=8) positional error; cannot be used for map
# development or classification
site = site.loc[~site['positional_accuracy'].isna()]

## Ensure entries correspond to a value in the data dictionary
print(site.positional_accuracy.isin(dictionary.data_attribute).all())

# Drop sites that aren't in the vegetation spreadsheet
veg_data = site.Plot_ID.isin(veg_original["Plot ID"])
site_filtered = site[veg_data].copy()

# Drop site for which all percent cover are zero
## Notes indicate this is because % cover was not recorded
temp = veg_original.groupby(['Plot ID'])['Veg cover pct'].sum()
no_cover = temp.index[temp.tolist().index(0)]
site_filtered = site_filtered.loc[~(site_filtered.Plot_ID == no_cover)]

# Populate remaining columns
site_final = (site_filtered.assign(establishing_project_code = 'yukon_biophysical_2023',
                          perspective = 'aerial',
                          cover_method = 'semi-quantitative visual estimate',
                          h_datum = 'NAD83',
                          location_type = 'targeted')
              .rename(columns={'POINT_X': 'longitude_dd',
                       'POINT_Y': 'latitude_dd'}))

# Match template formatting
site_final = site_final[template.columns]

# Export to CSV
site_final.to_csv(site_output, index=False, encoding='UTF-8')
