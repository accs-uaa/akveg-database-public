# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Compile Alaska LPI Detail Data
# Author: Timm Nawrocki
# Last Updated: 2025-06-07
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Compile Alaska LPI Detail Data" compiles the LPI detail data for Alaska and exports it to an excel file for manipulations in R.
# ---------------------------------------------------------------------------

# Import packages
import os
import time
import pandas as pd
from akutils import *
import arcpy

# Set root directory
drive = 'C:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation',
                              'AKVEG_Database', 'Data', 'Data_Plots', '32_aim_various_2021')
data_folder = os.path.join(project_folder, 'source')
akveg_folder = os.path.join(drive, root_folder, 'Projects', 'AKVEG_Map', 'Data')

# Define geodatabases
aim_geodatabase = os.path.join(data_folder, 'BLM AIM 2022 TerrADat.gdb')
regions_geodatabase = os.path.join(akveg_folder, 'AKVEG_Regions.gdb')
workspace_geodatabase = os.path.join(akveg_folder, 'AKVEG_Workspace.gdb')

# Define input datasets
area_input = os.path.join(regions_geodatabase, 'AlaskaYukon_MapDomain_3338')
points_input = os.path.join(aim_geodatabase, 'TerrADat')
lines_input = os.path.join(aim_geodatabase, 'tblLines')
header_input = os.path.join(aim_geodatabase, 'tblLPIHeader')
detail_input = os.path.join(aim_geodatabase, 'tblLPIDetail')

# Define output datasets
data_output = os.path.join(project_folder, 'working', 'AIM_Terrestrial_Alaska_LPI.csv')

# Define intermediate datasets
points_clip = os.path.join(workspace_geodatabase, 'AIM_Terrestrial_Alaska')

# Set overwrite option
arcpy.env.overwriteOutput = True

# Specify core usage
arcpy.env.parallelProcessingFactor = '0'

# Set workspace
arcpy.env.workspace = workspace_geodatabase

# Clip AIM points to Alaska-Yukon Region
if arcpy.Exists(points_clip) == 0:
    print('Subsetting AIM data for Alaska...')
    iteration_start = time.time()
    arcpy.analysis.PairwiseClip(points_input, area_input, points_clip)
    end_timing(iteration_start)

# Join necessary data for LPI table
print('Exporting LPI data...')
iteration_start = time.time()
# Read tables into dataframes
points_data = geodatabase_to_dataframe(points_clip)
lines_data = geodatabase_to_dataframe(lines_input)
header_data = geodatabase_to_dataframe(header_input)
detail_data = geodatabase_to_dataframe(detail_input)
# Select fields to retain
points_data = points_data[['PlotKey', 'PlotID', 'ProjectName', 'Latitude_NAD83', 'Longitude_NAD83',
                           'DateEstablished', 'DateVisited']]
lines_data = lines_data[['PlotKey', 'LineKey', 'LineID', ]]
header_data = header_data[['LineKey', 'RecKey', 'FormDate', 'Observer', 'Recorder', 'DataEntry']]
detail_data = detail_data[['RecKey', 'PointLoc', 'PointNbr', 'TopCanopy', 'Lower1', 'Lower2', 'Lower3',
                           'Lower4', 'Lower5', 'Lower6', 'Lower7', 'SoilSurface', 'ChkboxTop',
                           'ChkboxLower1', 'ChkboxLower2', 'ChkboxLower3', 'ChkboxLower4',
                           'ChkboxLower5', 'ChkboxLower6', 'ChkboxLower7', 'ChkboxSoil',
                           'HeightWoody', 'HeightHerbaceous', 'SpeciesWoody', 'SpeciesHerbaceous',
                           'ChkboxWoody', 'ChkboxHerbaceous',
                           'DateLoadedInDb', 'created_date', 'last_edited_date']]
# Join lines to points
points_data = points_data.merge(lines_data, how='inner', on='PlotKey')
points_data = points_data.merge(header_data, how='inner', on='LineKey')
points_data = points_data.merge(detail_data, how='inner', on='RecKey')

# Export dataframe
points_data.to_csv(data_output, index=False, encoding='UTF-8')
end_timing(iteration_start)
