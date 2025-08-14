# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Compile Alaska Soil Pit Horizons Data
# Author: Timm Nawrocki, Amanda Droghini
# Last Updated: 2024-09-04
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Compile Alaska Soil Pit Horizons" compiles the soils pit horizons data for Alaska and exports it to a CSV file for manipulations in R.
# ---------------------------------------------------------------------------

# Import packages
import os
import time
from akutils import *
import arcpy

# Set root directory
drive = 'C:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '32_aim_various_2021')
source_folder = os.path.join(plot_folder, 'source')
output_folder = os.path.join(plot_folder, 'working')
spatial_data_folder = os.path.join(drive, root_folder, 'Projects', 'AKVEG_Map', 'Data')

# Define geodatabases
aim_geodatabase = os.path.join(source_folder, 'BLM AIM 2022 TerrADat.gdb')
regions_geodatabase = os.path.join(spatial_data_folder, 'AKVEG_Regions.gdb')
workspace_geodatabase = os.path.join(spatial_data_folder, 'AKVEG_Workspace.gdb')

# Define input datasets
area_input = os.path.join(regions_geodatabase, 'AlaskaYukon_MapDomain_3338')
points_input = os.path.join(aim_geodatabase, 'TerrADat')
soils_input = os.path.join(aim_geodatabase, 'tblSoilPitHorizons')

# Define output datasets
data_output = os.path.join(output_folder, 'AIM_Terrestrial_Alaska_Soils.csv')

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
print('Exporting Soils data...')
iteration_start = time.time()
# Read tables into dataframes
points_data = geodatabase_to_dataframe(points_clip)
soils_data = geodatabase_to_dataframe(soils_input)

# Select fields to retain
points_data = points_data[['PrimaryKey', 'PlotID', 'ProjectName', 'Latitude_NAD83', 'Longitude_NAD83',
                           'DateEstablished', 'DateVisited']]
soils_data = soils_data[['PrimaryKey', 'HorizonDepthUpper','HorizonDepthLower','DepthMeasure','ESD_Horizon','ESD_HorizonModifier',
                         'Texture','ESD_PctClay','RockFragments','ESD_FragmentType','ESD_FragVolPct','ESD_FragmentType2','ESD_FragVolPct2',
                         'ESD_FragmentType3','ESD_FragVolPct3','ESD_Structure','ESD_Hue','ESD_Value','ESD_Chroma','ESD_EC','ESD_pH']]

# Join soils data to points
points_data = points_data.merge(soils_data, how='inner', on='PrimaryKey')

# Export dataframe
points_data.to_csv(data_output, index=False, encoding='UTF-8')
end_timing(iteration_start)
