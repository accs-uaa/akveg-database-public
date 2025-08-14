# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Compile BLM AIM Alaska 2021 Environment Data
# Author: Timm Nawrocki, Amanda Droghini
# Last Updated: 2025-06-05
# Usage: Must be executed in an ArcGIS Pro Python 3.11+ distribution.
# Description: "Compile BLM AIM Alaska 2021 Environment Data" compiles the environment data for Alaska from BLM's TerrADat
# database and exports it to a CSV for manipulations in R. The script also appends elevation data from the Alaska DTM
# 10m Composite in case elevation values in source data are inaccurate or missing.
# ---------------------------------------------------------------------------

# Import packages
import os
import time
from akutils import *
import arcpy
from arcpy.sa import *

# Set root directory
drive = 'C:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '32_aim_various_2021')
source_folder = os.path.join(plot_folder, 'source')
spatial_folder = os.path.join(drive, root_folder, 'Projects', 'AKVEG_Map', 'Data')

# Define geodatabases
aim_gdb = os.path.join(source_folder, 'BLM AIM 2022 TerrADat.gdb')
supplemental_gdb = os.path.join(source_folder, 'AK_TAIM_MossActiveLayer_withPlotKeys_edited20211115.gdb')
regions_gdb = os.path.join(spatial_folder, 'AKVEG_Regions.gdb')
workspace_gdb = os.path.join(spatial_folder, 'AKVEG_Workspace.gdb')

# Define input datasets
plots_input = os.path.join(aim_gdb, 'tblPlots')
moisture_input = os.path.join(supplemental_gdb, 'AK_TAIM_MossActiveLayer')
suppl_input = os.path.join(supplemental_gdb, 'MossActiveLayer_Details')
area_input = os.path.join(regions_gdb, 'AlaskaYukon_MapDomain_3338')
elevation_input = os.path.join(spatial_folder, 'topography', 'Alaska_Composite_DTM_10m', 'integer', 'Elevation_10m_3338.tif')

# Define output datasets
plots_output = os.path.join(plot_folder, 'working', 'AIM_Terrestrial_Alaska_Environment.csv')
suppl_output = os.path.join(plot_folder, 'working', 'AIM_Supplemental_Moss.csv')

# Define intermediate datasets
plots_clip = os.path.join(workspace_gdb, 'AIM_Terrestrial_Clip')
plots_clip_elevation = os.path.join(workspace_gdb, 'AIM_Terrestrial_Clip_Elevation')

# Set overwrite option
arcpy.env.overwriteOutput = True

# Specify core usage
arcpy.env.parallelProcessingFactor = '0'

# Set workspace
arcpy.env.workspace = workspace_gdb

# Clip AIM plots to Alaska-Yukon Region
print('Subsetting AIM data for Alaska...')
iteration_start = time.time()
arcpy.analysis.PairwiseClip(plots_input, area_input, plots_clip)
end_timing(iteration_start)

# Extract elevation data
print('Extracting elevation data...')
ExtractValuesToPoints(plots_clip, elevation_input, plots_clip_elevation)

# Convert shapefile to CSV and retain desired columns
print('Exporting environment data...')
iteration_start = time.time()

# Read tables into dataframes
plots_data = geodatabase_to_dataframe(plots_clip_elevation)
moisture_data = geodatabase_to_dataframe(moisture_input)
suppl_data = geodatabase_to_dataframe(suppl_input)

# Format moisture data
## Drop duplicates: Some sites have 3 entries each
## Only one value needs to be retained
moisture_data = (moisture_data.drop_duplicates(subset='PlotID')
                 .dropna(subset='PlotID')
                 .filter(items=['PlotID', 'Moisture_Regime']))

# Join with plots dataframe
merged_data = plots_data.merge(moisture_data, how='outer', on="PlotID")
print(merged_data.Moisture_Regime.value_counts().sum() == moisture_data.shape[0]) ## Ensure merge worked as expected

# Select fields to retain

## Dop columns that are all NULL
columnsToDrop = merged_data.isna().sum()
columnsToDrop = columnsToDrop.loc[columnsToDrop != merged_data.shape[0]]
columnsToDrop = columnsToDrop.index.to_numpy()
merged_data = merged_data[merged_data.columns.intersection(columnsToDrop)]

suppl_data = (suppl_data.dropna(subset='MossActiveLayerPlotKey')
              .filter(items=['LineNumber', 'PointNbr', 'PointLoc',
'MossDuff_Thickness_cm',
                                    'Depth_to_Water_cm', 'Depth_Soil_to_Permafrost_cm', 'MossActiveLayerPlotKey']))

# Export dataframe to CSV
merged_data.to_csv(plots_output, index=False, encoding='UTF-8')
suppl_data.to_csv(suppl_output, index=False, encoding='UTF-8')

end_timing(iteration_start)
