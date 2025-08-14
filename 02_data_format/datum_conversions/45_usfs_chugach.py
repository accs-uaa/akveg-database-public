# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Obtain coordinates from shapefiles
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2024-05-09
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Obtain coordinates from shapefiles" reads in original shapefiles from the USFS Chugach project geodatabase, removes NULL entries, re-projects to NAD83, appends XY data, and exports files as CSV. Export files are used to inform usfs_cordova_2021, usfs_glacier_2023, and usfs_kenai_2019 datasets.
# ---------------------------------------------------------------------------

# Import packages
import os
import arcpy
from akutils import *

# Set root directory
drive = 'C:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
data_folder = os.path.join(project_folder, 'Data/Data_Plots/45_usfs_chugach_2022')
akveg_folder = os.path.join(drive, root_folder, 'Projects/AKVEG_Database')

# Define input geodatabase
input_gdb = os.path.join(data_folder, 'source/CNF_NRIS_Plots_20232112.gdb')

# Set workspace
workspace_gdb = os.path.join(akveg_folder, 'AKVEG_Workspace.gdb')
arcpy.env.workspace = workspace_gdb

# Define input datasets
glacier_input = os.path.join(input_gdb, 'CRD_GRD_PLOTS')

# Define output datasets
glacier_output = os.path.join(data_folder, 'working/site_glacier_cordova_coordinates.csv')

# Define intermediate datasets
glacier_features = os.path.join(workspace_gdb, 'site_glacier_cordova')

# Set overwrite option
arcpy.env.overwriteOutput = True

# Set output coordinate system to NAD83
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4269)

# Drop rows with no site ID in the glacier_cordova shapefile
# These are the Kenai sites that seem to have been entered improperly
site_query = ' "SITE_ID_1" IS NOT NULL '
glacier_layer = "gla_cor_lyr"
arcpy.management.MakeFeatureLayer(in_features=glacier_input,
                                  out_layer=glacier_layer,
                                  where_clause=site_query)

# Copy data to preserve original dataset
arcpy.management.CopyFeatures(glacier_layer, glacier_features)

# Add XY coordinates to copy features
arcpy.AddXY_management(glacier_features)

# Read tables into dataframes
glacier_data = geodatabase_to_dataframe(glacier_features)

# Export dataframes
glacier_data.to_csv(glacier_output, index=False, encoding='UTF-8')