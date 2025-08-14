# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Obtain coordinates from shapefiles
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2024-05-09
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Obtain coordinates from shapefiles" reads in original shapefiles from the USFS Chugach project geodatabase, removes NULL entries, re-projects to NAD83, appends XY data, and exports files as CSV. The output is used to inform ingestion of the usfs_kenai_2019 dataset into the AKVEG database.
# ---------------------------------------------------------------------------

# Import packages
import os
import arcpy
from akutils import *
import pandas

# Set root directory
drive = 'C:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
data_folder = os.path.join(project_folder, 'Data/Data_Plots/47_usfs_kenai_2019')
akveg_folder = os.path.join(drive, root_folder, 'Projects/AKVEG_Database')

# Define input geodatabase
input_gdb = os.path.join(data_folder, 'source/CNF_NRIS_Plots_20232112.gdb')

# Set workspace
workspace_gdb = os.path.join(akveg_folder, 'AKVEG_Workspace.gdb')
arcpy.env.workspace = workspace_gdb

# Define input datasets
kenai_input = os.path.join(input_gdb, 'SRD_Kenai_PLOTS')

# Define output datasets
kenai_output = os.path.join(data_folder, 'working/site_kenai_coordinates.csv')

# Define intermediate datasets
kenai_features = os.path.join(workspace_gdb, 'site_kenai')

# Set overwrite option
arcpy.env.overwriteOutput = True

# Set output coordinate system to NAD83
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4269)

# Copy data to preserve original dataset
arcpy.management.CopyFeatures(kenai_input, kenai_features)

# Add XY coordinates to copy features
arcpy.AddXY_management(kenai_features)

# Read tables into dataframes
kenai_data = geodatabase_to_dataframe(kenai_features)

# Remove 9 duplicate entries (same site id and coordinates)
kenai_data = kenai_data.drop_duplicates(subset='SITE_ID')

# Export dataframes
kenai_data.to_csv(kenai_output, index=False, encoding='UTF-8')