# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Obtain coordinates from shapefiles for FWS Pribilof Islands 2022 data
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2024-09-05
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Obtain coordinates from shapefiles for FWS Pribilof Islands 2022 data" reads in shapefiles, obtains coordinates of polygon centroids, re-projects to NAD83, appends XY data, extracts elevation values, and exports files as CSV. The output is used to inform ingestion of the fws_pribilof_2022 dataset into the AKVEG database.
# ---------------------------------------------------------------------------

# Import packages
import os
import arcpy
from akutils import *
from arcpy.sa import *

# Set root directory
drive = 'C:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
data_folder = os.path.join(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
source_folder = os.path.join(data_folder, 'source', 'GIS')
spatial_data_folder = os.path.join(drive, root_folder, 'Projects', 'AKVEG_Map', 'Data')

# Set workspace
workspace_gdb = os.path.join(spatial_data_folder, 'AKVEG_Workspace.gdb')
arcpy.env.workspace = workspace_gdb

# Define input datasets
stgeorge_input = os.path.join(source_folder,  'StGeorge_FieldPlots.shp')
stpaul_input = os.path.join(source_folder,  'StPaul_FieldPlots.shp')
elevation_input = os.path.join(spatial_data_folder, 'topography', 'Alaska_Composite_DTM_10m', 'integer', 'Elevation_10m_3338.tif')

# Define output datasets
pribilof_output = os.path.join(data_folder, 'working/pribilof_centroid_coordinates.csv')

# Define intermediate datasets
stgeorge_centroids = os.path.join(workspace_gdb, 'stgeorge_centroids')
stpaul_centroids = os.path.join(workspace_gdb, 'stpaul_centroids')
pribilof_centroids = os.path.join(workspace_gdb, 'pribilof_centroids')
pribilof_centroids_elevation = os.path.join(workspace_gdb, 'pribilof_centroids_elevation')

# Set overwrite option
arcpy.env.overwriteOutput = True

# Set output coordinate system to NAD83
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4269)

# Convert polygons to centroid points
arcpy.management.FeatureToPoint(stgeorge_input, stgeorge_centroids, point_location="INSIDE")
arcpy.management.FeatureToPoint(stpaul_input, stpaul_centroids)

# Drop unnecessary fields
fields_to_keep = ["Island", "PlotID", "Relevé"]
arcpy.management.DeleteField(stgeorge_centroids, fields_to_keep, "KEEP_FIELDS")
arcpy.management.DeleteField(stpaul_centroids, fields_to_keep, "KEEP_FIELDS")

# Merge into single feature class
# Because output coordinate system has been set, merge output will be in NAD83 (how convenient!)
arcpy.management.Merge([stgeorge_centroids, stpaul_centroids], pribilof_centroids)

# Add XY coordinates to copy features
arcpy.AddXY_management(pribilof_centroids)

# Extract elevation data
print('Extracting elevation data...')
ExtractValuesToPoints(pribilof_centroids, elevation_input, pribilof_centroids_elevation)

# Read tables into dataframes
pribilof_data = geodatabase_to_dataframe(pribilof_centroids_elevation)

# Export dataframes
pribilof_data.to_csv(pribilof_output, index=False, encoding='UTF-8')
