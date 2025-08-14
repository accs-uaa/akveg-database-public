# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Extract vegetation cover data from features in BLM AIM Wetlands geodatabase
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2024-08-02
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Extract vegetation cover data from features in BLM AIM Wetlands geodatabase" joins a shapefile and standalone table, selects only those sites that are in Alaska, drops extraneous columns, and exports files as CSV. The output is used to ingest the 44_aim_various_2022 dataset into the AKVEG database.
# ---------------------------------------------------------------------------

# Import packages
import os
import arcpy
from akutils import *

# Define directories

## Define root directories
drive = 'C:/'
root = 'ACCS_Work'

## Define folder structure
project_folder = os.path.join(drive, root, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database', 'Data', 'Data_Plots', '44_aim_various_2022')
data_folder = os.path.join(project_folder, 'source')

## Define geodatabases
source_geodatabase = os.path.join(data_folder, 'AIMWetlandPub6-22-23_clean.gdb')
workspace_geodatabase = os.path.join(drive, root, 'Projects', 'AKVEG_Map', 'AKVEG_Workspace.gdb')

# Define files

## Define input files
input_lpi_feature = os.path.join(source_geodatabase, 'F_LPI')
input_lpi_table = os.path.join(source_geodatabase, 'F_LPIDetail')

## Define intermediate datasets
simplified_lpi_feature = os.path.join(workspace_geodatabase, 'AIM_Wetlands_LPIMetadata')
copied_lpi_table = os.path.join(workspace_geodatabase, 'F_LPIDetail')
rename_lpi_table = os.path.join(workspace_geodatabase, 'AIM_Wetlands_LPITable')
lpi_join_features_ak= os.path.join(workspace_geodatabase, 'AIM_Wetlands_LPIJoin')

## Define output file
output_veg_cover = os.path.join(project_folder, 'working', 'veg_cover_data_export.csv')

# Set environment settings

## Set workspace geodatabase
arcpy.env.workspace = workspace_geodatabase

## Set overwrite option
arcpy.env.overwriteOutput = True

## Set output coordinate reference system
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4269)

## Do not include table name in join field names
arcpy.env.qualifiedFieldNames = False

# Join input files into a single feature

## Select only relevant columns from input features
fields_to_keep = ["AdminState", "LineKey", "LineNumber", "LineLength"]
arcpy.management.CopyFeatures(input_lpi_feature, simplified_lpi_feature)
arcpy.management.DeleteField(simplified_lpi_feature, fields_to_keep, method='KEEP_FIELDS')

# Copy table to workspace geodatabase so that both join inputs are in the same gdb
# Do not use 'Copy' tool since it will copy all tables that have a relationship to the F_LPIDetail table
arcpy.management.ExtractDataFromGeodatabase(in_data=input_lpi_table, out_geodatabase=workspace_geodatabase, all_records_for_tables='ALL_RECORDS_FOR_TABLES')

# Rename copied table in gdb
# Process will fail if file already exists, even though overwrite is set to TRUE
arcpy.management.Rename(copied_lpi_table, rename_lpi_table)

# Join features
lpi_join_features = arcpy.management.AddJoin(simplified_lpi_feature, in_field="LineKey",
                         join_table=rename_lpi_table, join_field="RecKey", join_operation='JOIN_ONE_TO_MANY')

# Restrict to sites only in Alaska
arcpy.management.SelectLayerByAttribute(lpi_join_features, 'NEW_SELECTION', "AdminState = 'AK'")

# Copy layer to new feature class
arcpy.management.CopyFeatures(lpi_join_features, lpi_join_features_ak)

## Export as CSV
# Convert to dataframe
lpi_table = geodatabase_to_dataframe(lpi_join_features_ak)

# Export dataframe
lpi_table.to_csv(output_veg_cover, index=False, encoding='UTF-8')
