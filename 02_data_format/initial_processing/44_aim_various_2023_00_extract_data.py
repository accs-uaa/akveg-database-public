# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Extract data from features in BLM AIM Wetlands geodatabase
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-10-28
# Usage: Must be executed in an ArcGIS Pro Python 3.11+ distribution.
# Description: "Extract data from features in BLM AIM Wetlands geodatabase" extracts relevant site and vegetation data from a geodatabase. The script restricts extracted data to include only sites
# that are in Alaska, drops extraneous columns, and re-projects coordinates to NAD83. The outputs are two CSV files that can be used to ingest the BLM AIM 2023 dataset into the AKVEG database.
# ---------------------------------------------------------------------------

# Import packages
import os.path
import arcpy
from akutils import *

# Define root directories
drive = 'C:/'
root_folder = 'ACCS_Work'

# Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '44_aim_various_2023')
source_folder = os.path.join(plot_folder, 'source')
workspace_folder = os.path.join(plot_folder, 'working')

# Set workspace
workspace_geodatabase = os.path.join(drive, root_folder, 'Projects', 'AKVEG_Map', 'Data', 'AKVEG_Workspace.gdb')
arcpy.env.workspace = workspace_geodatabase

# Define inputs
source_geodatabase = os.path.join(source_folder, 'RW_AKSDEExport_20241021clean.gdb')
site_input = os.path.join(source_geodatabase, 'F_SiteEvaluation')
plot_input = os.path.join(source_geodatabase, 'F_PlotCharacterization')

lpi_input = os.path.join(source_geodatabase, 'F_LPI')
lpi_detail_input = os.path.join(source_geodatabase, 'F_LPIDetail')

# Define intermediate datasets
site_clean = os.path.join(workspace_geodatabase, 'aim_2023_SiteEval')  # File name in gdb cannot start with numbers
plot_clean = os.path.join(workspace_geodatabase, 'aim_2023_PlotChar')
site_join = os.path.join(workspace_geodatabase, 'aim_2023_SitePlotJoin')

lpi_simplified = os.path.join(workspace_geodatabase, 'aim_2023_LPI')
lpi_detail_copy = os.path.join(workspace_geodatabase, 'F_LPIDetail')
lpi_detail_rename = os.path.join(workspace_geodatabase, 'aim_2023_LPIDetail')
lpi_join_ak= os.path.join(workspace_geodatabase, 'aim_2023_LPIJoin')

# Define outputs
site_output = os.path.join(workspace_folder, '44_aim_2023_site_export.csv')
vegetation_output = os.path.join(workspace_folder, '44_aim_2023_veg_export.csv')

# Set environment options
arcpy.env.overwriteOutput = True
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4269)  # NAD83
arcpy.env.qualifiedFieldNames = False  # Exclude table name in join field names

# ----- Extract site data ----- #

# Drop unnecessary columns from input files

## Create FieldMappings object to manage output fields
field_map_site = arcpy.FieldMappings()
field_map_plot = arcpy.FieldMappings()

## Add all fields to field map objects
field_map_site.addTable(site_input)
field_map_plot.addTable(plot_input)

## Specify desired output columns
columns_site = ["EvaluationID", "PlotID", "Project", "AdminState", "EstablishmentDate", "GPSAccuracy"]
columns_plot = ["EvaluationID", "SamplingApproach", "Observer", "AdditionalObservers", "PlotLayout", "AvgWidthArea",
                "MaxPlotLengthCalc", "ActualPlotLength","TransectSpacingCalc","LayoutJustification","LandscapeType",
                "LandscapeTypeSecondary", "VerticalSlopeShape", "HorizontalShape", "AlaskaEcotypeClassification", "WetlandType"]

# Remove extraneous columns in field map objects
for field in field_map_site.fields:
    if field.name not in columns_site:
        field_map_site.removeFieldMap(field_map_site.findFieldMapIndex(field.name))

for field in field_map_plot.fields:
    if field.name not in columns_plot:
        field_map_plot.removeFieldMap(field_map_plot.findFieldMapIndex(field.name))

# Create new feature classes with fewer columns
arcpy.conversion.ExportFeatures(site_input, site_clean, field_mapping=field_map_site)
arcpy.conversion.ExportFeatures(plot_input, plot_clean, field_mapping=field_map_plot)

# Join input files
## Use inner join to drop sites present in 'site eval' but that were not visited (and thus not include in 'plot char')
site_alaska = arcpy.management.AddJoin(in_layer_or_view=site_clean, in_field='EvaluationID',
                                             join_table=plot_clean, join_field='EvaluationID',
                                             join_type='KEEP_COMMON')

# Select only plots from Alaska
select_expression = "AdminState = 'AK'"
arcpy.management.SelectLayerByAttribute(site_alaska, "NEW_SELECTION","AdminState = 'AK'")

# Copy layer to a new feature class
arcpy.management.CopyFeatures(site_alaska, site_join)

# Add XY coordinates in NAD83
arcpy.management.AddXY(site_join)

# Convert to dataframe
site_table = geodatabase_to_dataframe(site_join)

# Export dataframe
site_table.to_csv(site_output, index=False, encoding='UTF-8')

# ----- Extract vegetation data ----- #

# Select relevant columns from input featuress
fields_to_keep = ["AdminState", "LineKey", "LineNumber", "LineLength"]
arcpy.management.CopyFeatures(lpi_input, lpi_simplified)
arcpy.management.DeleteField(lpi_simplified, fields_to_keep, method='KEEP_FIELDS')

# Copy table to workspace geodatabase so that both join inputs are in the same gdb
## Do not use 'Copy' tool since it will copy all tables that have a relationship to the F_LPIDetail table
## F_LPIDetail cannot already exist in the gdb
arcpy.management.ExtractDataFromGeodatabase(in_data=lpi_detail_input,
                                            out_geodatabase=workspace_geodatabase,
                                            all_records_for_tables='ALL_RECORDS_FOR_TABLES')

# Rename copied table in gdb
## Process will fail if file already exists, even though overwrite is set to TRUE
arcpy.management.Rename(lpi_detail_copy, lpi_detail_rename)

# Join features
lpi_join = arcpy.management.AddJoin(lpi_simplified, in_field="LineKey",
                         join_table=lpi_detail_rename, join_field="RecKey", join_operation='JOIN_ONE_TO_MANY')

# Restrict to sites from Alaska
arcpy.management.SelectLayerByAttribute(lpi_join, 'NEW_SELECTION', "AdminState = 'AK'")

# Copy layer to new feature class
arcpy.management.CopyFeatures(lpi_join, lpi_join_ak)

# Export as CSV
lpi_table = geodatabase_to_dataframe(lpi_join_ak)
lpi_table.to_csv(vegetation_output, index=False, encoding='UTF-8')
