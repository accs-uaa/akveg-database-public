# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Extract site metadata from features in BLM AIM Wetlands geodatabase
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-06-10
# Usage: Must be executed in an ArcGIS Pro Python 3.9+ distribution.
# Description: "Extract site metadata from features in BLM AIM Wetlands geodatabase" joins two shapefiles, selects only those sites that are in Alaska, drops extraneous columns, re-projects to NAD83, appends XY data, and exports files as CSV. The output is used to ingest the 44_aim_various_2022 dataset into the AKVEG database.
# ---------------------------------------------------------------------------

# Import packges
import os.path
import arcpy
from akutils import *

# Define directories

## Define root directories
drive = 'C:/'
root_folder = 'ACCS_Work'

## Define folder structure
project_folder = os.path.join(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = os.path.join(project_folder, 'Data', 'Data_Plots', '44_aim_various_2022')
source_folder = os.path.join(plot_folder, 'source')
workspace_folder = os.path.join(plot_folder, 'working')

# Set workspace
workspace_geodatabase = os.path.join(drive, root_folder, 'Projects', 'AKVEG_Map', 'Data', 'AKVEG_Workspace.gdb')
arcpy.env.workspace = workspace_geodatabase

# Define files

## Define inputs
source_geodatabase = os.path.join(source_folder, 'AIMWetlandPub6-22-23_clean.gdb')
site_eval_input = os.path.join(source_geodatabase, 'F_SiteEvaluation')
plot_char_input = os.path.join(source_geodatabase, 'F_PlotCharacterization')

## Define intermediate datasets
site_eval_clean = os.path.join(workspace_geodatabase, 'AIM_Wetlands_SiteEval')
plot_char_clean = os.path.join(workspace_geodatabase, 'AIM_Wetlands_PlotChar')
site_join_output = os.path.join(workspace_geodatabase, 'AIM_Wetlands_SitePlotJoin')

## Define output
site_table_output = os.path.join(workspace_folder, 'site_data_export.csv')

# Set environment options

## Set overwrite option
arcpy.env.overwriteOutput = True

## Set output coordinate system to NAD 83
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4269)

## Do not include table name in join field names
arcpy.env.qualifiedFieldNames = False

# Clean up input files: Drop unnecessary or redundant columns

## Create FieldMappings object to manage output fields
field_map_site = arcpy.FieldMappings()
field_map_plot = arcpy.FieldMappings()

## Add all fields to field map objects
field_map_site.addTable(site_eval_input)
field_map_plot.addTable(plot_char_input)

## Specify desired output columns
columns_site = ["EvaluationID", "PlotID", "Project", "AdminState", "EstablishmentDate", "GPSAccuracy"]
columns_plot = ["EvaluationID", "SamplingApproach", "PlotLayout", "AvgWidthArea","MaxPlotLengthCalc","ActualPlotLength","TransectSpacingCalc","LayoutJustification","LandscapeType", "LandscapeTypeSecondary", "VerticalSlopeShape", "HorizontalShape", "AlaskaEcotypeClassification", "WetlandType"]

## Remove extraneous columns in field map objects
for field in field_map_site.fields:
    if field.name not in columns_site:
        field_map_site.removeFieldMap(field_map_site.findFieldMapIndex(field.name))

for field in field_map_plot.fields:
    if field.name not in columns_plot:
        field_map_plot.removeFieldMap(field_map_plot.findFieldMapIndex(field.name))

## Create new feature classes with fewer columns
arcpy.conversion.ExportFeatures(site_eval_input, site_eval_clean, field_mapping=field_map_site)
arcpy.conversion.ExportFeatures(plot_char_input, plot_char_clean, field_mapping=field_map_plot)

# Join input files
# As both contain information that will be used in the 02- Site and 03- Site Visit tables
# Use inner join to drop sites present in 'site eval' but that were not visited (and thus not include in 'plot char')
site_feature_join = arcpy.management.AddJoin(in_layer_or_view=site_eval_clean, in_field='EvaluationID',
                                             join_table=plot_char_clean, join_field='EvaluationID',
                                             join_type='KEEP_COMMON')

## Select only plots in Alaska
select_expression = "AdminState = 'AK'"
arcpy.management.SelectLayerByAttribute(site_feature_join, "NEW_SELECTION","AdminState = 'AK'")

## Copy layer to a new feature class
arcpy.management.CopyFeatures(site_feature_join, site_join_output)

## Add XY coordinates (in NAD 83)
arcpy.management.AddXY(site_join_output)

# Convert to dataframe
site_table = geodatabase_to_dataframe(site_join_output)

# Export dataframe
site_table.to_csv(site_table_output, index=False, encoding='UTF-8')