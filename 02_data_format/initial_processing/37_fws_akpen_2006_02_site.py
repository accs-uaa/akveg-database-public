# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Obtain centroid coordinates from polygons
# Author: Amanda Droghini
# Last Updated: 2023-05-26
# Usage: Must be executed in an ArcGIS Pro Python 3.9 installation.
# Description: "Obtain centroid coordinates from polygons" generates centroids within each polygon and obtains the XY coordinates for that centroid. The output is a CSV file which contains the site name and coordinates for each surveyed site.
# ---------------------------------------------------------------------------

# Import packages
import arcpy
import os
import glob

# Set root directory
drive = 'D:\\'
root_folder = 'ACCS_Work'

# Set overwrite option
arcpy.env.overwriteOutput = True

# Define folder structure
data_folder = os.path.join(drive, root_folder, 'Projects\AKVEG_Database')
project_folder = os.path.join(data_folder, 'Data\Data_Plots\\37_fws_alaskapeninsula_2006')
spatial_folder = os.path.join(project_folder, 'source\SHAPEFILES')
temp_folder = os.path.join(project_folder,'temp')

# Define workspace and geodatabase
work_geodatabase = os.path.join(data_folder, 'GIS\\fws_akpeninsula.gdb')
arcpy.env.workspace = work_geodatabase

# Define projection (NAD 83)
output_projection = arcpy.SpatialReference(4269)

# Define input files
all_files = glob.glob(os.path.join(spatial_folder, 'akpb*.shp'))
input_list = [x for x in all_files if 'wildlife' not in x]

# Define global outputs
all_centroids = os.path.join(work_geodatabase, "merged_centroids")
output_table = os.path.join(temp_folder,'site_centroid_coordinates.csv')

# Iterate through polygon shapefiles and extract centroids
for i in range(len(input_list)):

    input_shp = input_list[i]
    # Create file name by removing extension and select last part of string
    file_name = (os.path.splitext(input_shp)[0]).split('\\', -1)[-1]
    print('Processing ' + file_name + ', file ' + str(i+1) + ' of ' + str(len(input_list)))

    # Define outputs
    output_project = os.path.join(work_geodatabase, ''.join([file_name, '_project']))
    output_point = os.path.join(work_geodatabase, ''.join([file_name, '_pts']))
    output_simple = (''.join([file_name, '_pts_simple']))

    # Project to NAD 83
    arcpy.Project_management(input_shp, output_project, output_projection)

    # Convert feature to point
    print("Converting polygon to point...")
    arcpy.FeatureToPoint_management(output_project, output_point, "INSIDE")

    # Add coordinate fields
    print("Adding coordinates...")
    arcpy.AddXY_management(output_point)

    # Add file name as a field in the attribute table
    arcpy.CalculateField_management(output_point, field="File_Name",
                                    expression='"' + file_name + '"',
                                    field_type="TEXT")

    # List fields to keep in final, merged file
    if file_name != "akpb_fsite_all":
        output_fields = ["YEAR_", "SITE_NO", "AREA_NAME",
                         "File_Name", "POINT_X", "POINT_Y"]
    elif file_name == "akpb_fsite_all":
        output_fields = ["year", "SITE_NO", "AREA_NAME",
                         "File_Name", "POINT_X", "POINT_Y"]

    # Create field mappings
    field_mappings = arcpy.FieldMappings()

    for field in output_fields:
        field_map = arcpy.FieldMap()
        field_map.addInputField(output_point, field)
        field_mappings.addFieldMap(field_map)

    # Apply the field mappings
    arcpy.FeatureClassToFeatureClass_conversion(output_point,
                                                work_geodatabase,
                                                output_simple,
                                                "",
                                                field_mappings,
                                                "")
    # Rename year field for fsite_all
    if file_name == "akpb_fsite_all":
        arcpy.management.AlterField(output_simple, "year", "YEAR_")

# List all centroid files
input_point_list = arcpy.ListFeatureClasses(wild_card='*_simple')

# Combine centroids into a single shapefile
print ("Merging all centroids...")
arcpy.Merge_management(inputs=input_point_list, output=all_centroids)

# Delete duplicate entries
duplicate_fields = ["YEAR_", "SITE_NO", "AREA_NAME"]
arcpy.management.DeleteIdentical(all_centroids, duplicate_fields)

# Export to CSV
arcpy.conversion.ExportTable(all_centroids, output_table)