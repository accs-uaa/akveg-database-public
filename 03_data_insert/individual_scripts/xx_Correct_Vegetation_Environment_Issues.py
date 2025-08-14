# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Perform QA/QC checks for Tables 5-14 of the AKVEG Database
# Author: Amanda Droghini
# Last Updated: 2025-05-27
# Usage: Execute in Python 3.13+.
# Description: "Perform QA/QC checks for Tables 5-14 of the AKVEG Database" queries the AKVEG database to identify
# data entry
# errors in Tables 5 through 14 of the AKVEG Database.
# ---------------------------------------------------------------------------

# Import packages
import os
import numpy as np
import pandas as pd
import datetime
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Set options
pd.set_option("display.max_columns", 8)

# Set root directory
drive = "C:/"
root_folder = (
    "ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database"
)

# Define folder structure
credential_folder = os.path.join(drive, root_folder, "Credentials")
project_folder = os.path.join(drive, "ACCS_Work", "Projects")
repository_folder = os.path.join(drive, "ACCS_Work/Repositories/akveg-database")
output_folder = os.path.join(project_folder, "AKVEG_Database", "Bug_Fixes")

# Define input files
authentication_file = os.path.join(
    credential_folder, "akveg_private_build/authentication_akveg_private_build.csv"
)

## Define query files
vegetation_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_05_VegetationCover.sql"
)
abiotic_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_06_AbioticTopCover.sql"
)
tussock_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_07_WholeTussockCover.sql"
)
ground_cover_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_08_GroundCover.sql"
)
str_group_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_09_StructuralGroupCover.sql"
)
shrub_str_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_11_ShrubStructure.sql"
)
environment_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_12_Environment.sql"
)
soil_metrics_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_13_SoilsMetrics.sql"
)
soil_horizons_file = os.path.join(
    repository_folder, "05_queries", "standard", "Query_14_SoilHorizons.sql"
)

# Define export file
date_today = datetime.date.today().strftime("%Y%m%d")
site_output_filename = "sites_dropped_" + date_today + ".csv"
site_output = os.path.join(output_folder, site_output_filename)

# Create initial database connection
database_connection = connect_database_postgresql(authentication_file)

# Read in SQL queries

## Vegetation Cover query
vegetation_read = open(vegetation_file, "r")
vegetation_query = vegetation_read.read()
vegetation_read.close()
vegetation_data = query_to_dataframe(database_connection, vegetation_query)

## Abiotic Cover query
abiotic_read = open(abiotic_file, "r")
abiotic_query = abiotic_read.read()
abiotic_read.close()
abiotic_data = query_to_dataframe(database_connection, abiotic_query)

## Whole Tussock Cover query
tussock_read = open(tussock_file, "r")
tussock_query = tussock_read.read()
tussock_read.close()
tussock_data = query_to_dataframe(database_connection, tussock_query)

## Ground Cover Query
ground_cover_read = open(ground_cover_file, "r")
ground_cover_query = ground_cover_read.read()
ground_cover_read.close()
ground_cover_data = query_to_dataframe(database_connection, ground_cover_query)

## Structural Group Cover query
str_group_read = open(str_group_file, "r")
str_group_query = str_group_read.read()
str_group_read.close()
str_group_data = query_to_dataframe(database_connection, str_group_query)

## Shrub Structure query
shrub_str_read = open(shrub_str_file, "r")
shrub_str_query = shrub_str_read.read()
shrub_str_read.close()
shrub_str_data = query_to_dataframe(database_connection, shrub_str_query)

## Environment query
environment_read = open(environment_file, "r")
environment_query = environment_read.read()
environment_read.close()
environment_data = query_to_dataframe(database_connection, environment_query)

## Soil Metrics query
soil_metrics_read = open(soil_metrics_file, "r")
soil_metrics_query = soil_metrics_read.read()
soil_metrics_read.close()
soil_metrics_data = query_to_dataframe(database_connection, soil_metrics_query)

## Soil Horizons query
soil_horizons_read = open(soil_horizons_file, "r")
soil_horizons_query = soil_horizons_read.read()
soil_horizons_read.close()
soil_horizons_data = query_to_dataframe(database_connection, soil_horizons_query)

# QA/QC Abiotic Top Cover Table

## Ensure range of cover percent values are reasonable
abiotic_data = abiotic_data.assign(
    abiotic_top_cover_percent=abiotic_data.abiotic_top_cover_percent.astype(float)
)
abiotic_data.abiotic_top_cover_percent.describe()
abiotic_data.isna().sum()  # Database constraints should prevent any nulls from being entered

## Ensure sum of abiotic top cover for each site visit does not exceed 100%
abiotic_cover_sum = abiotic_data.groupby(
    "site_visit_code"
).abiotic_top_cover_percent.sum()
print(abiotic_cover_sum.max())

## Ensure each abiotic element is included each site visit
abiotic_incomplete = abiotic_data.groupby("site_visit_code").count().iloc[:, 0:1]
abiotic_incomplete = abiotic_incomplete[
    abiotic_incomplete.abiotic_cover_id != 8
]  # Address these issues in data ingestion script

del abiotic_file, abiotic_read, abiotic_query, abiotic_cover_sum, abiotic_incomplete

# QC Whole Tussock Cover

# Check for null values
print(tussock_data.isna().sum)

## Ensure range of cover percent values are reasonable
tussock_data = tussock_data.assign(
    cover_percent=tussock_data.cover_percent.astype(float)
)
tussock_data.cover_percent.describe()
tussock_data.isna().sum()  # Database constraints should prevent any nulls from being entered

del tussock_file, tussock_query, tussock_read

# QC Ground Cover

# Check for null values
print(ground_data.isna().sum)

## Ensure range of cover percent values are reasonable
ground_cover_data = ground_cover_data.assign(
    ground_cover_percent=ground_cover_data.ground_cover_percent.astype(float)
)
ground_cover_data.ground_cover_percent.describe()
ground_cover_data.isna().sum()  # Database constraints should prevent any nulls from being entered

## Ensure sum of ground cover for each site visit does not exceed 100%
ground_cover_sum = ground_cover_data.groupby(
    "site_visit_code"
).ground_cover_percent.sum()
print(ground_cover_sum.max())

## Ensure each ground_cover element is included each site visit
ground_cover_incomplete = (
    ground_cover_data.groupby("site_visit_code").count().iloc[:, 0:1]
)
ground_cover_incomplete = ground_cover_incomplete[
    ground_cover_incomplete.ground_cover_id != 13
]  # Address these issues in data ingestion script
ground_cover_incomplete = pd.merge(
    ground_cover_incomplete,
    lookup_table,
    how="left",
    left_on="site_visit_code",
    right_on="site_visit_code",
)

del (
    ground_cover_file,
    ground_cover_query,
    ground_cover_read,
    ground_cover_sum,
    ground_cover_incomplete,
)

# QA/QC Structural Group Cover

## Ensure range of cover percent values are reasonable
str_group_data = str_group_data.assign(
    cover_percent=str_group_data.cover_percent.astype(float)
)
str_group_data.cover_percent.describe()
str_group_data.isna().sum()  # Database constraints should prevent any nulls from being entered

## Ensure sum of structural group for 'absolute caaopy cover' sites do not exceed 100%
str_group_sum = (
    str_group_data[str_group_data.cover_type == "absolute canopy cover"]
    .groupby("site_visit_code")
    .cover_percent.sum()
)
print(str_group_sum.max())

## Ensure each structural group is included each site visit
str_group_incomplete = str_group_data.groupby("site_visit_code").count().iloc[:, 0:1]
str_group_incomplete = str_group_incomplete[
    str_group_incomplete.structural_cover_id != 16
]  # No issues

del str_group_file, str_group_incomplete, str_group_query, str_group_read, str_group_sum

# QC Shrub Structure Table

# Check for null values
print(shrub_str_data.isna().sum)

## Ensure values for numeric columns are reasonable
shrub_str_data[
    [
        "height_cm",
        "cover_percent",
        "mean_diameter_cm",
        "number_stems",
        "shrub_subplot_area_m2",
    ]
] = shrub_str_data[
    [
        "height_cm",
        "cover_percent",
        "mean_diameter_cm",
        "number_stems",
        "shrub_subplot_area_m2",
    ]
].astype(
    float
)
shrub_str_data.describe()  # -999 should not exist in height_cm and shrub_subplot columns

## Explore entries for which cover type is null - cover percent should be -999
shrub_str_null = shrub_str_data[shrub_str_data.cover_type.isna()]
shrub_str_null.cover_percent.unique()

del shrub_str_file, shrub_str_null, shrub_str_query, shrub_str_read

# QC Environment

## Explore numeric columns
environment_data[
    [
        "disturbance_time_y",
        "depth_water_cm",
        "depth_moss_duff_cm",
        "depth_restrictive_layer_cm",
        "microrelief_cm",
        "depth_15_percent_coarse_fragments_cm",
    ]
] = environment_data[
    [
        "disturbance_time_y",
        "depth_water_cm",
        "depth_moss_duff_cm",
        "depth_restrictive_layer_cm",
        "microrelief_cm",
        "depth_15_percent_coarse_fragments_cm",
    ]
].astype(
    float
)
environment_data.describe()

## Explore sites with no disturbance
envr_no_disturbance = environment_data[
    (environment_data.disturbance == "none") | environment_data.disturbance.isna()
]
envr_no_disturbance.describe()  # disturbance time should all be -999
envr_no_disturbance.disturbance_severity.unique()

# QC Soil Metrics

## Explore numeric columns
soil_metrics_data[
    ["measure_depth_cm", "ph", "conductivity_mus", "temperature_deg_c"]
] = soil_metrics_data[
    ["measure_depth_cm", "ph", "conductivity_mus", "temperature_deg_c"]
].astype(
    float
)

soil_metrics_data = soil_metrics_data.replace(
    to_replace=-999, value=np.nan
)  # Replace -999 with null to see 'real' distribution of
# values

soil_metrics_data.describe()

## Explore extreme values
soil_metrics_extreme = soil_metrics_data[soil_metrics_data.ph < 3]
soil_metrics_extreme = pd.merge(
    soil_metrics_extreme, lookup_table, how="left", on="site_visit_code"
)
soil_metrics_extreme.project_code.value_counts()

soil_metrics_extreme = soil_metrics_data[
    soil_metrics_data.temperature_deg_c < 5
]  # Value of -18.9C corresponds to what is written in the source file.

# QC Soil Horizons

## Format numeric columns
soil_horizons_data[
    [
        "horizon_order",
        "thickness_cm",
        "depth_upper_cm",
        "depth_lower_cm",
        "clay_percent",
        "total_coarse_fragment_percent",
        "gravel_percent",
        "cobble_percent",
        "stone_percent",
        "boulder_percent",
    ]
] = soil_horizons_data[
    [
        "horizon_order",
        "thickness_cm",
        "depth_upper_cm",
        "depth_lower_cm",
        "clay_percent",
        "total_coarse_fragment_percent",
        "gravel_percent",
        "cobble_percent",
        "stone_percent",
        "boulder_percent",
    ]
].astype(
    float
)

## Ensure percent columns are between 0 and 100
soil_percentages = soil_horizons_data.filter(
    items=[
        "site_visit_code",
        "horizon_order",
        "clay_percent",
        "total_coarse_fragment_percent",
        "gravel_percent",
        "cobble_percent",
        "stone_percent",
        "boulder_percent",
    ]
)
soil_percentages = soil_percentages.replace(
    to_replace=-999, value=np.nan
)  # Replace -999 with null to see 'real' distribution of values
soil_percentages.describe()

## Ensure sum of rock totals is between 0 and 100
rock_percent = soil_percentages.filter(
    items=[
        "site_visit_code",
        "gravel_percent",
        "cobble_percent",
        "stone_percent",
        "boulder_percent",
    ]
)
rock_percent["sum"] = rock_percent.sum(axis=1, numeric_only=True)
rock_percent["sum"].describe()

## Ensure depth extend is True only for the last soil horizon
max_horizon = (
    soil_horizons_data.filter(items=["site_visit_code", "horizon_order"])
    .groupby("site_visit_code")
    .max()
    .rename(columns={"horizon_order": "max_horizon"})
    .merge(soil_horizons_data, how="left", on="site_visit_code")
    .filter(items=["site_visit_code", "horizon_order", "max_horizon", "depth_extend"])
    .query("horizon_order != max_horizon and depth_extend == True")
    .merge(lookup_table, how="left", on="site_visit_code")
)  # 2 sites to address
