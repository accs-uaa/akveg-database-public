# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Table for BLM AIM Various 2023 data
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-10-28
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Site Table for BLM AIM Various 2023 data" formats site-level information for ingestion into
# the AKVEG Database. The script depends on the output from the 44_aim_various_2023_00_extract_data.py script. The
# script standardizes project and site codes, checks for spatial outliers, formats plot dimension values,
# and adds required metadata fields. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import polars as pl
from pathlib import Path

# Define directories
drive = Path('C:/')
root_folder = drive / 'ACCS_Work'

# Define folders
project_folder = root_folder / 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data'
plot_folder = project_folder / 'Data_Plots' / '44_aim_various_2023'
template_folder = project_folder / 'Data_Entry'
repository_folder = root_folder / 'Repositories' / 'akveg-database-public'

# Define inputs
site_input = plot_folder / 'working' / '44_aim_2023_site_export.csv'
project_input = plot_folder / '01_project_aimvarious2023.csv'
template_input = template_folder / '02_site.xlsx'

# Define output
site_output = plot_folder / '02_site_aimvarious2023.csv'

# Read in data
site_original = pl.read_csv(site_input)
project_original = pl.read_csv(project_input)
template = pl.read_excel(template_input)

# Explore data ----

# List project names
unique(site_original$Project)

# Ensure that each site code is unique
length(unique(site_original$PlotID)) == nrow(site_original)

# Are any sites missing coordinates?
site_original % > %
filter( is.na(POINT_X) | is.na(POINT_Y))

# Ensure prefixes for site codes are consistent
site_original % > %
group_by(Project) % > %
mutate(site_prefix=str_c(str_split_i(`PlotID`, "-", 2), str_split_i(`PlotID`, "-", 3), sep="-")) % > %
distinct(site_prefix) % > %
print(n=100)

# Format project code ----

# Correct project name for one entry
# 'UnspecifiedBLM' should be Central Yukon (based on PlotID)
site_data = site_original % > %
mutate(Project=case_when(Project == "UnspecifiedBLM"
~ "AK_CentralYukonFO_2022",
.default = Project))
site_data = site_data % > %
mutate(establishing_project_code=str_replace(Project, "AK", "AIM"),
       establishing_project_code=str_replace(establishing_project_code, "-", "_"),
       establishing_project_code=gsub("([a-z])([A-Z])", "\\1_\\2", establishing_project_code),
       establishing_project_code=str_to_lower(establishing_project_code))

# Format site codes ----
# Remove 'AK' prefix
# Remove "CYFO' and 'EIFO' prefixes to shorten site code. project_code already includes this info
site_data = site_data % > %
mutate(site_code=str_remove_all(PlotID, c("AK-|CYFO-|EIFO-")),
       site_code=str_replace_all(site_code, "-", "_"))

# Ensure that site codes are still unique
length(unique(site_data$site_code)) == nrow(site_data)

# Look for spatial outliers ----
summary(site_data$POINT_X)  # X-axis, negative values
summary(site_data$POINT_Y)  # Y-axis, positive values

# Convert to sf object
# Datum is NAD83
site_sf = st_as_sf(site_data,
                   coords=c("POINT_X", "POINT_Y"),
                   crs=4269)

# Map points
# Point on the north side of the Brooks Range are valid
plot_display = "FALSE"

if (plot_display == "TRUE"){
plot_outliers(site_sf, api_key, 5)
}

# Format plot dimensions ----
# We assume that the radius for all sites that use a spoke layout is 30 m, even for sites that list an AvgWidthArea that is less than 60. There is nothing in the comments section for these plots that indicate that a different plot layout or dimension was used.
# Page 24 of the BLM AIM Wetland Field Protocols: "The spoke layout is intended for riparian or wetland areas (or zones of interest) that are large enough to accommodate a 30-m radius circle". We therefore assume that a spoke layout would not have been used if the area was too small to accommodate a 30 m radius.
# For transverse plot, we calculate the radius of a circle given the Plot Area and Minimum Plot Length listed in Table 4 (page 29) that corresponds with the value in the ActualPlotLength column.
# Bureau of Land Management. 2024. AIM National Aquatic Monitoring Framework: Field Protocol for Lentic Riparian and Wetland Systems. Tech Reference 1735-3. U.S. Department of the Interior, Bureau of Land Management, National Operations Center, Denver, CO.
site_data = site_data % > %
mutate(plot_dimensions_m=case_when(PlotLayout == "Spoke"
~ "30 radius",
PlotLayout == "Transverse" & ActualPlotLength == 86
~ '20 radius',

.default = "unknown"))

site_data % > %filter(plot_dimensions_m == "unknown")  # Ensure all sites have a plot dimension value

# Populate remaining columns ----
# Use h_error_m field to inform positional accuracy
site_data_final = site_data % > %
mutate(perspective="ground",
       cover_method="line-point intercept",
       h_datum="NAD83",
       longitude_dd=round(POINT_X, digits=5),
       latitude_dd=round(POINT_Y, digits=5),
       h_error_m=round(GPSAccuracy, digits=2),
       positional_accuracy=case_when(h_error_m < 2
~ "mapping grade GPS",
.default = "consumer grade GPS"),
location_type = str_to_lower(SamplingApproach)) % > %
select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
    lapply(
        lapply(site_data_final, is.na)
, sum))

# Check that project codes match with those listed in the 01- Project table
unique(site_data_final$establishing_project_code) == project_original$project_code

# Check that site code prefixes are consistent
site_data_final % > %mutate(site_prefix=case_when(grepl("West", site_code)
~ str_split_i(site_code, "_\\d", 1),
.default = str_split_i(site_code, "_", 1))) % > %distinct(site_prefix)

# Check plot dimensions values
table(site_data_final$plot_dimensions_m)

# Check that range of error values is reasonable
summary(site_data_final$h_error_m)

# Check values of positional accuracy
site_data_final % > %filter(h_error_m >= 2) % > %nrow()
table(site_data_final$positional_accuracy)  # 6 sites should be listed as 'consumer grade'

# Export as CSV ----
write_csv(site_data_final, site_output)

# Clear workspace ----
rm(list=ls())
