# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Table for BLM AIM Various 2023 data
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-10-29
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Site Table for BLM AIM Various 2023 data" formats site-level information for ingestion into
# the AKVEG Database. The script depends on the output from the 44_aim_various_2023_00_extract_data.py script. The
# script standardizes project and site codes, checks for spatial outliers, formats plot dimension values,
# and adds required metadata fields. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import polars as pl
from pathlib import Path
from utils import filter_sites_in_alaska

# Define directories
drive = Path('C:/')
root_folder = drive / 'ACCS_Work'

# Define folders
project_folder = root_folder / 'OneDrive - University of Alaska/ACCS_Teams' / 'Vegetation' / 'AKVEG_Database' / 'Data'
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
site_original = pl.read_csv(site_input, try_parse_dates=True)
project_original = pl.read_csv(project_input)
template = pl.read_excel(template_input)

# Drop any sites with null coordinates or date (n=0)
site = site_original.drop_nulls(subset=pl.col("POINT_X", "POINT_Y", "EstablishmentDate"))
site = site.drop_nans(subset=pl.col("POINT_X", "POINT_Y"))

# Format project code
site = site.with_columns(
    pl.when(pl.col("Project") == "UnspecifiedBLM")
    .then(pl.lit("AK_CentralYukonFO_2022"))
    .otherwise(pl.col("Project"))
    .alias("project_name"))

site = site.with_columns(pl.col("project_name").str.replace_many(["AK", "-"], ["AIM", "_"]))
site = site.with_columns(pl.col("project_name")
                         .str.replace_all(r"([a-z])([A-Z])", r"${1}_${2}", literal=False)
                         .str.to_lowercase()
                         .alias("project_code")
                         )
# Ensure site code prefixes are consistent
site = site.with_columns(pl.col("PlotID").str.extract(pattern=r"(^[a-zA-Z]*-[a-zA-z]*)").alias("site_prefix"))
print(site['site_prefix'].unique())

# Rename site code
site = site.rename({"PlotID": "site_code"})

# Filter sites that aren't in Alaska
## Coordinates were already re-projected to NAD83 in previous script
site_filtered = filter_sites_in_alaska(site, input_crs="EPSG:4269", latitude_col="POINT_Y", longitude_col="POINT_X")

# Format plot dimensions
## Page 24 of the BLM AIM Wetland Field Protocols: "The spoke layout is intended for riparian or wetland areas (or
# zones of interest) that are large enough to accommodate a 30-m radius circle". Assume that a spoke layout would not
# have been used if the area was too small for a 30 m radius, even for sites that list an AvgWidthArea less than 60.
# There is nothing in the comments section for these plots that indicate that a different plot layout or dimension was used.
# For transverse plot, we calculate the radius of a circle given the Plot Area and Minimum Plot Length listed in
# Table 4 (Page 29) that corresponds with the value in the ActualPlotLength column.
# Bureau of Land Management. 2024. AIM National Aquatic Monitoring Framework: Field Protocol for Lentic Riparian and Wetland Systems. Tech Reference 1735-3. U.S. Department of the Interior, Bureau of Land Management, National Operations Center, Denver, CO.
site_filtered = (site_filtered.with_columns(pl.when(pl.col("PlotLayout") == "Spoke")
                                           .then(pl.lit("30 radius"))
                                           .when((pl.col("PlotLayout") == "Transverse") & (pl.col("ActualPlotLength") ==
                                                 86))
                                            .then(pl.lit("20 radius"))
                                            .when((pl.col("PlotLayout") == "Transverse") & (pl.col("ActualPlotLength") ==
                                                 120))
                                            .then(pl.lit("15 radius"))
                                            .otherwise(pl.lit("unknown"))
                                            .alias("plot_dimensions_m")))

## Examine sites with unknown plot dimensions
print(site_filtered.filter(pl.col("plot_dimensions_m") == "unknown"))  # None

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

# Ensure that site codes are still unique
length(unique(site_data$site_code)) == nrow(site_data)
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
