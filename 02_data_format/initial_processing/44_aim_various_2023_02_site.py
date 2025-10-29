# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Table for BLM AIM Various 2023 data
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-10-29
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Site Table for BLM AIM Various 2023 data" formats site-level information for ingestion into
# the AKVEG Database. The script depends on the output from the 44_aim_various_2023_00_extract_data.py script. The
# script standardizes project and site codes, checks for spatial outliers, formats plot dimension values,
# and adds required metadata fields. The output is a CSV table that can be converted and included in a SQL INSERT
# statement.
# Notes: For plot dimensions, page 24 of the BLM AIM Wetland Field Protocols states, "The spoke layout is intended for
# riparian or wetland areas (or zones of interest) that are large enough to accommodate a 30-m radius circle". We
# assumed all plots that used a spoke layout had a plot dimension of 30 m radius. For transverse plot, we calculated
# the circular radius using the Plot Area with Minimum Plot Length in Table 4 (Page 29) that corresponded to the
# ActualPlotLength column (which we assumed was the same as the Maximum Plot Length column in Table 4).
# Bureau of Land Management. 2024. AIM National Aquatic Monitoring Framework: Field Protocol for Lentic Riparian and
# Wetland Systems. Tech Reference 1735-3. U.S. Department of the Interior, Bureau of Land Management, National
# Operations Center, Denver, CO.
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

# Define inputs
site_input = plot_folder / 'working' / '44_aim_2023_site_export.csv'
project_input = plot_folder / '01_project_aimvarious2023.csv'
template_input = template_folder / '02_site.xlsx'

# Define output
site_output = plot_folder / '02_site_aimvarious2023.csv'

# Read in data
site_original = pl.read_csv(site_input, try_parse_dates=True, columns=["Project", "PlotID", "EstablishmentDate",
                                                                       "GPSAccuracy", "SamplingApproach",
                                                                       "AvgWidthArea", "PlotLayout",
                                                                       "ActualPlotLength",
                                                                       "POINT_X", "POINT_Y"])
project_original = pl.read_csv(project_input)
template = pl.read_excel(template_input)

# Ensure site code prefixes are consistent
site = site_original.with_columns(pl.col("PlotID")
                                  .str.extract(pattern=r"(^[a-zA-Z]*-[a-zA-z]*)")
                                  .alias("site_prefix"))
print(site['site_prefix'].unique())

# Drop any sites with null coordinates or date (n=0)
site = site.drop_nulls(subset=pl.col("POINT_X", "POINT_Y", "EstablishmentDate"))
site = site.drop_nans(subset=pl.col("POINT_X", "POINT_Y"))

# Filter sites that aren't in Alaska
## Coordinates were already re-projected to NAD83 in previous script
site_filtered = filter_sites_in_alaska(site, input_crs="EPSG:4269", latitude_col="POINT_Y", longitude_col="POINT_X")

# Initialize lazy frame
lazy_site = (
    site_filtered.lazy()

    # Format project code
    .with_columns(
        pl.when(pl.col("Project") == "UnspecifiedBLM")
        .then(pl.lit("AK_CentralYukonFO_2022"))
        .otherwise(pl.col("Project"))
        .alias("project_name"))
    .with_columns(pl.col("project_name").str.replace_many(["AK", "-"], ["AIM", "_"]))
    .with_columns(pl.col("project_name")
                  .str.replace_all(r"([a-z])([A-Z])", r"${1}_${2}", literal=False)
                  .str.to_lowercase()
                  .alias("establishing_project_code")
                  )
    # Rename PlotID columns to site code
    .rename({"PlotID": "site_code"})

    # Format plot dimensions (see Notes in script header)
    .with_columns(pl.when(pl.col("PlotLayout") == "Spoke")
                  .then(pl.lit("30 radius"))
                  .when((pl.col("PlotLayout") == "Transverse") & (pl.col("ActualPlotLength") ==
                                                                  86))
                  .then(pl.lit("20 radius"))
                  .when((pl.col("PlotLayout") == "Transverse") & (pl.col("ActualPlotLength") ==
                                                                  120))
                  .then(pl.lit("15 radius"))
                  .otherwise(pl.lit("unknown"))
                  .alias("plot_dimensions_m"))

    # Populate remaining columns
    .with_columns(pl.lit("ground").alias("perspective"),
                  pl.lit("line-point intercept").alias("cover_method"),
                  pl.lit("NAD83").alias("h_datum"),
                  pl.col("SamplingApproach").str.to_lowercase().alias("location_type"),
                  pl.when(pl.col("GPSAccuracy").is_not_null())
                  .then(pl.col("GPSAccuracy").round(decimals=2))
                  .otherwise(-999)
                  .alias("h_error_m"))
    .with_columns(pl.when(pl.col("h_error_m") < 2)
                  .then(pl.lit("mapping grade GPS"))
                  .otherwise(pl.lit("consumer grade GPS"))
                  .alias("positional_accuracy"))
)

# Execute lazy frame operations
site_final = lazy_site.collect()

# Match template columns
site_final = site_final[template.columns]

# QC
with pl.Config(tbl_cols=12):
   print(site_final.describe())

## Ensure that project codes match with those listed in the Project table
print(site_final["establishing_project_code"].unique().sort().equals(project_original["project_code"].sort()))

## Ensure that all site codes are unique (false indicates all codes are unique)
print(site_final['site_code'].is_duplicated().value_counts())

## Verify values
print(site_final["plot_dimensions_m"].value_counts())  # None with 'unknown'

# Export as CSV
site_final.write_csv(site_output)
