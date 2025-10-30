# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2024 ACCS NWI Southcentral Site Data
# Author: Amanda Droghini
# Last Updated: 2025-09-23
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2024 ACCS NWI Southcentral Site Data" formats site-level information for ingestion into the AKVEG Database.
# The script verifies that site codes are unique, that coordinates are within the map boundary, and adds/corrects
# required metadata fields. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# -------------------

# Import libraries
import polars as pl
import geopandas as gpd
from pathlib import Path

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '54_accs_nwisouthcentral_2024'
spatial_folder = root / "Projects" / "AKVEG_Map" / "Data" / "region_data"

# Define inputs
site_input = plot_folder / 'source' / '02_site_accsnwisouthcentral2024.xlsx'
template_input = project_folder / 'Data_Entry' / '02_site.xlsx'
boundary_input = spatial_folder / "AlaskaYukon_100_Tiles_3338.shp"

# Define output
site_output = plot_folder / '02_site_accsnwisouthcentral2024.csv'

# Read in data
template = pl.read_excel(template_input, schema_overrides={"latitude_dd": pl.Decimal,
                                                           "longitude_dd": pl.Decimal,
                                                           "h_error_m": pl.Decimal})
region_boundary = gpd.read_file(boundary_input)
site_original = pl.read_excel(site_input)

with pl.Config(tbl_cols=site_original.shape[1]):
    print(site_original.null_count())

# Ensure that all site codes are unique
print(site_original['site_code'].is_unique().unique())

# Ensure that all sites are in Alaska

## Create geodataframe
site_spatial = gpd.GeoDataFrame(
    site_original,
    geometry=gpd.points_from_xy(site_original['longitude_dd'], site_original['latitude_dd']),
    crs="EPSG:4269",
)

## Convert to EPSG:3338 to match region boundary
site_spatial = site_spatial.to_crs(crs="EPSG:3338")
print(region_boundary.crs)  # Ensure projection is also EPSG 3338

## List points that intersect with the area of interest
sites_inside = region_boundary.sindex.query(
    geometry=site_spatial.geometry, predicate="intersects"
)[0]

## Investigate points that are outside region boundary
sites_outside = site_spatial.loc[~site_spatial.index.isin(sites_inside)]
print(sites_outside.shape[0]) ## All sites are within map boundary

del site_spatial, sites_inside, sites_outside, region_boundary

# Correct project code, datum, and plot dimensions
site = site_original.with_columns(pl.lit('accs_nwisouthcentral_2024').alias('establishing_project_code'),
                                  pl.lit('NAD83').alias('h_datum'),
                                  pl.lit('10 radius').alias('plot_dimensions_m')
                                  )

# Export as CSV
site.write_csv(site_output)