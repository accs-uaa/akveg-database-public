# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2024 NPS SWAN Site Data
# Author: Amanda Droghini
# Last Updated: 2025-09-17
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2024 NPS SWAN Site Data" formats site-level information for ingestion into the AKVEG Database.
# The script drops sites with missing coordinates, verifies that coordinates are within the map boundary,
# and adds required metadata fields. The output is a
# CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries
import polars as pl
import geopandas as gpd
from pathlib import Path

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '25_nps_swan_2024'
spatial_folder = root / "Projects" / "AKVEG_Map" / "Data" / "region_data"

# Define inputs
template_input = project_folder / 'Data_Entry' / '02_site.xlsx'
boundary_input = spatial_folder / "AlaskaYukon_100_Tiles_3338.shp"
site_input = plot_folder / 'source' / 'SWAN_Vegetation_Database' / 'SWAN_Veg_Plot.csv'
coordinates_input = plot_folder / 'archive' / 'source' / 'AK_Veg_20211115_SWAN_ptint_trees.xlsx'
vegetation_input = plot_folder / 'source' / 'SWAN_Vegetation_Database' / 'SWAN_Veg_PointIntercept.csv'

# Define output
site_output = plot_folder / '02_site_npsswan2024.csv'

# Read in data
template = pl.read_excel(template_input, schema_overrides={"latitude_dd": pl.Decimal,
                                                           "longitude_dd": pl.Decimal,
                                                           "h_error_m": pl.Decimal})
region_boundary = gpd.read_file(boundary_input)
coordinates_original = pl.read_excel(coordinates_input, columns=['Plot', 'Latitude_sw_corner',
                                                                 'Longitude_sw_corner'],
                                                       sheet_name='PlotBasics')
site_original = pl.read_csv(site_input, columns=['Plot'])
vegetation_original = pl.read_csv(vegetation_input, columns=['Plot'], encoding='utf8-lossy')

# Append coordinates to site df
site = site_original.join(coordinates_original, on='Plot', how='right')

# Drop plots with no coordinates
site = site.drop_nulls()

# Drop sites that are not included in vegetation table
## Sites do not meet AKVEG Minimum Standards
missing_sites = site.join(vegetation_original, on='Plot', how='anti')
site = site.join(missing_sites, on="Plot", how="anti")

# Rename columns
site = site.rename({"Plot": "site_code",
                    "Latitude_sw_corner": "latitude_dd",
                    "Longitude_sw_corner": "longitude_dd"})

# Ensure that all sites are in Alaska
## Create geodataframe
site_spatial = gpd.GeoDataFrame(
    site,
    geometry=gpd.points_from_xy(site['longitude_dd'], site['latitude_dd']),
    crs="EPSG:4269",
)

## Convert to EPSG:3338 to match region boundary
site_spatial = site_spatial.to_crs(crs="EPSG:3338")
print(region_boundary.crs)  # Ensure projection is also EPSG 3338

# List points that intersect with the area of interest
sites_inside = region_boundary.sindex.query(
    geometry=site_spatial.geometry, predicate="intersects"
)[0]

# Investigate points that are outside region boundary
sites_outside = site_spatial.loc[~site_spatial.index.isin(sites_inside)]
print(sites_outside.shape[0]) ## All sites are within map boundary

del site_spatial, sites_inside, sites_outside, region_boundary

# Populate remaining columns
site = site.with_columns(
    pl.lit('nps_swan_2024').alias('establishing_project_code'),
    pl.lit('ground').alias('perspective'),
    pl.lit('line-point intercept').alias('cover_method'),
    pl.lit('NAD83').alias('h_datum'),
    pl.lit(1).alias('h_error_m'),
    pl.lit('mapping grade GPS').alias('positional_accuracy'),
    pl.lit('30×30').alias('plot_dimensions_m'),
    pl.lit('targeted').alias('location_type')
)

# Reorder columns to match data entry template
site = site[template.columns]

# Export to CSV
site.write_csv(site_output)
