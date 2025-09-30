# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site table for  Yukon Biophysical Inventory System plot data
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-09-29
# Usage: Must be executed in an ArcGIS Pro Python 3.11+ distribution.
# Description: "Format Site table for Yukon Biophysical Inventory System plot data" extracts relevant
# information from an ESRI shapefile, re-projects to NAD83, drops sites with missing data, verifies that coordinates
# are within the map boundary, replaces values with the correct constrained values, and performs QC checks. The
# output is a CSV file that can be used to write a SQL INSERT
# statement for ingestion into the AKVEG database.
# ---------------------------------------------------------------------------

# Import packages
import geopandas as gpd
import pandas as pd
import polars as pl
from pathlib import Path

# Define folder structure
drive = Path('C:/')
root_folder = drive / 'ACCS_Work'

project_folder = root_folder / 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '52_yukon_biophysical_2023'
source_folder = plot_folder / 'source' / 'ECLDataForAlaska_20240919' / 'YBIS_Data'

# Define inputs
plot_input = source_folder / 'YBISPlotLocations.shp'
veg_input = source_folder / 'Veg_2024Apr09.xlsx'
template_input = project_folder / 'Data_Entry' / '02_site.xlsx'

# Define output
site_output = plot_folder / '02_site_yukonbiophysical2023.csv'

# Read in data
site_original = gpd.read_file(plot_input)
veg_original = pl.read_excel(veg_input, columns=["Project ID", "Plot ID", "Scientific name", "Veg cover pct", "Access"])
template = pl.read_excel(template_input)

# Drop sites with no geometry
site_project = site_original[site_original.geometry.notna()]

# Re-project plot data to NAD83
print(site_project.crs)
output_crs = 'EPSG:4269'
site_project = site_project.to_crs(output_crs)

# Add XY coordinates
site_project['longitude_dd'] = site_project.geometry.x
site_project['latitude_dd'] = site_project.geometry.y

# Explore coordinates
## Not particularly useful to intersect with map boundary since ~50% of sites are outside of it. Perform a visual
# check instead
site_project[["longitude_dd", "latitude_dd"]].describe()  ## Values are reasonable

# Convert to polars dataframe
site = pd.DataFrame(site_project).drop(columns=['geometry'])
site = pl.from_pandas(site)

# Retain relevant columns
site = site.select(['Project_ID', 'Plot_ID', 'Survey_dat', 'Plot_type_', 'latitude_dd', 'longitude_dd',
                                   'Coord_prec', 'Coord_sour'])

print(site.select("Plot_ID","Project_ID").unique().height == site.height)  ## Ensure Plot IDs are unique

# Drop sites that are not for public access
## Communication with data manager on 2025-09-25: Project IDs 206 and 208 can be made public
public_sites = veg_original.filter((pl.col("Access") == 'Available to all users.') |
                            (pl.col("Project ID").is_in([206, 208])))
site = site.join(public_sites, left_on='Plot_ID', right_on='Plot ID', how='semi')

# Drop site for which all percent cover are zero
## Notes indicate this is because % cover was not recorded
no_cover = (public_sites.group_by('Plot ID')
            .agg(pl.col("Veg cover pct").sum().alias("total_cover"))
            .filter(pl.col('total_cover') == 0)
            .select('Plot ID'))
site = site.join(no_cover, left_on='Plot_ID', right_on='Plot ID', how='anti')

# Drop sites without enough data

## Replace unknown/empty values with NaN
print(site.null_count())

values_to_replace = [' ', 'UNK']
site = site.with_columns(
    pl.col(pl.Utf8)  # Select all string (Utf8) columns
    .replace(values_to_replace, None)
)

## Drop sites without survey dates (n=1)
## Drop sites with only plot + soil data (no vegetation) (n=6)
## Drop sites with unknown coordinate precision (n=1500)
site = (site.drop_nulls(subset=pl.col('Survey_dat'))
        .filter(pl.col('Plot_type_') != 'Plot and soil information only')
        .filter(pl.col('Coord_sour').is_not_null() | pl.col('Coord_prec').is_not_null()))

## Drop sites where minimum mapping unit is fairly coarse (1:50,000 and 1:250,000)
site = site.filter(~pl.col('Coord_prec').is_in(['MMU50K', 'MMU250K']))

# Drop sites with unreliable survey dates (n=2)
site = site.with_columns(observe_date = pl.col("Survey_dat").str.to_date("%Y %b %d"))
site = site.with_columns(observe_month = pl.col('observe_date').dt.month())
site = site.filter((pl.col('observe_month') >= 4) & (pl.col('observe_month') <= 10))
print(site.select('observe_date').describe())
print(site.select('observe_month').unique().sort(by='observe_month'))

# Replace null values
site = site.with_columns(pl.col('Plot_type_').fill_null(pl.lit('unknown')),
                         pl.col('Coord_prec').fill_null(pl.lit('-999')),
                         pl.col('Coord_sour').fill_null(pl.lit('unknown')))
print(site.null_count())

# Format site code

## Create combination of project + plot ID for easier retrieval/comparison later on
site = site.with_columns(
    (pl.col("Project_ID").cast(pl.Utf8) +
    pl.lit("_") +
    pl.col("Plot_ID").cast(pl.Utf8)).alias("site_code"))

# Format horizontal error
print(site['Coord_prec'].value_counts())
site = site.with_columns(pl.when(pl.col('Coord_prec') == '>100m')
                         .then(pl.lit("200"))
                         .otherwise(pl.col('Coord_prec').str.replace(r"m$", "", literal=False))
                         .alias('h_error_m'))
site = site.with_columns(pl.col('h_error_m').str.strip_chars().cast(pl.Int16))  ## Convert to integer

print(site['h_error_m'].value_counts())

# Format plot dimensions
print(site['Plot_type_'].value_counts())

## Create replacement map
dimensions_map = {
    '20 m diameter': '10 radius',
    '10 m diameter': '5 radius',
    '1x1 m': '1×1',
    '10x10 m': '10×10',
    '10x5 m': '5×10',
    '20x20 m': '20×20',
    'approximately 100x100 m observed from an aircraft (size may vary)': '100×100',
    'Non-standard plot of irregular shape (e.g., trough in polygonal landscape)': 'unknown',
    'Unable to determine plot type for older data': 'unknown',
    'Trees: 10x10m; Shrubs: 5x5m; Herbs: 1x1m': '5×5',
    'Trees: 20x20m; Shrubs: 5x5m; Herbs: 1x1m': '5×5'
}

## Replace values to match constrained values in AKVEG Database
site = site.with_columns(pl.col('Plot_type_').str.replace_many(dimensions_map)
                         .alias('plot_dimensions_m'))

## Drop sites with unknown plot dimensions
site = site.filter(pl.col('plot_dimensions_m') != 'unknown')
print(site['plot_dimensions_m'].value_counts())

# Format positional accuracy
print(site['Coord_sour'].value_counts())

## Create replacement map
accuracy_map = {
    'NGPS': 'consumer grade GPS',
    'DGPS': 'consumer grade GPS',
    'ORTHOPHOTO1.0': 'image interpretation',
    'DIGITIZED50K': 'map interpretation',
    'MAP50K': 'map interpretation',
    'DIGITIZED250K': 'map interpretation',
    'MAP250K': 'map interpretation',
}

## Replace values to match constrained values in data dictionary
site = site.with_columns(pl.col('Coord_sour').str.replace_many(accuracy_map)
                         .alias('positional_accuracy'))
site = site.with_columns(pl.col('positional_accuracy').str.replace("MAP", "map interpretation")
                         .alias('positional_accuracy'))

print(site['positional_accuracy'].value_counts())

## Explore entries with 'unknown' positional accuracy
unknown_accuracy = site.filter(pl.col('positional_accuracy') == 'unknown')
print(unknown_accuracy['h_error_m'].value_counts())

## Correct 2 sites with unknown positional accuracy and relatively low positional error
site = site.with_columns(pl.when((pl.col('positional_accuracy') == 'unknown') & (pl.col('h_error_m') <= 10))
                         .then(pl.lit('consumer grade GPS'))
                         .otherwise(pl.col('positional_accuracy'))
                         .alias('positional_accuracy'))

## Drop remaining sites with unknown positional accuracy and high positional error (n=7). Cannot be used for map
# development or classification.
site = site.filter(pl.col('positional_accuracy') != 'unknown')

# Populate remaining columns and match template formatting
site_final = (site.with_columns(pl.lit('yukon_biophysical_2023').alias('establishing_project_code'),
                                pl.lit('aerial').alias('perspective'),
                                pl.lit('semi-quantitative visual estimate').alias('cover_method'),
                                pl.lit('NAD83').alias('h_datum'),
                                pl.lit('targeted').alias('location_type'))
              .select(template.columns))

# Export to CSV
site_final.write_csv(site_output)
