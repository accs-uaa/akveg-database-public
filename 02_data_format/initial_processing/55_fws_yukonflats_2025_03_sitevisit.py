# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Visit table for USFWS Yukon Flats data
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2026-02-04
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Site Visit table for USFWS Yukon Flats data" extracts site visit data from an Esri
# shapefile. The script verifies the data for completeness, ensures that all constrained values
# are included in the AKVEG Database, corrects unknown personnel names, and formats required metadata. The
# output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import geopandas as gpd
import polars as pl
from pathlib import Path
from utils import get_template
from utils import get_valid_values

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '55_fws_yukonflats_2025'

# Define input
site_input = plot_folder / '02_site_fwsyukonflats2025.csv'
visit_input = plot_folder / 'source' / 'YKF25_biomass_sites' / 'YKF25_biomass_sites.shp'

# Define output
visit_output = plot_folder / '03_sitevisit_fwsyukonflats2025.csv'

# Read in data
visit_original = gpd.read_file(visit_input)
site_original = pl.read_csv(site_input, columns='site_code')
template = get_template("site visit")

# Query AKVEG database
personnel_set = get_valid_values('personnel')

# Convert GeoPandas DataFrame to Polars DataFrame
visit = pl.from_pandas(visit_original.drop(columns=['geometry', 'fid', 'plot_size', 'Locality']))

# Perform pre-processing quality checks

## Ensure that all site codes are present in the visit table
print(site_original['site_code'].equals(visit['plotID']))

## Ensure date ranges are reasonable
print(visit.select('Date').describe())
print(visit['Date'].dt.month().unique())

# Convert to LazyFrame
lazy_visit = visit.lazy()

# Process data
lazy_processed = (
    lazy_visit
    # Convert datetime to date string
    .with_columns(pl.col('Date').dt.strftime("%Y-%m-%d").alias('observe_date'))
    # Combine site code and date into site visit code
    .with_columns(pl.concat_str([pl.col('plotID'),
                                 pl.col('observe_date')],
                                separator='_')
                  .str.replace_all("-", "")
                  .alias('site_visit_code'))
    # Format personnel names
    .with_columns(pl.col("Observers")
                  .str.replace("Timm", "Timm Nawrocki")
                  .str.replace("Hunter", "Hunter Gravley")
                  .str.replace("CeLee", "CeLee Terasa")
                  .str.split_exact(", ", 3)
                  .struct.rename_fields(["veg_observer", "veg_recorder", "env_observer"])
                  .alias("fields")
                  )
    .unnest("fields")
    # Populate missing columns
    .with_columns(pl.lit('fws_yukonflats_2025').alias('project_code'),
                  pl.lit('map development & verification').alias('data_tier'),
                  pl.lit('none').alias('soils_observer'),
                  pl.lit('not assessed').alias('structural_class'),
                  pl.lit('exhaustive').alias('scope_vascular'),
                  pl.lit('category').alias('scope_bryophyte'),
                  pl.lit('none').alias('scope_lichen'),
                  pl.lit('TRUE').alias('homogeneous'))
    # Rename columns
    .rename({'plotID': 'site_code'})
    # Select columns to match template formatting
    .select(template.columns)
)

# Execute processing
visit_final = lazy_processed.collect()

# Perform quality control checks

## Ensure that all site visit codes are unique
print(visit_final['site_visit_code'].is_unique().unique())

## Ensure all personnel names are included in reference table
## Add missing names to database dictionary
personnel_columns = ['veg_observer', 'veg_recorder', 'env_observer', 'soils_observer']
missing_personnel = ((visit_final.unpivot(on=personnel_columns)
                     .filter(~pl.col('value').is_in(personnel_set)))
                     ['value']
                     .unique()
                     .sort()
                     .to_list())
print(f"Unknown personnel: {missing_personnel}")

## Ensure there are no null values
with pl.Config(tbl_cols=visit_final.shape[1]):
    print(visit_final.null_count())

# Export as CSV
visit_final.write_csv(visit_output)
