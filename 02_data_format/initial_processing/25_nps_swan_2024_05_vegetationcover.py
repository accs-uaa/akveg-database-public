# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for 2024 NPS SWAN data"
# Author: Amanda Droghini
# Last Updated: 2025-09-22
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Calculate Vegetation Cover for 2024 NPS SWAN data" uses data from line-point intercept surveys to
# calculate site-level percent foliar cover for each recorded species. It also appends unique site visit identifiers,
# corrects taxonomic names using the AKVEG database as a taxonomic standard, and populates required metadata fields.
# The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries
import polars as pl
from pathlib import Path
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database'
plot_folder = project_folder / 'Data' / 'Data_Plots' / '25_nps_swan_2024'
credential_folder = project_folder / "Credentials"

# Define inputs
template_input = project_folder / 'Data' / 'Data_Entry' / '05_vegetation_cover.xlsx'
visit_input = plot_folder / '03_sitevisit_npsswan2024.csv'
vegetation_input = plot_folder / 'source' / 'SWAN_Vegetation_Database' / 'SWAN_Veg_PointIntercept.csv'
codes_input = plot_folder / 'source' / 'SWAN_Vegetation_Database_Fields' / 'catvars_SWAN_Veg_PointIntercept.txt'
akveg_credentials = (
    credential_folder / "akveg_public_read" / "authentication_akveg_public_read.csv"
)

# Define output
vegetation_output = plot_folder / '05_vegetationcover_npsswan2024.csv'

# Read in data
template = pl.read_excel(template_input)
visit_original = pl.read_csv(visit_input, columns='site_visit_code')
vegetation_original = pl.read_csv(vegetation_input, columns=['Plot', 'Sample_Date', 'Transect',
                                                   'Point', 'Species_Code', 'Damage_Text'],
                                  try_parse_dates=False, encoding='utf8-lossy')
swan_codes_original = pl.read_csv(codes_input, separator='\t', encoding='utf8-lossy')

# Query AKVEG database
akveg_db_connection = connect_database_postgresql(akveg_credentials)

# Query database for taxonomy checklist
taxonomy_query = """SELECT taxon_all.taxon_code, taxon_all.taxon_name, taxon_all.taxon_accepted_code
                    FROM taxon_all;"""

taxonomy_original = query_to_dataframe(akveg_db_connection, taxonomy_query)
taxonomy_original = pl.from_pandas(taxonomy_original)

## Close the database connection
akveg_db_connection.close()

# Create accepted taxonomy table
taxonomy_accepted = (
    taxonomy_original.filter(pl.col("taxon_code") == pl.col("taxon_accepted_code"))
    .rename({"taxon_name": "name_adjudicated"})
    .drop("taxon_code")
)

taxonomy_akveg = (taxonomy_original.join(taxonomy_accepted, on='taxon_accepted_code', how='left')
                  .drop(["taxon_code","taxon_accepted_code"]))

# Create site visit codes
vegetation = vegetation_original.with_columns(observe_date = pl.col('Sample_Date').str.to_date("%m/%d/%Y"))
vegetation = vegetation.with_columns(date_string = pl.col('observe_date').dt.to_string().str.replace_all("-", ""))
vegetation = vegetation.with_columns(site_visit_code = pl.concat_str([pl.col('Plot'),pl.col('date_string')],
                                                                     separator="_"))

# Ensure that all site visit codes are included in the Site Visit table
print(vegetation.join(visit_original, on='site_visit_code', how='anti').shape[0])
print(visit_original.join(vegetation, on='site_visit_code', how='anti').shape[0])

# Re-classify "Damage Text" column into Boolean "dead" or "live"
vegetation = vegetation.with_columns(
    dead_status = pl.when(pl.col('Damage_Text').is_in(["Standing dead", "Standing dead fire burned"]))
    .then(pl.lit("TRUE"))
    .when(pl.col("Species_Code").is_in(['SD', 'DLI', 'DM']) ) ## Definition: Standing dead or dead
    .then(pl.lit("TRUE"))
    .otherwise(pl.lit("FALSE")))

# Calculate cover percent

# Calculate maximum number of hits based on number of points per transect and number of transects per site
surveyed_points = vegetation.group_by(["site_visit_code", "Transect"]).agg(pl.col("Point").unique().len())
max_hits = surveyed_points.group_by(["site_visit_code"]).sum()
max_hits = (max_hits.drop('Transect')
            .rename({"Point":"max_hits"}))

# Calculate number of hits per species per plot
# If a species shows up multiple times at the same point (i.e., is present in more than one height strata), it is only
# counted once
vegetation = vegetation.with_columns(hit = 1)
cover_summary = vegetation.group_by(["site_visit_code", "Species_Code",
                                    "dead_status", "Transect", "Point"]).min().select(["site_visit_code",
                                                                                       "Species_Code",
                                    "dead_status", "hit"])
cover_summary = cover_summary.group_by(["site_visit_code", "Species_Code", "dead_status"]).sum()
cover_summary = cover_summary.join(max_hits, on = "site_visit_code", how = "left")

# Calculate percent cover
cover_summary = (cover_summary.with_columns(cover_percent = pl.col('hit') / pl.col('max_hits') * 100)
                 .drop(["hit", "max_hits"]))

# Obtain species names

## Format SWAN codes to exclude authorship
swan_codes = swan_codes_original.with_columns(
    pl.col("definition")
    .str.replace_many(["Lichen", "L."], ["lichen", ""])
    .str.replace_all(r"\s+", " ")
    .str.extract(r"^(.*?)(?:\s[A-Z]\.|\s[A-Z]|\s\(.*|$)", 1)
    .str.strip_chars().alias(
    "name_original"))

swan_codes = swan_codes.with_columns(pl.col('name_original').str.replace(pattern="Betula � dugleana", value="Betula × dugleana"))

## Append to cover summary
cover_summary = cover_summary.join(swan_codes, how='left', left_on='Species_Code', right_on='code')

## Replace subspecies abbreviation
cover_summary = cover_summary.with_columns(pl.col('name_original').str.replace(pattern='subsp.', value='ssp.'))

## Join with AKVEG Comprehensive Checklist to obtain adjudicated name
cover_summary = (cover_summary.join(taxonomy_akveg, how='left', left_on='name_original', right_on='taxon_name')
                 .drop("attributeName"))

## Drop abiotic cover codes
abiotic_codes = ['BO', 'SW', 'BG', 'DW', 'OM', 'UN', 'SC', 'G', 'LT', 'RK']
cover_summary = cover_summary.filter(~pl.col('Species_Code').is_in(abiotic_codes))

## Address missing taxa names
missing_names = (cover_summary.filter(pl.col('name_adjudicated').is_null())
                 .unique(subset=pl.col(
    'name_original'))
                 .drop(['site_visit_code', 'dead_status', 'cover_percent']))

## Fill in missing names
replace_codes = {"BEDU": "Betula cf. occidentalis",
 "BL": "lichen",
 "BM": "moss",
 "CR": "biotic soil crust",
 "DLI": "lichen",
 "DM": "moss",
 "DROC": "Dryas",
 "FUNGI": "fungus",
 "MO": "moss",
 "SD": "unknown",
 "TRICAEC": "Trichophorum cespitosum",
 "VAOX": "Oxycoccus microcarpus"}

cover_summary = cover_summary.with_columns(pl.when(pl.col('name_adjudicated').is_null())
                                           .then(pl.col('Species_Code'))
                                           .otherwise(pl.col('name_adjudicated'))
                                           .alias("name_adjudicated"))

cover_summary = cover_summary.with_columns(pl.col('name_adjudicated')
                                           .str.replace_many(replace_codes))

## Ensure that all null values have been addressed
print(cover_summary.null_count())

# Final column formatting
cover_final = (cover_summary.with_columns(pl.col("cover_percent").round(3),
                                          cover_type = pl.lit("absolute foliar cover"))
               .select(template.columns)
               .sort(by=["site_visit_code", "name_original"]))

# Export as CSV
cover_final.write_csv(vegetation_output)