# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover for 2024 ACCS NWI Southcentral data"
# Author: Amanda Droghini
# Last Updated: 2025-09-23
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Whole Tussock Cover for 2024 ACCS NWI Southcentral data" formats information and performs
# quality control checks about whole tussock cover cover for ingestion into AKVEG Database. The output is a CSV
# table that can be converted and included in a SQL INSERT statement.
# -------------------

# Import libraries
import polars as pl
from pathlib import Path

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database'
plot_folder = project_folder / 'Data' / 'Data_Plots' / '54_accs_nwisouthcentral_2024'

# Define inputs
visit_input = plot_folder / '03_sitevisit_accsnwisouthcentral2024.csv'
tussock_input = plot_folder / 'source' / '07_wholetussockcover_accsnwisouthcentral2024.xlsx'
template_input = project_folder / 'Data' / 'Data_Entry' / '07_whole_tussock_cover.xlsx'

# Define output
tussock_output = plot_folder / '07_wholetussockcover_accsnwisouthcentral2024.csv'

# Read in data
template = pl.read_excel(template_input)
visit_original = pl.read_csv(visit_input, columns='site_visit_code')
tussock_original = pl.read_excel(tussock_input)

# Ensure that all site visit codes are included in the Site Visit table
print(tussock_original.join(visit_original, on='site_visit_code', how='anti').shape[0])
print(visit_original.join(tussock_original, on='site_visit_code', how='anti').shape[0])

# Perform QC checks
print(tussock_original['tussock_percent_cover'].describe())
print(tussock_original.null_count())
print(tussock_original['cover_type'].unique())

# Ensure column names & order match the template
tussock = tussock_original.select(template.columns)

# Export as CSV
tussock.write_csv(tussock_output)