# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Abiotic Top Cover for 2024 ACCS NWI Southcentral data"
# Author: Amanda Droghini
# Last Updated: 2025-09-24
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Abiotic Top Cover for 2024 ACCS NWI Southcentral data" formats information about abiotic top
# cover for ingestion into AKVEG Database. The script performs quality control checks and renames abiotic elements
# to match constrained values in the AKVEG Database. The output is a CSV table that can be converted and included in
# a SQL INSERT statement.
# -------------------

# Import libraries
import polars as pl
from pathlib import Path
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database'
plot_folder = project_folder / 'Data' / 'Data_Plots' / '54_accs_nwisouthcentral_2024'
credential_folder = project_folder / "Credentials"

# Define inputs
visit_input = plot_folder / '03_sitevisit_accsnwisouthcentral2024.csv'
abiotic_input = plot_folder / 'source' / '06_abiotictopcover_accsnwisouthcentral2024.xlsx'
template_input = project_folder / 'Data' / 'Data_Entry' / '06_abiotic_top_cover.xlsx'
akveg_credentials = (
    credential_folder / "akveg_public_read" / "authentication_akveg_public_read.csv"
)

# Define output
abiotic_output = plot_folder / '06_abiotictopcover_accsnwisouthcentral2024.csv'

# Read in data
template = pl.read_excel(template_input)
visit_original = pl.read_csv(visit_input, columns='site_visit_code')
abiotic_original = pl.read_excel(abiotic_input)

# Query AKVEG database
akveg_db_connection = connect_database_postgresql(akveg_credentials)

## Query database for taxonomy checklist
abiotic_query = """SELECT ground_element
                    FROM ground_element
                    WHERE element_type IN ('abiotic', 'both') ;"""

abiotic_list = query_to_dataframe(akveg_db_connection, abiotic_query)
abiotic_list = pl.from_pandas(abiotic_list)['ground_element'].to_list()

## Close the database connection
akveg_db_connection.close()

# Ensure that all site visit codes are included in the Site Visit table
print(abiotic_original.join(visit_original, on='site_visit_code', how='anti').shape[0])
print(visit_original.join(abiotic_original, on='site_visit_code', how='anti').shape[0])

# Verify that range of top cover percent is between 0-100%
print(abiotic_original['abiotic_top_cover_percent'].describe())

# Correct name of abiotic elements to match constrained values
abiotic = abiotic_original.with_columns(pl.col('abiotic_element').str.replace(pattern="dead down wood", value="dead "
                                                                                                              "down wood (≥ 2 mm)"))
print(abiotic['abiotic_element'].unique().is_in(abiotic_list).unique())

# Ensure that all 7 abiotic elements are included for every site visit
print((abiotic.group_by('site_visit_code').agg(pl.count("abiotic_element"))['abiotic_element'].unique()) ==
      len(
    abiotic_list))

# Ensure column names & order match the template
abiotic = abiotic.select(template.columns)

# Export as CSV
abiotic.write_csv(abiotic_output)
