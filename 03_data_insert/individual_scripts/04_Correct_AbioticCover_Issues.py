# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Perform QC check for Abiotic Top Cover table
# Author: Amanda Droghini
# Last Updated: 2025-09-24
# Usage: Execute in Python 3.13+.
# Description: "Perform QC check for Abiotic Top Cover table" identifies and corrects
# data entry errors in the Abiotic Top Cover of the AKVEG Database. The script drops elements that are not abiotic
# elements and adds missing abiotic elements (i.e., those with 0% cover).
# ---------------------------------------------------------------------------

# Import packages
import polars as pl
from pathlib import Path
from akutils import connect_database_postgresql
from akutils import query_to_dataframe

# Set root directory
drive = Path("C:/")
root_folder = (
    drive
    / "ACCS_Work"
    / "OneDrive - University of Alaska"
    / "ACCS_Teams"
    / "Vegetation"
    / "AKVEG_Database"
)

# Define folder structure
data_folder = drive / root_folder / "Data" / "Data_Plots" / "processed"
corrected_folder = data_folder / "corrected"
credential_folder = root_folder / "Credentials"

# Define input files
abiotic_file = corrected_folder / "abiotic_top_cover.csv"  ## Use corrected version
akveg_credentials = (
    credential_folder / "akveg_public_read" / "authentication_akveg_public_read.csv"
)

# Define export file
abiotic_output = corrected_folder / "abiotic_top_cover.csv"

# Read in data
abiotic_data = pl.read_csv(abiotic_file)

# Query AKVEG database
akveg_db_connection = connect_database_postgresql(akveg_credentials)

## Query database for taxonomy checklist
abiotic_query = """SELECT ground_element_code
                    FROM ground_element
                    WHERE element_type IN ('abiotic', 'both') ;"""

abiotic_list = query_to_dataframe(akveg_db_connection, abiotic_query)
abiotic_list = pl.from_pandas(abiotic_list)['ground_element_code'].to_list()

## Close the database connection
akveg_db_connection.close()

# Identify which entries have an abiotic element code that does not match with the constrained values in AKVEG
## All errors are sites with 'animal litter' (n=671)
unmatched_codes = abiotic_data.filter(~pl.col('abiotic_element_code').is_in(abiotic_list))

## Drop codes that are not abiotic elements
corrected_abiotic = abiotic_data.filter(pl.col('abiotic_element_code').is_in(abiotic_list))

# Insert missing abiotic elements
## Each site visit must include one entry for each abiotic element (7 elements per site visit)
site_visits = corrected_abiotic['site_visit_code'].unique().to_list()

# Create dataframe with all possible combinations
all_combinations = pl.DataFrame({
    'site_visit_code': site_visits,
    '__join_key': 1
}).join(
    pl.DataFrame({
        'abiotic_element_code': abiotic_list,
        '__join_key': 1
    }),
    on='__join_key'
).drop('__join_key')

# Find the missing combinations
missing_rows = all_combinations.join(
    corrected_abiotic,
    on=['site_visit_code', 'abiotic_element_code'],
    how='anti'
)

# Populate the missing rows with the required values
missing_rows = missing_rows.with_columns(
    pl.lit(0).cast(pl.Float64).alias('abiotic_top_cover_percent')
)

# Concatenate the original DataFrame with the newly created missing rows
final_abiotic = pl.concat([corrected_abiotic, missing_rows])

## Ensure that each site visit code has 7 elements
print(final_abiotic.group_by(by='site_visit_code').agg(pl.count('abiotic_element_code'))[
    'abiotic_element_code'].unique())

# Export corrected table
final_abiotic.write_csv(abiotic_output)
