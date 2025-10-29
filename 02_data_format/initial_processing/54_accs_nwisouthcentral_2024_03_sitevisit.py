# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2024 ACCS NWI Southcentral Site Visit Data
# Author: Amanda Droghini
# Last Updated: 2025-10-28
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2024 ACCS NWI Southcentral Site Visit Data" formats information about site visits for
# ingestion into AKVEG Database. The script verifies the data for completeness, ensures that all constrained values
# are included in the AKVEG Database, corrects unknown personnel names, and formats required metadata. The
# output is a CSV table that can be converted and included in a SQL INSERT statement.
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
site_input = plot_folder / '02_site_accsnwisouthcentral2024.csv'
visit_input = plot_folder / 'source' / '03_sitevisit_accsnwisouthcentral2024.xlsx'
template_input = project_folder / 'Data' / 'Data_Entry' / '03_site_visit.xlsx'
akveg_credentials = (
    credential_folder / "akveg_public_read" / "authentication_akveg_public_read.csv"
)

# Define output
visit_output = plot_folder / '03_sitevisit_accsnwisouthcentral2024.csv'

# Read in data
template = pl.read_excel(template_input)
site_original = pl.read_csv(site_input, columns='site_code')
visit_original = pl.read_excel(visit_input)

# Query AKVEG database
akveg_db_connection = connect_database_postgresql(akveg_credentials)

# Query database for constrained values
personnel_query = """SELECT personnel.personnel
                    FROM personnel;"""

personnel_original = query_to_dataframe(akveg_db_connection, personnel_query)
personnel_original = pl.from_pandas(personnel_original)['personnel'].to_list()

structural_query = """SELECT structural_class.structural_class
                    FROM structural_class;"""
structural_original = query_to_dataframe(akveg_db_connection, structural_query)
structural_original = pl.from_pandas(structural_original)['structural_class'].to_list()

## Close the database connection
akveg_db_connection.close()

# Check for null values
with pl.Config(tbl_cols=visit_original.shape[1]):
    print(visit_original.null_count())

# Ensure that all site codes are present in the visit table
print(site_original['site_code'].equals(visit_original['site_code']))

# Ensure that all site visit codes are unique
print(visit_original['site_visit_code'].is_unique().unique())

# Ensure date ranges are reasonable
print(visit_original.select('observe_date').describe())
print(visit_original['observe_date'].str.to_date("%Y-%m-%d").dt.month().unique())

# Correct columns: project code, data_tier, env_observer, homogenous
visit = (visit_original.with_columns(pl.lit('accs_nwisouthcentral_2024').alias('project_code'),
                                     pl.lit('map development & verification').alias('data_tier'),
                                     pl.lit('none').alias('env_observer'))  ## environment data
         # not collected
         .rename({'homogenous': 'homogeneous'}))

# Verify that values match constrained values
print(visit.unique(subset=['scope_vascular', 'scope_bryophyte', 'scope_lichen'])
      .select(['scope_vascular', 'scope_bryophyte', 'scope_lichen']))

## Identify which personnel names are not included in database
personnel_columns = ['veg_observer', 'veg_recorder', 'env_observer', 'soils_observer']
missing_personnel = ((visit.unpivot(on=personnel_columns)
                     .filter(~pl.col('value').is_in(personnel_original)))['value']
                     .unique()
                     .sort()
                     .to_list())

print(f"Unknown personnel: {missing_personnel}")  ## Missing names will be added to the database during the next
# round of updates

## Verify that all structural class values match constrained values
print(visit.filter(~pl.col('structural_class').is_in(structural_original)).unique().shape[0])

## Verify that homogenous column is boolean
print(visit['homogeneous'].dtype)

# Reorder/drop columns to match data entry template
visit = visit[template.columns]

# Export as CSV
visit.write_csv(visit_output)
