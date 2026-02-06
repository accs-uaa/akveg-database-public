# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Environment data for NRCS Alaska 2024"
# Author: Amanda Droghini
# Last Updated: 2026-02-05
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Environment data for NRCS Alaska 2024" ingests previously processed environment data and
# appends a new site visit code for correspondence with the most recent formatting of the dataset. The output is a
# CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import polars as pl
from pathlib import Path
from utils import get_template

# Define directories
drive = Path('C:/')
root_folder = drive / 'ACCS_Work'

# Define folders
project_folder = root_folder / 'OneDrive - University of Alaska' / 'ACCS_Teams' / 'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '34_nrcs_soils_2024'

# Define inputs
envr_input = plot_folder / 'archive' / '34_nrcs_soils_2022'/ '12_environment_nrcssoils2022.csv'
visit_input = plot_folder / '03_sitevisit_nrcssoils2024.csv'
lookup_input = plot_folder / 'working' / 'lookup_visit.csv'

# Define output
envr_output = plot_folder / '12_environment_nrcssoils2024.csv'

# Read in data
envr_original = pl.read_csv(envr_input)
visit_original = pl.read_csv(visit_input, columns="site_visit_code")
lookup_original = pl.read_csv(lookup_input, columns=["vegplotid", "site_visit_code"])
template = get_template("environment")

# Ensure site visit codes are unique
print(envr_original["site_visit_code"].is_duplicated().unique())

# Drop date string from site visit code to obtain site code
envr = envr_original.with_columns(pl.col("site_visit_code").str.extract(r"(.*)(_)").alias("site_code"))

# Join with look-up table to obtain new site visit code
envr_lookup = envr.join(lookup_original, how="left", left_on="site_code", right_on="vegplotid")

# Join with visit df to drop excluded sites
envr = (envr_lookup.join(visit_original, how="inner", left_on="site_visit_code_right", right_on="site_visit_code"))

# Format final table
envr_final = (envr.drop("site_visit_code")
              .rename({"site_visit_code_right":"site_visit_code"})
              .select(template.columns))

# Ensure new site visit codes are unique
print(envr_final["site_visit_code"].is_duplicated().unique())

# Export as CSV
envr_final.write_csv(envr_output)
