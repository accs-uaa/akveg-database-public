# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2024 ACCS NWI Southcentral Project Data
# Author: Amanda Droghini
# Last Updated: 2025-09-22
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2024 ACCS NWI Southcentral Project Data" formats and corrects project-level information for
# ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# -------------------

# Import libraries
import polars as pl
from pathlib import Path

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '54_accs_nwisouthcentral_2024'

# Define inputs
project_input = plot_folder / 'source' / '01_project_accsnwisouthcentral2024.xlsx'
template_input = project_folder / 'Data_Entry' / '01_project.xlsx'

# Define output
project_output = plot_folder / '01_project_accsnwisouthcentral2024.csv'

# Read in data
project_original = pl.read_excel(project_input, schema_overrides={"year_start": pl.Int64,
                                                           "year_end": pl.Int64})
template = pl.read_excel(template_input, schema_overrides={"year_start": pl.Int64,
                                                           "year_end": pl.Int64})

# Edit existing data
project = (project_original.with_columns(project_code = pl.lit('accs_nwisouthcentral_2024'),
                                        originator = pl.lit('ACCS'),
                                        funder = pl.lit('USFWS'),
                                        project_description = pl.lit('Ground surveys to support the interpretation and '
                                                              'mapping of wetlands in southcentral Alaska.'))
.select(template.columns))

# Export as CSV
project.write_csv(project_output)