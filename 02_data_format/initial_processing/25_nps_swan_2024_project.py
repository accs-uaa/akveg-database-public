# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format 2024 NPS SWAN Project Data
# Author: Amanda Droghini
# Last Updated: 2025-09-16
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format 2024 NPS SWAN Project Data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# -------------------

# Import libraries
from pathlib import Path
import polars as pl

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '25_nps_swan_2024'

# Define input
template_input = project_folder / 'Data_Entry' / '01_project.xlsx'
template = pl.read_excel(template_input, schema_overrides={"year_start": pl.Int64,
                                                           "year_end": pl.Int64})

# Define output
project_output = plot_folder / '01_project_npsswan2024.csv'

# Populate columns
project_data = [('nps_swan_2024', 'Southwest Alaska Network Vegetation Monitoring', 'NPS', 'NPS', 'Amy Miller',
                'finished', 2007, 2024, 'Vegetation composition and structure data collected in long-term monitoring '
                                        'plots for the NPS Southwest Alaska Network Inventory & Monitoring Program.',
                'FALSE')]

# Append to template
project = pl.concat([template, pl.DataFrame(project_data, schema=template.schema, orient='row')])

# Export to CSV
project.write_csv(project_output)
