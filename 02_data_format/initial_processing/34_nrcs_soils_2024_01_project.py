# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format NRCS Alaska 2024 Project
# Author: Amanda Droghini
# Last Updated: 2025-07-19
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format NRCS Alaska 2024 Project Data" populates the Project table according to AKVEG
# requirements. The output is a CSV file that can be used in an SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries
from pathlib import Path
import polars as pl

# Set root directory
drive = Path("C:/")
root_folder = Path("ACCS_Work")

# Define folder structure
project_folder = (
    drive
    / root_folder
    / "OneDrive - University of Alaska"
    / "ACCS_Teams"
    / "Vegetation"
    / "AKVEG_Database"
    / "Data"
)

plot_folder = project_folder / "Data_Plots" / "34_nrcs_soils_2024"

# Define input
template_input = project_folder / "Data_Entry" / "01_project.xlsx"

# Define output
project_output = plot_folder / "01_project_nrcssoils2024.csv"

# Read in data
template = pl.read_excel(template_input)

# Populate columns
project = pl.DataFrame(
    {
        "project_code": ["nrcs_soils_2024"],
        "project_name": ["NRCS Alaska Region Soil Mapping " "Surveys"],
        "originator": ["NRCS"],
        "funder": ["NRCS"],
        "manager": ["Blaine Spellman"],
        "completion": ["ongoing"],
        "year_start": ["1997"],
        "year_end": ["-999"],
        "project_description": [
            "Vegetation and associated data collected by NRCS as part of the production of soils maps for Alaska."
        ],
        "private": ["TRUE"],
    }
)

# Ensure columns match data entry template
project = project[template.columns]

# Export dataframes to CSV
project.write_csv(project_output)
