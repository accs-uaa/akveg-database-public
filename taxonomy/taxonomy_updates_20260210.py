# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Add Yukon taxa to taxonomy table
# Author: Amanda Droghini
# Last Updated: 2026-02-10
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Add Yukon taxa to taxonomy table" appends taxa to be added to the taxonomy table and creates unique
# taxon short codes for the new taxa. The output is a .xlsx table that can be ingested in the AKVEG Database.
# ---------------------------------------------------------------------------

# Import libraries
import polars as pl
from pathlib import Path
from taxonomy_utils import generate_taxon_codes

# Define directories
drive = Path('C:/')
root = drive / 'ACCS_Work'
project_folder = root / 'OneDrive - University of Alaska' /'ACCS_Teams' /'Vegetation' / 'AKVEG_Database' / 'Data'
taxonomy_folder = project_folder / 'Tables_Taxonomy'

# Define input
taxonomy_input = taxonomy_folder / 'taxonomy_20260126.xlsx'

# Define output
taxonomy_output = taxonomy_folder / 'taxonomy.xlsx'

# Read in data
taxonomy_original = pl.read_excel(taxonomy_input, sheet_name='taxonomy')
taxonomy_yukon = pl.read_excel(taxonomy_input, sheet_name='yukon_to_add',
                               schema_overrides={'taxon_code': pl.String,
                                                 'code_manual': pl.Boolean}
                               )

# Verify that there are no null values except for `taxon_code` and `code_manual`
with pl.Config() as cfg:
    cfg.set_tbl_cols(16)
    print(taxonomy_yukon.null_count())

# Obtain taxon short code
taxonomy_codes = generate_taxon_codes(taxonomy_yukon)

# Ensure no nulls

# Ensure no duplicates

# Replace weird whitespace with true spaces