# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Batch run 'correct' scripts
# Author: Amanda Droghini
# Last Updated: 2026-02-04
# Usage: Execute in Python 3.13+.
# Description: "Batch run 'correct' scripts" iterates through each Python script that identifies and corrects quality
# control issues. The output is a series of CSV files that contain the corrected tables. Importantly, this
# script is intended to provide a convenient shortcut only after the results from the individual QC scripts have
# been examined and deemed correct. This script should not be used if new sites are being added to the AKVEG
# database. In such cases, QC scripts should be run individually.
# ---------------------------------------------------------------------------

# Import required libraries
import subprocess
from pathlib import Path

# Define Python environment
envr = Path('C:\\Users\\adroghini\\miniforge3\\envs\\accs_akveg\\python.exe')

# Set root directory
drive = Path('C:/')
root_folder = 'ACCS_Work'

# Define folder structure
repository_folder = (drive / root_folder / 'Repositories' / 'akveg-database' / '03_data_insert' /
                     'individual_scripts')

# List relevant files
file_list = list(repository_folder.glob('[0-9]*Correct*'))

for script in file_list:
        print(script)
        subprocess.call([envr, script])
