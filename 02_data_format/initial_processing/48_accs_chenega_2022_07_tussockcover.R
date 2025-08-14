# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-03
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Whole Tussock Cover for ACCS Chenega Data" formats whole tussock cover data collected by ACCS. Since tussock cover was 0% for all sites, the script creates a dataframe with an entry for each site visit and adds a whole tussock percent column with a value of 0% for all site visits. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(fs)
library(readr)
library(readxl)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/48_accs_chenega_2022')
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
site_visit_input = path(plot_folder, '03_sitevisit_accschenega2022.csv')
template_input = path(template_folder, "07_whole_tussock_cover.xlsx")

# Define output datasets
tussock_cover_output = path(plot_folder, '07_wholetussockcover_accschenega2022.csv')

# Read in data ----
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Create dataframe ----
tussock_cover = data.frame(site_visit_code = site_visit_original$site_visit_code)
tussock_cover$cover_type = 'absolute foliar cover'
tussock_cover$tussock_percent_cover = 0

# Export as CSV ----
write_csv(tussock_cover, tussock_cover_output)

# Clear workspace ----
rm(list=ls())