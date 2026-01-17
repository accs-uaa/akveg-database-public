# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Tetlin 2022-2024 project data for AKVEG Database
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2026-01-16
# Usage: Must be executed in R version 4.5.1+.
# Description: "Format Tetlin 2022-2024 project data for AKVEG Database" formats project data for entry into AKVEG Database.
# ---------------------------------------------------------------------------

# Load packages
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(readr)
library(readxl)

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
template_folder = path(project_folder, 'Data/Data_Entry')
plot_folder = path(project_folder, 'Data/Data_Plots/53_fws_tetlin_2024')

# Define input template
project_template = path(template_folder, '01_project.xlsx')

# Define output dataset
project_output = path(plot_folder, '01_project_fwstetlin2024.csv')

# Create data template
project_columns = colnames(read_xlsx(path=project_template))

# Parse project data
project_data = setNames(data.frame(matrix(ncol = length(project_columns), nrow = 1)), project_columns) %>%
  mutate(project_code = 'fws_tetlin_2024',
         project_name = 'Tetlin National Wildlife Refuge Bison Habitat Linear Transects',
         originator = 'USFWS',
         funder = 'USFWS',
         manager = 'Hunter Gravley',
         completion = 'finished',
         year_start = 2022,
         year_end = 2024,
         project_description = 'Linear transect vegetation plots assessed to quantify and describe potential habitat for wood bison on the Tetlin National Wildlife Refuge.',
         private = 'TRUE')

# Export data
write_csv(project_data, project_output)

# Clear workspace
rm(list=ls())
