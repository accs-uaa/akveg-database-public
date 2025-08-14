# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for Yukon Biophysical Inventory Data System Plots"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-11
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Project Table for Yukon Biophysical Inventory Data System Plots" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries ----
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(readr)
library(readxl)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database', 'Data')
plot_folder = path(project_folder, 'Data_Plots', '52_yukon_biophysical_2023')
template_folder = path(project_folder, "Data_Entry")

# Define inputs
template_input = path(template_folder, "01_project.xlsx")

# Define outputs
project_output = path(plot_folder, "01_project_yukonbiophysical2023.csv")

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Populate fields ----
project_data = template %>% 
  add_row(project_code = 'yukon_biophysical_2023') %>% 
  mutate(project_name = 'Yukon Biophysical Inventory Data System Plots') %>% 
  mutate(originator = 'YG') %>%
  mutate(funder = 'YG') %>% 
  mutate(manager = 'Anthony Francis') %>% 
  mutate(completion = 'ongoing') %>% 
  mutate(year_start = as.numeric("1975")) %>% 
  mutate(year_end = as.numeric("-999")) %>% 
  mutate(project_description = 'Site, soil, and vegetation information collected by multiple agencies to support vegetation inventory, wildlife habitat assessment, and baseline ecosystem products.') %>% 
  mutate(private = "FALSE")

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(project_data, is.na)
    , sum))

# Export as CSV ----
write_csv(project_data, project_output)

# Clear workspace ----
rm(list=ls())
