# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for NPS Central Arctic Network data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-05-02
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Project Table for NPS Central Arctic Network data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries ----
library(dplyr)
library(fs)
library(readr)
library(readxl)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database', 'Data')
plot_folder = path(project_folder, 'Data_Plots', '26_nps_cakn_2021')
template_folder = path(project_folder, "Data_Entry")

# Define inputs ----
template_input = path(template_folder, "01_project.xlsx")

# Define outputs ----
project_output = path(plot_folder, "01_project_npscakn2021.csv")

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Populate fields ----
project_data = template %>% 
  add_row(project_code = 'nps_cakn_2021') %>% 
  mutate(project_name = 'Central Alaska Network Vegetation Monitoring') %>% 
  mutate(originator = 'NPS') %>%
  mutate(funder = 'NPS') %>% 
  mutate(manager = 'Carl Roland') %>% 
  mutate(completion = 'finished') %>% 
  mutate(year_start = as.numeric("2001")) %>% 
  mutate(year_end = as.numeric("2019")) %>% 
  mutate(project_description = 'Vegetation composition and structure data collected in long-term monitoring plots for the NPS Central Alaska Network inventory & monitoring program.') %>% 
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
