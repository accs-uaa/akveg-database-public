# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for NPS Arctic Network 2014 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-08
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Project Table forNPS Arctic Network 2014 data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '35_nps_arcn_2014')
template_folder = path(project_folder, "Data", "Data_Entry")

# Define inputs ----
template_input = path(template_folder, "01_project.xlsx")

# Define outputs ----
project_output = path(plot_folder, "01_project_npsarcn2014.csv")

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Populate fields ----
project_data = template %>% 
  add_row(project_code = 'nps_arcn_2014') %>% 
  mutate(project_name = 'NPS Terrestrial Vegetation Monitoring Protocol for the Arctic Alaska Network') %>% 
  mutate(originator = 'NPS') %>%
  mutate(funder = 'NPS') %>% 
  mutate(manager = 'David Swanson') %>% 
  mutate(completion = 'finished') %>% 
  mutate(year_start = as.numeric("2009")) %>% 
  mutate(year_end = as.numeric("2014")) %>% 
  mutate(project_description = 'Vegetation plots data collected as part of terrestrial monitoring of parks in the NPS Arctic Alaska Network.') %>% 
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
