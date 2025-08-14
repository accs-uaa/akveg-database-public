# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-23
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Project Table for USFWS Pribilof Islands 2022 data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
template_input = path(template_folder, "01_project.xlsx")

# Define output datasets
project_output = path(plot_folder, '01_project_fwspribilof2022.csv')

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Populate fields ----
project_data = template %>% 
  add_row(project_code = 'fws_pribilof_2022') %>% 
  mutate(project_name = 'Pribilof Islands Land Cover Reconnaissance ') %>% 
  mutate(originator = 'USFWS') %>%
  mutate(funder = 'USFWS') %>% 
  mutate(manager = 'Hunter Gravley') %>% 
  mutate(completion = 'finished') %>% 
  mutate(year_start = as.numeric("2022")) %>% 
  mutate(year_end = as.numeric("2022")) %>% 
  mutate(project_description = 'Vegetation plots data collected to inventory general and detailed land cover classes on St. Paul and St. George islands.') %>% 
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