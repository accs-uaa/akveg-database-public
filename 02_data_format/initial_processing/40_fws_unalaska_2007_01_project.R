# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for USFWS Unalaska 2007 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-24
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Project Table for USFWS Unalaska 2007 data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/40_fws_unalaska_2007')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
template_input = path(template_folder, "01_project.xlsx")

# Define output datasets
project_output = path(plot_folder, '01_project_fwsunalaska2007.csv')

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Populate fields ----
project_data = template %>% 
  add_row(project_code = 'fws_unalaska_2007') %>% 
  mutate(project_name = 'Alaska Arctic Vegetation Archive: Unalaska Vegetation Plots') %>% 
  mutate(originator = 'USFWS') %>%
  mutate(funder = 'USFWS') %>% 
  mutate(manager = 'Stephen Talbot') %>% 
  mutate(completion = 'finished') %>% 
  mutate(year_start = as.numeric("2007")) %>% 
  mutate(year_end = as.numeric("2010")) %>% 
  mutate(project_description = 'Vegetation plots data collected to identify, describe, and compare the main plant communities on Unalaska Island.') %>% 
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