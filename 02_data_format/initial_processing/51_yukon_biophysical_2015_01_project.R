# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for Yukon Biophysical Plots data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-06
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Project Table for Yukon Biophysical Plots data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/51_yukon_biophysical_2015')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
template_input = path(template_folder, "01_project.xlsx")

# Define output datasets
project_yukon_output = path(plot_folder, '01_project_yukonbiophysical2015.csv')

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Populate fields ----
project_yukon = template %>% 
  add_row(project_code = 'yukon_biophysical_2015') %>% 
  mutate(project_name = 'Yukon Biophysical Plots Data') %>% 
  mutate(originator = 'YG') %>%
  mutate(funder = 'YG') %>% 
  mutate(manager = 'Nadele Flynn') %>% 
  mutate(completion = 'ongoing') %>% 
  mutate(year_start = as.numeric("2000")) %>% 
  mutate(year_end = as.numeric("2015")) %>% 
  mutate(project_description = 'Site, vegetation, and soil information collected since the year 2000 as part of the Yukon Ecological and Landscape Classification project.') %>% 
  mutate(private = "TRUE")

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(project_yukon, is.na)
    , sum))

# Export as CSV ----
write_csv(project_yukon, project_yukon_output)

# Clear workspace ----
rm(list=ls())
