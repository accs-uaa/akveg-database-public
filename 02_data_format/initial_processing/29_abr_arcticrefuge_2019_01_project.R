# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for 2019 ABR Arctic Refuge data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-28
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Project Table for 2019 ABR Arctic Refuge data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data')
plot_folder = path(project_folder, 'Data_Plots', '29_abr_arcticrefuge_2019')
template_folder = path(project_folder, "Data_Entry")

# Define inputs ----
template_input = path(template_folder, "01_project.xlsx")

# Define outputs ----
project_output = path(plot_folder, "01_project_abrarcticrefuge2019.csv")

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Populate fields ----
project_data = template %>% 
  add_row(project_code = 'abr_arcticrefuge_2019') %>% 
  mutate(project_name = 'Arctic National Wildlife Refuge Coastal Plain Land Cover') %>% 
  mutate(originator = 'ABR') %>%
  mutate(funder = 'USFWS') %>% 
  mutate(manager = 'Aaron Wells') %>% 
  mutate(completion = 'finished') %>% 
  mutate(year_start = as.numeric("2019")) %>% 
  mutate(year_end = as.numeric("2019")) %>% 
  mutate(project_description = 'Aerial and ground vegetation top cover data collected for development of a land cover map of the Arctic Refuge Coastal Plain.') %>% 
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
