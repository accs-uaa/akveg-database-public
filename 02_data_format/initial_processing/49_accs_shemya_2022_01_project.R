# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for ACCS Shemya Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-08
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Project Table for ACCS Shemya Data" formats project-level data collected and entered by ACCS for ingestion into the AKVEG Database. The script populates fields with values that match constrained values in the AKVEG Database and adds required fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/49_accs_shemya_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input datasets
project_shemya_input = path(source_folder,'01_Project_Shemya_2022.xlsx')
template_input = path(template_folder, "01_project.xlsx")

# Define output datasets
project_shemya_output = path(plot_folder, '01_project_accsshemya2022.csv')

# Read in data ----
project_shemya_original = read_xlsx(project_shemya_input)
template = colnames(read_xlsx(path=template_input))

# Format data ----

# Populate existing fields with names that match constrained values in AKVEG
# Add Project Description column
project_shemya = project_shemya_original %>% 
  mutate(project_code = 'accs_shemya_2022') %>% 
  mutate(originator = 'ACCS') %>%
  mutate(funder = 'USACE') %>% 
  mutate(manager = 'Justin Fulkerson') %>% 
  mutate(completion = 'finished') %>% 
  mutate(private = "FALSE") %>% 
  mutate(project_description = 'Ground-based, random plots of native vegetation surveyed as part of a non-native plant survey.') %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(project_shemya, project_shemya_output)

# Clear workspace ----
rm(list=ls())
