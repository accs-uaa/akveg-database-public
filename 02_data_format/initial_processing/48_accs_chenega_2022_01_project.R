# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-08
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Project Table for ACCS Chenega Data" formats project-level data collected and entered by ACCS for ingestion into the AKVEG Database. The script populates fields with values that match constrained values in the AKVEG Database and adds required fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/48_accs_chenega_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input datasets
project_chenega_input = path(source_folder,'01_Project_Chenega_2022.xlsx')
template_input = path(template_folder, "01_project.xlsx")

# Define output datasets
project_chenega_output = path(plot_folder, '01_project_accschenega2022.csv')

# Read in data ----
project_chenega_original = read_xlsx(project_chenega_input)
template = colnames(read_xlsx(path=template_input))

# Format data ----

# Populate existing fields with names that match constrained values in AKVEG
# Add Project Description column
project_chenega = project_chenega_original %>% 
  mutate(project_code = 'accs_chenega_2022') %>% 
  mutate(project_name = 'National Wetlands Inventory Mapping for the Chenega Region') %>% 
  mutate(originator = 'ACCS') %>% 
  mutate(manager = 'Lindsey Flagstad') %>% 
  mutate(completion = 'finished') %>% 
  mutate(private = "FALSE") %>% 
  mutate(project_description = 'Ground and aerial plots collected to update the National Wetlands Inventory map for the Chenega Region.') %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(project_chenega, project_chenega_output)

# Clear workspace ----
rm(list=ls())