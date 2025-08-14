# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Project Table for BLM AIM Various 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-06
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Project Table for BLM AIM Various 2022 data" enters and formats project-level information for ingestion into the AKVEG Database. The script depends on the output from the 44_aim_various_2022.py script in the /datum_conversion subfolder. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(lubridate)
library(readr)
library(readxl)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = 'C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots/44_aim_various_2022'
workspace_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
site_input = path(workspace_folder, 'site_data_export.csv')
template_input = path(template_folder, "01_project.xlsx")

# Define output datasets
project_output = path(plot_folder, paste0("01_project_", 'aimvarious2022', ".csv"))

# Read in data ----
site_original = read_csv(site_input)
template = colnames(read_xlsx(path=template_input))

# Parse out project names and years ----
# Correct 'unknown' project code (should be Central Yukon based on plot ID)
project_data = site_original %>% 
  select(Project, EstablishmentDate) %>% 
  mutate(Project = case_when(Project == "UnspecifiedBLM" ~ "AK_CentralYukonFO_2022",
                             .default = Project),
         year_start = min(year(EstablishmentDate)),
         year_end = max(year(EstablishmentDate))) %>%
  mutate(project_code = str_replace(Project, "AK", "AIM"),
         project_code = str_replace(project_code, "-", "_"),
         project_code = gsub("([a-z])([A-Z])","\\1_\\2", project_code),
         project_code = str_to_lower(project_code)) %>% 
  mutate(project_name = str_remove(Project, "AK_"),
         project_name = case_when(grepl("Kobuk-Seward", project_name) ~ str_replace(project_name, "_", " "),
                                  .default = str_remove(project_name, "_\\d{4}$")),
         project_name = str_replace(project_name, "FO", "Field Office"),
         project_name = gsub("([a-z])([A-Z])","\\1 \\2", project_name),
         project_name = str_c(project_name, "Assessment, Inventory, and Monitoring", sep = " ")) %>%
  select(-c(Project, EstablishmentDate)) %>% 
  distinct(project_code, .keep_all = TRUE)

# Populate remaining fields ----
project_data = project_data %>%
  mutate(originator = c("BLM", "BLM", "ABR")) %>% 
  mutate(funder = 'BLM') %>% 
  mutate(manager = c('Tina Boucher', 'Tina Boucher', 'Gerald Frost')) %>% 
  mutate(completion = 'finished') %>%
  mutate(project_description = 'Vegetation plots data collected as part of the BLM AIM program.') %>% 
  mutate(private = "FALSE") %>% 
  select(all_of(template))

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
