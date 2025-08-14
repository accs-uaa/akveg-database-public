# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for BLM AIM Various 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-31
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Visit Table for BLM AIM Various 2022 data" formats information about site visits for ingestion into the AKVEG Database. The script depends on the output from the 44_aim_various_2022_02_extract_site_data.py script in the /datum_conversion subfolder. The script formats dates, creates site visit codes, re-classifies structural class data, populates required metadata, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = 'C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots/44_aim_various_2022'
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
site_visit_input = 'C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots/44_aim_various_2022/working/site_data_export.csv'
site_input = path(plot_folder, paste0("02_site_", 'aimvarious2022', ".csv"))
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, paste0("03_sitevisit_", 'aimvarious2022', ".csv"))

# Read in data ----
site_visit_original = read_csv(site_visit_input)
site_original = read_csv(site_input)
template = colnames(read_xlsx(path=template_input))

# Format site codes ----
# Remove 'AK' prefix
# Remove "CYFO' and 'EIFO' prefixes to shorten site code. project_code already includes this info
site_visit_data = site_visit_original %>% 
  mutate(site_code = str_remove_all(PlotID, c("AK-|CYFO-|EIFO-")),
         site_code = str_replace_all(site_code, "-", "_"))

# Ensure all sites in Site table are in Site Visit table and vice-versa
which(!(site_original$site_code %in% site_visit_data$site_code))
which(!(site_visit_data$site_code %in% site_original$site_code))

# Obtain project code ----
# Join with site data
site_visit_data = site_original %>% 
  select(establishing_project_code, site_code) %>% 
  right_join(site_visit_data, by = "site_code")

# Format date & site visit code ----

# Ensure date range is reasonable
hist(as.Date(site_visit_data$EstablishmentDate), 
     breaks = "day", 
     xlab = "Survey Date") 
summary(as.Date(site_visit_data$EstablishmentDate))

# Drop timestamp and create site visit code
site_visit_data = site_visit_data %>% 
  mutate(observe_date = as.Date(EstablishmentDate),
         date_string = str_replace_all(observe_date, "-", ""),
         site_visit_code = paste(site_code, date_string, sep = "_"))

# Format structural class ----
# Unable to determine whether 'post-fire scrub' refers to 'low' or 'tall' shrub
unique(site_visit_data$AlaskaEcotypeClassification)
unique(site_visit_data$WetlandType)

site_visit_class = site_visit_data %>% 
  mutate(structural_class = case_when(grepl("Tall S[a-z]rub", AlaskaEcotypeClassification) ~ "tall shrub",
                                      grepl("Low and Tall Shrub", AlaskaEcotypeClassification) ~ "tall shrub",
                                      grepl("Low S[a-z]rub", AlaskaEcotypeClassification) ~ "low shrub",
                                      grepl("Spruce (Forest|Woodland)", AlaskaEcotypeClassification) ~ "needleleaf forest",
                                      grepl("Aspen (Forest|Woodland)", AlaskaEcotypeClassification) ~ "broadleaf forest",
                                      grepl("Spruce-Birch Forest", AlaskaEcotypeClassification) ~ "mixed forest",
                                      grepl("Barrens", AlaskaEcotypeClassification) ~ "barrens or partially vegetated",
                                      grepl("Dwarf", AlaskaEcotypeClassification) ~ "dwarf shrub",
                                      grepl("Bluejoint", AlaskaEcotypeClassification) ~ "grass meadow",
                                      .default = "not available"))

# Confirm classification scheme                                     
table(site_visit_class$AlaskaEcotypeClassification, site_visit_class$structural_class)

# Populate remaining columns ----
site_visit_final = site_visit_class %>% 
  rename(project_code = establishing_project_code) %>% 
  mutate(data_tier = "map development & verification",
         veg_observer = "unknown",
         veg_recorder = "unknown",
         env_observer = "unknown",
         soils_observer = "unknown",
         scope_vascular = "exhaustive",
         scope_bryophyte = "common species",
         scope_lichen = "common species",
         homogenous = "TRUE") %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_final, is.na)
    , sum))

# Verify personnel names
unique(site_visit_final$veg_observer)
unique(site_visit_final$veg_recorder)
unique(site_visit_final$env_observer)
unique(site_visit_final$soils_observer)

# Verify that all structural class values match a constrained value
table(site_visit_final$structural_class)

# Verify homogeneous values
table(site_visit_final$homogenous)

# Export as CSV ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())
