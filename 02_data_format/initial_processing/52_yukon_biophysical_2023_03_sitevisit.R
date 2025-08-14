# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for Yukon Biophysical Inventory Data System Plots"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-06-17
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Site Visit Table for Yukon Biophysical Inventory Data System Plots" formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, re-classifies structural class data, populates required metadata, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts=FALSE)
library(fs)
library(lubridate, warn.conflicts=FALSE)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database', 'Data')
plot_folder = path(project_folder, 'Data_Plots', '52_yukon_biophysical_2023')
source_folder = path(plot_folder, 'source', 'ECLDataForAlaska_20240919', 'YBIS_Data')
template_folder = path(project_folder, "Data_Entry")

# Define datasets ----

# Define input datasets
visit_input = path(source_folder, 'Plot_2024Apr09.xlsx')
site_input = path(plot_folder, "02_site_yukonbiophysical2023.csv")
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
visit_output = path(plot_folder, "03_sitevisit_yukonbiophysical2023.csv")

# Read in data ----
site_original = read_csv(site_input, col_select = c("site_code", "establishing_project_code"))
visit_original = read_xlsx(visit_input, range="A1:I15124")
template = colnames(read_xlsx(path=template_input))

# Format site code ----
## Mirror formatting used in processed site table
visit_data = visit_original %>% 
  mutate(site_code = str_c(`Project ID`, `Plot ID`, sep = "_")) %>% 
  right_join(site_original, by = "site_code")  ## Drop sites that aren't included in site table

## Ensure all site codes have a match in the site table
which(!(visit_data$site_code %in% site_original$site_code))

# Format date & site visit code ----

# Format date
visit_data = visit_data %>% 
  mutate(observe_date = as.Date(visit_data$`Survey date`, format="%Y %b %d"))

# Ensure date range is reasonable
summary(visit_data$observe_date)  ## No NAs
unique(month(visit_data$observe_date))

# Create site visit code
visit_data = visit_data %>% 
  mutate(date_string = str_replace_all(observe_date, "-", ""),
         site_visit_code = paste(site_code, date_string, sep = "_")) %>% 
  select(-date_string)

# Populate remaining columns ----
visit_final = visit_data %>% 
  mutate(project_code = 'yukon_biophysical_2023',
         data_tier = "map development & verification",
         veg_observer = "unknown",
         veg_recorder = "unknown",
         env_observer = "none",
         soils_observer = case_when(grepl("Soil", visit_data$Observers) ~ "unknown",
                                    .default = "none"),
         scope_vascular = "top canopy", # Review
         scope_bryophyte = "top canopy", # Review
         scope_lichen = "top canopy", # Review
         homogeneous = "TRUE",
         structural_class = 'not available') %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(visit_final, is.na)
    , sum))

# Verify personnel names
unique(visit_final$veg_observer)
unique(visit_final$veg_recorder)
unique(visit_final$env_observer)
unique(visit_final$soils_observer)

# Verify that all structural class values match a constrained value
table(visit_final$structural_class)

# Verify homogeneous values
table(visit_final$homogeneous)

# Export as CSV ----
write_csv(visit_final, visit_output)

# Clear workspace ----
rm(list=ls())
